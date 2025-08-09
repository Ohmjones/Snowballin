package app

import (
	"Snowballin/dataprovider"
	cg "Snowballin/dataprovider/coingecko"
	cmc "Snowballin/dataprovider/coinmarketcap"
	"Snowballin/notification/discord"
	"Snowballin/pkg/broker"
	krakenBroker "Snowballin/pkg/broker/kraken"
	"Snowballin/strategy"
	"Snowballin/utilities"
	"Snowballin/web"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type TradingState struct {
	broker                  broker.Broker
	logger                  *utilities.Logger
	config                  *utilities.AppConfig
	discordClient           *discord.Client
	cache                   *dataprovider.SQLiteCache
	activeDPs               []dataprovider.DataProvider
	providerNames           map[dataprovider.DataProvider]string
	peakPortfolioValue      float64
	lastWithdrawalCheck     time.Time
	lastGlobalMetricsFetch  time.Time
	lastBTCDominance        float64
	openPositions           map[string]*utilities.Position
	pendingOrders           map[string]string // Maps orderID to assetPair
	stateMutex              sync.RWMutex
	sellReasons             map[string]string
	isCircuitBreakerTripped bool
	makerFeeRate            float64
	takerFeeRate            float64
	lastCMCFetch            time.Time
	lastAccountValue        float64   // Stores the most recently fetched account value.
	lastValueFetchTime      time.Time // Timestamp of the last fetch.
}

var (
	fngMutex   sync.RWMutex
	currentFNG dataprovider.FearGreedIndex
)

const accountValueCacheDuration = 15 * time.Second // Cache the value for 15 seconds

// getFreshAccountValue is a new helper method with caching to prevent API spam.
func (s *TradingState) getFreshAccountValue(ctx context.Context) (float64, error) {
	s.stateMutex.Lock() // Use a full lock since we might be writing to the state
	defer s.stateMutex.Unlock()

	// If the last fetch was within our cache duration, return the cached value.
	if time.Since(s.lastValueFetchTime) < accountValueCacheDuration {
		s.logger.LogDebug("getFreshAccountValue: Returning cached account value (%.2f)", s.lastAccountValue)
		return s.lastAccountValue, nil
	}

	// If the cache is stale, fetch a new value from the broker.
	s.logger.LogDebug("getFreshAccountValue: Cache stale, fetching fresh account value from broker...")
	newValue, err := s.broker.GetAccountValue(ctx, s.config.Trading.QuoteCurrency)
	if err != nil {
		// If the fetch fails, return the last known value to prevent errors, but don't update the timestamp.
		return s.lastAccountValue, err
	}

	// Update the cache with the new value and timestamp.
	s.lastAccountValue = newValue
	s.lastValueFetchTime = time.Now()

	return newValue, nil
}

func startFNGUpdater(ctx context.Context, fearGreedProvider dataprovider.FearGreedProvider, logger *utilities.Logger, updateInterval time.Duration) {
	if fearGreedProvider == nil {
		logger.LogWarn("F&G Updater: No FearGreed provider configured.")
		fngMutex.Lock()
		currentFNG = dataprovider.FearGreedIndex{Value: 50, Level: "Neutral", Timestamp: time.Now().Unix()}
		fngMutex.Unlock()
		return
	}
	fetchFNG := func() {
		fngData, err := fearGreedProvider.GetFearGreedIndex(ctx)
		if err != nil {
			logger.LogError("F&G Updater: Failed to fetch: %v", err)
			return
		}
		fngMutex.Lock()
		currentFNG = fngData
		fngMutex.Unlock()
		logger.LogInfo("F&G Updater: Updated Index: Value=%d, Level=%s", fngData.Value, fngData.Level)
	}
	go fetchFNG()
	ticker := time.NewTicker(updateInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fetchFNG()
			}
		}
	}()
}
func modulateConfigBySentiment(originalConfig utilities.TradingConfig, fngValue int, logger *utilities.Logger) utilities.TradingConfig {
	// Start with a copy of the original config to avoid modifying the global state.
	modulatedConfig := originalConfig

	if fngValue <= 25 { // Extreme Fear -> Time to be greedy.
		logger.LogInfo("Sentiment Overlay: EXTREME FEAR (FNG: %d). Activating aggressive buy mode.", fngValue)
		// Become more aggressive with buys: tighten safety order spacing and increase buy size.
		modulatedConfig.DcaAtrSpacingMultiplier *= 0.8 // Tighten the ATR spacing by 20% to buy dips faster.
		if originalConfig.ConsensusBuyMultiplier > 1.0 {
			modulatedConfig.ConsensusBuyMultiplier *= 1.25 // Increase the buy size multiplier by 25%.
		} else {
			modulatedConfig.ConsensusBuyMultiplier = 1.25 // Set a default aggressive multiplier if none exists.
		}
		return modulatedConfig
	}

	if fngValue >= 75 { // Extreme Greed -> Time to be fearful (conservative).
		logger.LogInfo("Sentiment Overlay: EXTREME GREED (FNG: %d). Activating conservative profit-taking mode.", fngValue)
		// Take profits faster and protect gains more aggressively.
		modulatedConfig.TakeProfitPercentage *= 0.8   // Reduce take-profit target by 20% to secure profits sooner.
		modulatedConfig.TrailingStopDeviation *= 0.75 // Tighten the trailing stop by 25%.
		return modulatedConfig
	}

	// Return the original, unmodified config if sentiment is neutral.
	return originalConfig
}
func getCurrentFNG() dataprovider.FearGreedIndex {
	fngMutex.RLock()
	defer fngMutex.RUnlock()
	return currentFNG
}
func Run(ctx context.Context, cfg *utilities.AppConfig, logger *utilities.Logger) error {
	if len(cfg.Trading.AssetPairs) == 0 {
		return errors.New("pre-flight check failed: no asset_pairs configured in config.json")
	}
	if cfg.Orders.WatcherIntervalSec <= 0 {
		return errors.New("pre-flight check failed: orders.watcher_interval_sec must be a positive integer")
	}

	discordClient := discord.NewClient(cfg.Discord.WebhookURL)
	discordClient.SendMessage(fmt.Sprintf("✅ **Snowballin' Bot v%s Starting Up**", cfg.Version))
	defer discordClient.SendMessage("🛑 **Snowballin' Bot Shutting Down**")

	logger.LogInfo("AppRun: Starting pre-flight checks...")

	sqliteCache, err := dataprovider.NewSQLiteCache(cfg.DB, logger)
	if err != nil {
		// The error will now be detailed and logged by NewSQLiteCache itself.
		return fmt.Errorf("pre-flight check failed: sqlite cache init failed: %w", err)
	}
	defer sqliteCache.Close()

	if err := sqliteCache.InitSchema(); err != nil {
		return fmt.Errorf("pre-flight check failed: could not initialize db schema: %w", err)
	}

	go sqliteCache.StartScheduledCleanup(24*time.Hour, "CoinGecko")
	go sqliteCache.StartScheduledCleanup(24*time.Hour, "CoinMarketCap")
	go sqliteCache.StartScheduledCleanup(24*time.Hour, "kraken")

	sharedHTTPClient := &http.Client{Timeout: time.Duration(15 * time.Second)}

	logger.LogInfo("Pre-Flight: Initializing and verifying broker (Kraken)...")
	krakenAdapter, krakenErr := krakenBroker.NewAdapter(&cfg.Kraken, sharedHTTPClient, logger, sqliteCache)
	if krakenErr != nil {
		return fmt.Errorf("pre-flight check failed: could not initialize Kraken adapter: %w", krakenErr)
	}
	if err := krakenAdapter.RefreshAssetInfo(ctx); err != nil {
		return fmt.Errorf("pre-flight check failed: could not refresh broker asset info: %w", err)
	}

	// --- ADDED: Fetch fees on startup ---
	logger.LogInfo("AppRun: Fetching account fee schedule from Kraken...")
	var makerFee, takerFee float64
	var feeErr error
	if len(cfg.Trading.AssetPairs) > 0 {
		// The GetTradeFees method is on the broker interface, which krakenAdapter implements.
		makerFee, takerFee, feeErr = krakenAdapter.GetTradeFees(ctx, cfg.Trading.AssetPairs[0])
		if feeErr != nil {
			logger.LogFatal("Could not fetch trading fees from Kraken, which is critical for profitability calculations. Halting. Error: %v", feeErr)
		}
		logger.LogInfo("AppRun: Successfully fetched fees. Maker: %.4f%%, Taker: %.4f%%", makerFee*100, takerFee*100)
	} else {
		return errors.New("cannot run without at least one asset pair to determine fees")
	}
	// --- END ADDED ---

	initialPortfolioValue, portfolioErr := krakenAdapter.GetAccountValue(ctx, cfg.Trading.QuoteCurrency)
	if portfolioErr != nil {
		return fmt.Errorf("pre-flight check failed: could not get account value from broker. Check API keys and permissions: %w", portfolioErr)
	}
	logger.LogInfo("Pre-Flight: Broker verification passed. Initial portfolio value: %.2f %s", initialPortfolioValue, cfg.Trading.QuoteCurrency)

	var configuredDPs []dataprovider.DataProvider
	providerNames := make(map[dataprovider.DataProvider]string)
	if cfg.Coingecko != nil && cfg.Coingecko.APIKey != "" {
		cgClient, _ := cg.NewClient(cfg, logger, sqliteCache)
		if cgClient != nil {
			configuredDPs = append(configuredDPs, cgClient)
			providerNames[cgClient] = "coingecko"
		}
	}
	if cfg.Coinmarketcap != nil && cfg.Coinmarketcap.APIKey != "" {
		cmcClient, _ := cmc.NewClient(cfg, logger, sqliteCache)
		if cmcClient != nil {
			configuredDPs = append(configuredDPs, cmcClient)
			providerNames[cmcClient] = "coinmarketcap"
		}
	}

	var activeDPs []dataprovider.DataProvider
	logger.LogInfo("Pre-Flight: Initializing and priming external data providers...")
	for _, dp := range configuredDPs {
		providerName := providerNames[dp]
		if err := dp.PrimeCache(ctx); err != nil {
			logger.LogError("Pre-Flight: Failed to prime cache for %s: %v. This provider will be disabled.", providerName, err)
			discordClient.SendMessage(fmt.Sprintf("⚠️ **Warning:** Data provider '%s' failed pre-flight check and will be disabled for this session.", providerName))
		} else {
			logger.LogInfo("Pre-Flight: %s provider primed successfully.", providerName)
			activeDPs = append(activeDPs, dp)
		}
	}

	if cfg.Preflight.PrimeHistoricalData && len(activeDPs) > 0 {
		logger.LogInfo("Pre-Flight Prime: Historical data priming is ENABLED. Fetching %d days of data...", cfg.Preflight.PrimingDays)
		primeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()

		for _, providerToUse := range activeDPs {
			providerName := providerNames[providerToUse]
			if providerName == "coingecko" {
				logger.LogInfo("Pre-Flight Prime: Using provider '%s' for historical data priming.", providerName)
				for _, pair := range cfg.Trading.AssetPairs {
					logger.LogInfo("Pre-Flight Prime: Priming data for %s...", pair)
					baseAsset := strings.Split(pair, "/")[0]
					coinID, err := providerToUse.GetCoinID(primeCtx, baseAsset)
					if err != nil {
						logger.LogError("Pre-Flight Prime: Could not get coin ID for %s using provider %s. Skipping. Error: %v", baseAsset, providerName, err)
						continue
					}
					for _, tf := range append(cfg.Consensus.MultiTimeframe.AdditionalTimeframes, cfg.Consensus.MultiTimeframe.BaseTimeframe) {
						logger.LogDebug("Pre-Flight Prime: Fetching %s data for %s...", tf, pair)
						err := providerToUse.PrimeHistoricalData(primeCtx, coinID, cfg.Trading.QuoteCurrency, tf, cfg.Preflight.PrimingDays)
						if err != nil {
							logger.LogWarn("Pre-Flight Prime: Failed to prime %s data for %s using %s. Continuing. Error: %v", tf, pair, providerName, err)
						}
						time.Sleep(2 * time.Second)
					}
				}
			}
		}
		logger.LogInfo("Pre-Flight Prime: Historical data priming complete.")
	} else {
		logger.LogInfo("Pre-Flight Prime: Historical data priming is DISABLED.")
	}

	var fearGreedProvider dataprovider.FearGreedProvider
	if cfg.FearGreed != nil && cfg.FearGreed.BaseURL != "" {
		fgClient, _ := dataprovider.NewFearGreedClient(cfg.FearGreed, logger, sharedHTTPClient)
		fearGreedProvider = fgClient
	}
	startFNGUpdater(ctx, fearGreedProvider, logger, 4*time.Hour)

	loadedPositions, err := sqliteCache.LoadPositions()
	if err != nil {
		return fmt.Errorf("failed to load open positions from db: %w", err)
	}
	loadedPendingOrders, err := sqliteCache.LoadPendingOrders()
	if err != nil {
		return fmt.Errorf("failed to load pending orders from db: %w", err)
	}
	logger.LogInfo("AppRun: Loaded %d open position(s) and %d pending order(s) from database.", len(loadedPositions), len(loadedPendingOrders))

	logger.LogInfo("Reconciliation: Verifying consistency between database state and exchange balances...")
	// --- MODIFIED: Pass the fetched fees into the temporary state for reconstruction ---
	tempStateForRecon := &TradingState{broker: krakenAdapter, logger: logger, config: cfg, makerFeeRate: makerFee, takerFeeRate: takerFee}

	// 1. Get ALL balances from the exchange.
	allBalances, err := krakenAdapter.GetAllBalances(ctx)
	if err != nil {
		return fmt.Errorf("reconciliation failed: could not get all balances from broker: %w", err)
	}

	// Create a quick lookup map of configured pairs.
	configuredPairs := make(map[string]bool)
	for _, p := range cfg.Trading.AssetPairs {
		configuredPairs[p] = true
	}

	// 2. Identify all potential orphaned pairs (configured pairs with significant balance but no DB position).
	var orphanedPairs []string
	for _, balance := range allBalances {
		if balance.Total < 1e-8 { // Skip zero or dust balances
			continue
		}

		assetPair := fmt.Sprintf("%s/%s", balance.Currency, cfg.Trading.QuoteCurrency)
		if !configuredPairs[assetPair] {
			logger.LogDebug("Reconciliation: Skipping balance for %s as it is not a configured trading pair.", balance.Currency)
			continue
		}

		_, hasPositionInDB := loadedPositions[assetPair]
		if !hasPositionInDB {
			// Fetch the current price to evaluate if the balance is dust.
			ticker, tickerErr := krakenAdapter.GetTicker(ctx, assetPair)
			if tickerErr != nil {
				logger.LogWarn("Reconciliation: Could not fetch ticker for %s to check for dust, proceeding with reconstruction attempt: %v", assetPair, tickerErr)
			} else {
				balanceInUSD := balance.Total * ticker.LastPrice
				if balanceInUSD < cfg.Trading.DustThresholdUSD {
					logger.LogInfo("Reconciliation: Ignoring orphaned dust balance of %f for %s (value: $%.4f).", balance.Total, assetPair, balanceInUSD)
					continue // Skip to the next balance
				}
			}
			orphanedPairs = append(orphanedPairs, assetPair)
		}
	}

	// 3. If there are any orphaned pairs, fetch ALL trade history ONCE (with pair="") for the lookback period.
	var allTrades []broker.Trade
	if len(orphanedPairs) > 0 {
		lookbackDuration := 90 * 24 * time.Hour // Consider lowering to 30*24*time.Hour if positions aren't old.
		startTime := time.Now().Add(-lookbackDuration)
		var err error
		allTrades, err = krakenAdapter.GetTrades(ctx, "", startTime) // Fetch ALL trades across pairs.
		if err != nil {
			return fmt.Errorf("reconciliation failed: could not get all trade history from broker: %w", err)
		}
		logger.LogInfo("Reconciliation: Fetched %d trades across all pairs for reconstruction.", len(allTrades))
	}

	// 4. For each orphaned pair, filter the trades and reconstruct.
	for _, assetPair := range orphanedPairs {
		// Find the balance for this pair (from allBalances).
		var actualBalance float64
		baseCurrency := strings.Split(assetPair, "/")[0]
		for _, bal := range allBalances {
			if bal.Currency == baseCurrency {
				actualBalance = bal.Total
				break
			}
		}

		// Filter allTrades for this specific assetPair.
		var pairTrades []broker.Trade
		for _, t := range allTrades {
			if t.Pair == assetPair {
				pairTrades = append(pairTrades, t)
			}
		}

		// Reconstruct using the filtered trades (modified ReconstructOrphanedPosition below).
		reconstructedPos, reconErr := ReconstructOrphanedPosition(ctx, tempStateForRecon, assetPair, actualBalance, pairTrades)
		if reconErr != nil {
			logger.LogFatal("ORPHANED POSITION DETECTED for %s, but reconstruction failed: %v. Manual intervention required.", assetPair, reconErr)
		}

		if err := sqliteCache.SavePosition(reconstructedPos); err != nil {
			logger.LogFatal("Failed to save reconstructed position for %s to database: %v. Halting.", assetPair, err)
		}
		loadedPositions[assetPair] = reconstructedPos
	}
	logger.LogInfo("Reconciliation: State verification complete.")

	state := &TradingState{
		broker:                  krakenAdapter,
		logger:                  logger,
		config:                  cfg,
		discordClient:           discordClient,
		cache:                   sqliteCache,
		activeDPs:               activeDPs,
		providerNames:           providerNames,
		peakPortfolioValue:      initialPortfolioValue,
		openPositions:           loadedPositions,
		pendingOrders:           loadedPendingOrders,
		isCircuitBreakerTripped: false,
		sellReasons:             make(map[string]string),
		makerFeeRate:            makerFee,
		takerFeeRate:            takerFee,
		lastCMCFetch:            time.Time{},
	}
	// Start the web server in a background goroutine
	go web.StartWebServer(ctx, state)
	// --- ADDED: Goroutine to refresh fees periodically ---
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				logger.LogInfo("AppRun: Refreshing Kraken fee schedule...")
				newMaker, newTaker, err := state.broker.GetTradeFees(ctx, state.config.Trading.AssetPairs[0])
				if err != nil {
					logger.LogError("Failed to refresh trading fees: %v", err)
				} else {
					state.stateMutex.Lock()
					state.makerFeeRate = newMaker
					state.takerFeeRate = newTaker
					state.stateMutex.Unlock()
					logger.LogInfo("AppRun: Successfully refreshed fees. New Maker: %.4f%%, New Taker: %.4f%%", newMaker*100, newTaker*100)
				}
			}
		}
	}()

	loopInterval := time.Duration(cfg.Orders.WatcherIntervalSec) * time.Second
	ticker := time.NewTicker(loopInterval)
	defer ticker.Stop()

	logger.LogInfo("AppRun: Pre-flight checks complete. Starting main trading loop every %s.", loopInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			processTradingCycle(ctx, state)
		}
	}
}
func ReconstructOrphanedPosition(ctx context.Context, state *TradingState, assetPair string, actualBalance float64, tradeHistory []broker.Trade) (*utilities.Position, error) {
	state.logger.LogWarn("Reconstruction: Attempting to reconstruct orphaned position for %s. Target balance: %f", assetPair, actualBalance)

	if len(tradeHistory) == 0 {
		return nil, fmt.Errorf("no trade history found for this asset, cannot reconstruct")
	}

	// Sort descending: most recent first
	sort.Slice(tradeHistory, func(i, j int) bool {
		return tradeHistory[i].Timestamp.After(tradeHistory[j].Timestamp)
	})

	var accumulatedVolume float64
	var positionTrades []broker.Trade

	const tolerance = 1e-8

	for _, trade := range tradeHistory {
		if trade.Side == "sell" {
			continue // Ignore sells, as per observation they may not appear or reset history
		}
		if trade.Side != "buy" {
			continue // Skip non-buy/sell
		}

		if accumulatedVolume >= actualBalance-tolerance {
			break // Stop once we have enough
		}

		positionTrades = append([]broker.Trade{trade}, positionTrades...) // Prepend recent
		accumulatedVolume += trade.Volume
	}

	if len(positionTrades) == 0 {
		return nil, fmt.Errorf("found trade history for %s, but could not isolate a sequence of buys for the current balance of %f", assetPair, actualBalance)
	}

	// Handle overshoot by scaling the oldest (least recent, now at index 0)
	if accumulatedVolume > actualBalance+tolerance {
		state.logger.LogWarn("Reconstruction [%s]: Accumulated volume (%.8f) overshot actual balance (%.8f). Adjusting oldest trade.", assetPair, accumulatedVolume, actualBalance)

		oldestTrade := &positionTrades[0]
		overshootVolume := accumulatedVolume - actualBalance
		neededVolumeFromOldest := oldestTrade.Volume - overshootVolume

		if neededVolumeFromOldest < 0 {
			return nil, fmt.Errorf("reconstruction logic failed: needed volume from oldest trade is negative")
		}

		scaleFactor := neededVolumeFromOldest / oldestTrade.Volume
		state.logger.LogInfo("Reconstruction [%s]: Scaling oldest trade (Vol: %.8f) by factor %.4f to match balance.", assetPair, oldestTrade.Volume, scaleFactor)

		oldestTrade.Cost *= scaleFactor
		oldestTrade.Fee *= scaleFactor
		oldestTrade.Volume = neededVolumeFromOldest

		accumulatedVolume = actualBalance // Exact now
	}

	// Fetch current price for final dust check (though scaling should prevent need)
	ticker, tickerErr := state.broker.GetTicker(ctx, assetPair)
	if tickerErr != nil {
		return nil, fmt.Errorf("failed to get ticker for %s during reconstruction: %w", assetPair, tickerErr)
	}
	currentPrice := ticker.LastPrice
	if currentPrice <= 0 {
		return nil, fmt.Errorf("invalid current price (%.2f) for %s during reconstruction", currentPrice, assetPair)
	}

	diff := math.Abs(accumulatedVolume - actualBalance)
	valueDiff := diff * currentPrice

	if diff > tolerance {
		if valueDiff < state.config.Trading.DustThresholdUSD {
			state.logger.LogWarn("Reconstruction [%s]: Adjusting for minor dust difference (%.8f, value: $%.4f)", assetPair, diff, valueDiff)
			accumulatedVolume = actualBalance
		} else {
			return nil, fmt.Errorf("reconstructed volume %.8f does not match actual balance %.8f for %s (difference %.8f exceeds tolerance; value $%.4f)", accumulatedVolume, actualBalance, assetPair, diff, valueDiff)
		}
	}

	var totalCost, totalFees float64
	baseOrderTrade := positionTrades[len(positionTrades)-1] // Oldest is now last after prepend

	for _, trade := range positionTrades {
		totalCost += trade.Cost
		totalFees += trade.Fee
	}
	totalTrueBuyCost := totalCost + totalFees

	finalVolume := accumulatedVolume
	if finalVolume <= tolerance {
		return nil, errors.New("reconstructed trades have zero or negative total volume, cannot reconstruct")
	}

	filledSafetyOrders := len(positionTrades) - 1

	reconstructedPosition := &utilities.Position{
		AssetPair:          assetPair,
		EntryTimestamp:     baseOrderTrade.Timestamp,
		AveragePrice:       totalCost / accumulatedVolume,
		TotalVolume:        finalVolume,
		BaseOrderPrice:     baseOrderTrade.Price,
		BaseOrderSize:      baseOrderTrade.Cost,
		FilledSafetyOrders: filledSafetyOrders,
		IsDcaActive:        true,
		BrokerOrderID:      baseOrderTrade.OrderID,
	}
	reconstructedPosition.CurrentTakeProfit = calculateFeeAwareTakeProfitPrice(reconstructedPosition, state, totalTrueBuyCost)

	state.logger.LogInfo("Reconstruction SUCCESS for %s. Avg Price: %.2f, Vol: %.8f, SOs: %d, Fee-Aware TP: %.2f",
		assetPair, reconstructedPosition.AveragePrice, reconstructedPosition.TotalVolume, reconstructedPosition.FilledSafetyOrders, reconstructedPosition.CurrentTakeProfit)

	return reconstructedPosition, nil
}
func reapStaleOrders(ctx context.Context, state *TradingState) {
	state.stateMutex.RLock()
	if len(state.pendingOrders) == 0 {
		state.stateMutex.RUnlock()
		return
	}
	orderIDsToCheck := make([]string, 0, len(state.pendingOrders))
	for id := range state.pendingOrders {
		orderIDsToCheck = append(orderIDsToCheck, id)
	}
	state.stateMutex.RUnlock()

	maxAge := time.Duration(state.config.Orders.MaxOrderAgeMinutesForGridBase) * time.Minute
	if maxAge <= 0 {
		return
	}

	for _, orderID := range orderIDsToCheck {
		order, err := state.broker.GetOrderStatus(ctx, orderID)
		if err != nil {
			continue
		}
		if order.Status == "open" && time.Since(order.TimePlaced) > maxAge {
			state.logger.LogWarn("Reaper: Order %s for %s is stale (age: %s). Cancelling.", orderID, order.Pair, time.Since(order.TimePlaced))
			err := state.broker.CancelOrder(ctx, orderID)
			if err != nil {
				state.logger.LogError("Reaper: Failed to cancel stale order %s: %v", orderID, err)
			} else {
				state.logger.LogInfo("Reaper: Successfully cancelled stale order %s.", orderID)
				state.discordClient.SendMessage(fmt.Sprintf("ℹ️ Canceled stale order for %s. ID: `%s`", order.Pair, orderID))
			}
		}
	}
}
func processPendingOrders(ctx context.Context, state *TradingState) {
	state.stateMutex.RLock()
	if len(state.pendingOrders) == 0 {
		state.stateMutex.RUnlock()
		return
	}
	orderIDsToCheck := make([]string, 0, len(state.pendingOrders))
	for id := range state.pendingOrders {
		orderIDsToCheck = append(orderIDsToCheck, id)
	}
	state.stateMutex.RUnlock()

	if len(orderIDsToCheck) > 0 {
		state.logger.LogInfo("PendingOrders: Checking status of %d pending order(s).", len(orderIDsToCheck))
	}

	for _, orderID := range orderIDsToCheck {
		order, err := state.broker.GetOrderStatus(ctx, orderID)
		if err != nil {
			state.logger.LogError("PendingOrders: Could not get status for order %s: %v", orderID, err)
			continue
		}

		if strings.EqualFold(order.Status, "partially filled") && order.ExecutedVol > 0 {
			state.logger.LogWarn("PendingOrders: Order %s for %s is PARTIALLY FILLED (%.4f / %.4f).", orderID, order.Pair, order.ExecutedVol, order.RequestedVol)
			state.stateMutex.Lock()
			assetPair, isPending := state.pendingOrders[orderID]
			if isPending {
				updatePositionFromFill(state, order, assetPair)
				if err := state.broker.CancelOrder(ctx, orderID); err != nil {
					state.logger.LogError("PendingOrders: Failed to cancel partially filled order %s before re-placing: %v", orderID, err)
					state.stateMutex.Unlock()
					continue
				}
				tf := state.config.Consensus.MultiTimeframe.BaseTimeframe
				krakenInterval, _ := utilities.ConvertTFToKrakenInterval(tf)
				bars, barsErr := state.broker.GetLastNOHLCVBars(ctx, assetPair, krakenInterval, state.config.Indicators.ATRPeriod+1)
				if barsErr != nil {
					state.logger.LogError("PendingOrders: Failed to get bars for ATR to handle partial fill: %v", barsErr)
					state.stateMutex.Unlock()
					continue
				}
				atr, atrErr := strategy.CalculateATR(bars, state.config.Indicators.ATRPeriod)
				if atrErr != nil {
					state.logger.LogError("PendingOrders: Failed to calculate ATR to handle partial fill: %v", atrErr)
					state.stateMutex.Unlock()
					continue
				}

				remainingVol := order.RequestedVol - order.ExecutedVol
				newPrice := strategy.HandlePartialFills(order.RequestedVol, order.ExecutedVol, order.Price, atr, 1.5)

				newOrderID, placeErr := state.broker.PlaceOrder(ctx, assetPair, order.Side, "limit", remainingVol, newPrice, 0, "")
				if placeErr != nil {
					state.logger.LogError("PendingOrders: Failed to place new order for remainder of partial fill: %v", placeErr)
				} else {
					state.logger.LogInfo("PendingOrders: Re-placed order for remaining %.4f of %s with new ID %s at price %.2f", remainingVol, assetPair, newOrderID, newPrice)
					state.pendingOrders[newOrderID] = assetPair
					_ = state.cache.SavePendingOrder(newOrderID, assetPair)
				}
				delete(state.pendingOrders, orderID)
				_ = state.cache.DeletePendingOrder(orderID)
			}
			state.stateMutex.Unlock()
			continue
		}

		if strings.EqualFold(order.Status, "closed") {
			state.logger.LogInfo("PendingOrders: Order %s for %s filled!", orderID, order.Pair)
			state.stateMutex.Lock()
			assetPair, isPending := state.pendingOrders[orderID]
			if isPending {
				updatePositionFromFill(state, order, assetPair)
				delete(state.pendingOrders, orderID)
				if err := state.cache.DeletePendingOrder(orderID); err != nil {
					state.logger.LogError("PendingOrders: Failed to delete pending order %s from DB: %v", orderID, err)
				}
			}
			state.stateMutex.Unlock()

		} else if strings.EqualFold(order.Status, "canceled") || strings.EqualFold(order.Status, "expired") {
			state.logger.LogWarn("PendingOrders: Order %s for %s has failed (status: %s).", orderID, state.pendingOrders[orderID], order.Status)
			state.discordClient.SendMessage(fmt.Sprintf("⚠️ Order for %s failed! Status: %s, ID: `%s`", state.pendingOrders[orderID], order.Status, orderID))
			state.stateMutex.Lock()
			if pos, ok := state.openPositions[state.pendingOrders[orderID]]; ok {
				pos.IsDcaActive = true
				state.openPositions[state.pendingOrders[orderID]] = pos
				_ = state.cache.SavePosition(pos)
			}
			delete(state.pendingOrders, orderID)
			if err := state.cache.DeletePendingOrder(orderID); err != nil {
				state.logger.LogError("PendingOrders: Failed to delete failed order %s from DB: %v", orderID, err)
			}
			state.stateMutex.Unlock()
		}
	}
}
func updatePositionFromFill(state *TradingState, order broker.Order, assetPair string) {
	if strings.EqualFold(order.Side, "buy") {
		position, hasPosition := state.openPositions[assetPair]
		if !hasPosition {
			baseOrderSizeInQuote := order.Price * order.ExecutedVol
			newPosition := &utilities.Position{
				AssetPair:          assetPair,
				EntryTimestamp:     order.TimeCompleted,
				AveragePrice:       order.Price,
				TotalVolume:        order.ExecutedVol,
				BaseOrderPrice:     order.Price,
				FilledSafetyOrders: 0,
				IsDcaActive:        true,
				BrokerOrderID:      order.ID,
				BaseOrderSize:      baseOrderSizeInQuote,
			}
			// --- MODIFIED: Use the new fee-aware calculator for a new position ---
			newPosition.CurrentTakeProfit = calculateFeeAwareTakeProfitPrice(newPosition, state, 0)
			state.openPositions[assetPair] = newPosition
			state.discordClient.NotifyOrderFilled(order, fmt.Sprintf("✅ New Position Opened (Base Order)\nTP: %.2f", newPosition.CurrentTakeProfit))
			if err := state.cache.SavePosition(newPosition); err != nil {
				state.logger.LogError("PendingOrders: Failed to save new position to DB for %s: %v", assetPair, err)
			}
		} else {
			oldVolume := position.TotalVolume
			oldAvgPrice := position.AveragePrice
			newVolume := order.ExecutedVol
			newPrice := order.Price
			position.TotalVolume += newVolume
			position.AveragePrice = ((oldAvgPrice * oldVolume) + (newPrice * newVolume)) / position.TotalVolume
			position.FilledSafetyOrders++
			// --- MODIFIED: Use the new fee-aware calculator for a DCA fill ---
			position.CurrentTakeProfit = calculateFeeAwareTakeProfitPrice(position, state, 0)
			position.IsDcaActive = true
			state.openPositions[assetPair] = position
			state.discordClient.NotifyOrderFilled(order, fmt.Sprintf("⤵️ Safety Order #%d Filled\nNew Avg Price: %.2f\nNew TP: %.2f", position.FilledSafetyOrders, position.AveragePrice, position.CurrentTakeProfit))
			if err := state.cache.SavePosition(position); err != nil {
				state.logger.LogError("PendingOrders: Failed to update position in DB for %s: %v", assetPair, err)
			}
		}
	} else { // Sell order
		if pos, ok := state.openPositions[assetPair]; ok {
			if math.Abs(pos.TotalVolume-order.ExecutedVol) < 1e-8 {
				// Get the reason for the sell, which was stored when the order was placed.
				// We can safely access the map because the parent function holds the lock.
				reason, hasReason := state.sellReasons[order.ID]
				if !hasReason {
					reason = "Unknown" // Fallback in case the reason was not stored.
				}

				// Calculate profit, accounting for fees.
				totalBuyCost := (pos.AveragePrice * pos.TotalVolume) * (1 + state.makerFeeRate)
				netProceeds := order.Cost // The 'cost' of a sell order is the proceeds.
				profit := netProceeds - totalBuyCost
				profitPercent := (profit / totalBuyCost) * 100

				// Send the notification with the reason included.
				state.discordClient.NotifyOrderFilled(order, fmt.Sprintf("💰 **Position Closed**\n**Reason: %s**\nNet Profit: `%.2f %s` (`%.2f%%`)", reason, profit, state.config.Trading.QuoteCurrency, profitPercent))

				// Clean up the state.
				delete(state.openPositions, assetPair)
				delete(state.sellReasons, order.ID) // Clean up the reasons map.
				_ = state.cache.DeletePosition(assetPair)
			} else {
				profit := (order.Price - pos.AveragePrice) * order.ExecutedVol
				pos.TotalVolume -= order.ExecutedVol
				state.discordClient.NotifyOrderFilled(order, fmt.Sprintf("💰 Partial Take-Profit Hit\nSold %.4f %s for a profit of %.2f %s. Trailing stop is now active on the remainder.", order.ExecutedVol, strings.Split(pos.AssetPair, "/")[0], profit, state.config.Trading.QuoteCurrency))
				_ = state.cache.SavePosition(pos)
			}
		}
	}
}
func processTradingCycle(ctx context.Context, state *TradingState) {
	state.logger.LogInfo("-------------------- New Trading Cycle --------------------")

	state.stateMutex.RLock()
	if state.isCircuitBreakerTripped {
		state.stateMutex.RUnlock()
		state.logger.LogWarn("Circuit breaker is active. Halting all new trading operations. Manual restart required.")
		return
	}
	state.stateMutex.RUnlock()

	currentPortfolioValue, valErr := state.getFreshAccountValue(ctx)
	if valErr != nil {
		state.logger.LogError("Cycle: Could not update portfolio value: %v", valErr)
		return
	}

	state.stateMutex.Lock()
	if currentPortfolioValue > state.peakPortfolioValue {
		state.peakPortfolioValue = currentPortfolioValue
	}
	state.stateMutex.Unlock()

	if state.config.CircuitBreaker.Enabled && state.peakPortfolioValue > 0 {
		drawdownPercent := state.config.CircuitBreaker.DrawdownThresholdPercent / 100.0
		drawdownThresholdValue := state.peakPortfolioValue * (1.0 - drawdownPercent)
		if currentPortfolioValue <= drawdownThresholdValue {
			reason := fmt.Sprintf("Portfolio value (%.2f) dropped below drawdown threshold (%.2f). Max drawdown of %.2f%% from peak (%.2f) was exceeded.",
				currentPortfolioValue, drawdownThresholdValue, state.config.CircuitBreaker.DrawdownThresholdPercent, state.peakPortfolioValue)
			state.logger.LogWarn("CIRCUIT BREAKER: %s", reason)
			state.stateMutex.Lock()
			state.isCircuitBreakerTripped = true
			state.stateMutex.Unlock()
			liquidateAllPositions(ctx, state, reason)
			return
		}
	}

	if time.Since(state.lastGlobalMetricsFetch) > 30*time.Minute {
		state.logger.LogInfo("GlobalMetrics: Fetching updated global market data...")
		if len(state.activeDPs) > 0 {
			provider := state.activeDPs[0]
			globalData, err := provider.GetGlobalMarketData(ctx)
			if err != nil {
				state.logger.LogError("GlobalMetrics: Failed to fetch global data: %v", err)
			} else {
				state.stateMutex.Lock()
				state.lastBTCDominance = globalData.BTCDominance
				state.lastGlobalMetricsFetch = time.Now()
				state.stateMutex.Unlock()
				state.logger.LogInfo("GlobalMetrics: Updated BTC Dominance to: %.2f%%", globalData.BTCDominance)
			}
		}
	}

	processPendingOrders(ctx, state)
	reapStaleOrders(ctx, state)

	// [MODIFIED] Get the current FNG value once per cycle.
	currentFNGValue := getCurrentFNG().Value

	// --- Thread-Safe Configuration Snapshot ---
	state.stateMutex.RLock()
	// Create a snapshot of the config and asset pairs for this cycle.
	// This prevents the config from changing mid-cycle if updated via the UI.
	cycleConfig := *state.config
	assetPairsForCycle := make([]string, len(cycleConfig.Trading.AssetPairs))
	copy(assetPairsForCycle, cycleConfig.Trading.AssetPairs)
	state.stateMutex.RUnlock()
	// --- End Snapshot ---

	for _, assetPair := range assetPairsForCycle {
		// Modulate the *copied* config for this specific trading cycle.
		activeAppConfig := cycleConfig // Start with the snapshot
		activeAppConfig.Trading = modulateConfigBySentiment(cycleConfig.Trading, currentFNGValue, state.logger)

		consolidatedData, err := gatherConsolidatedData(ctx, state, assetPair, currentPortfolioValue)
		if err != nil {
			state.logger.LogError("Cycle [%s]: Failed to gather consolidated data: %v", assetPair, err)
			continue
		}

		stratInstance := strategy.NewStrategy(state.logger)
		// [MODIFIED] Pass the new sentiment-adjusted config to the strategy.
		signals, _ := stratInstance.GenerateSignals(ctx, *consolidatedData, activeAppConfig)

		if len(signals) > 0 && signals[0].Direction != "hold" {
			mainSignal := signals[0]
			state.logger.LogInfo("GenerateSignals: %s -> %s - Reason: %s", assetPair, strings.ToUpper(mainSignal.Direction), mainSignal.Reason)
		} else {
			holdReason := "Conditions for buy/sell not met."
			if len(signals) > 0 {
				holdReason = signals[0].Reason
			}
			state.logger.LogInfo("GenerateSignals: %s -> HOLD - Reason: %s", assetPair, holdReason)
		}

		state.stateMutex.RLock()
		position, hasPosition := state.openPositions[assetPair]
		state.stateMutex.RUnlock()

		isEffectivelyDust := false
		if hasPosition {
			var currentPrice float64
			for _, pData := range consolidatedData.ProvidersData {
				if pData.Name == "kraken" {
					currentPrice = pData.CurrentPrice
					break
				}
			}

			if currentPrice > 0 {
				positionValueUSD := position.TotalVolume * currentPrice
				// [MODIFIED] Use the active (potentially modulated) config for dust threshold.
				dustThreshold := 1.0
				if activeAppConfig.Trading.DustThresholdUSD > 0 {
					dustThreshold = activeAppConfig.Trading.DustThresholdUSD
				}
				if positionValueUSD < dustThreshold {
					isEffectivelyDust = true
					state.logger.LogInfo("Position for %s is considered dust (value: $%.4f). Allowing new entry check.", assetPair, positionValueUSD)
				}
			}
		}

		if hasPosition && !isEffectivelyDust {
			// [MODIFIED] Pass the sentiment-adjusted config.
			manageOpenPosition(ctx, state, position, signals, consolidatedData, &activeAppConfig)
		} else {
			// [MODIFIED] Pass the sentiment-adjusted config.
			seekEntryOpportunity(ctx, state, assetPair, signals, consolidatedData, &activeAppConfig)
		}
	}
}
func manageOpenPosition(ctx context.Context, state *TradingState, pos *utilities.Position, signals []strategy.StrategySignal, consolidatedData *strategy.ConsolidatedMarketPicture, cfg *utilities.AppConfig) {
	state.logger.LogInfo("ManagePosition [%s]: Managing position. AvgPrice: %.2f, Vol: %.8f, SOs: %d, TP: %.2f",
		pos.AssetPair, pos.AveragePrice, pos.TotalVolume, pos.FilledSafetyOrders, pos.CurrentTakeProfit)

	for _, sig := range signals {
		if sig.Direction == "buy" || sig.Direction == "predictive_buy" {
			state.logger.LogInfo("ManagePosition [%s]: New '%s' signal received. Adding to existing position.", pos.AssetPair, strings.ToUpper(sig.Direction))

			orderPrice := sig.RecommendedPrice
			orderSizeInBase := sig.CalculatedSize

			if orderSizeInBase <= 0 {
				state.logger.LogWarn("ManagePosition [%s]: Calculated size for add-on order is zero. Falling back to base order size.", pos.AssetPair)
				// [MODIFIED] Use the passed-in, sentiment-adjusted config.
				orderSizeInBase = cfg.Trading.BaseOrderSize / orderPrice
			}
			if orderPrice <= 0 {
				state.logger.LogError("ManagePosition [%s]: Invalid add-on order price (<= 0). Aborting.", pos.AssetPair)
				return
			}

			// --- ADDED: Balance Check for Add-on Buy ---
			orderTotalValue := orderSizeInBase * orderPrice
			quoteCurrency := cfg.Trading.QuoteCurrency
			balance, balanceErr := state.broker.GetBalance(ctx, quoteCurrency)
			if balanceErr != nil {
				state.logger.LogError("ManagePosition [%s]: Could not verify account balance before placing add-on order: %v. Aborting.", pos.AssetPair, balanceErr)
				return
			}
			if orderTotalValue > balance.Total {
				state.logger.LogWarn("ManagePosition [%s]: Insufficient funds to place add-on order. Required: ~%.2f %s, Available: %.2f %s. Skipping.",
					pos.AssetPair, orderTotalValue, quoteCurrency, balance.Total, quoteCurrency)
				return
			}
			// --- END: Balance Check ---

			orderID, placeErr := state.broker.PlaceOrder(ctx, pos.AssetPair, "buy", "limit", orderSizeInBase, orderPrice, 0, "")
			if placeErr != nil {
				state.logger.LogError("ManagePosition [%s]: Failed to place add-on buy order: %v", pos.AssetPair, placeErr)
			} else {
				state.logger.LogInfo("ManagePosition [%s]: Placed add-on order ID %s at %.2f.", pos.AssetPair, orderID, orderPrice)
				baseAsset := strings.Split(pos.AssetPair, "/")[0]
				quoteAsset := strings.Split(pos.AssetPair, "/")[1]
				message := fmt.Sprintf("➕ **Adding to Position**\n**Pair:** %s\n**Size:** `%.4f %s`\n**Price:** `%.2f %s`\n**Reason:** %s\n",
					pos.AssetPair, orderSizeInBase, baseAsset, orderPrice, quoteAsset, sig.Reason)
				state.discordClient.SendMessage(message)
				state.stateMutex.Lock()
				state.pendingOrders[orderID] = pos.AssetPair
				_ = state.cache.SavePendingOrder(orderID, pos.AssetPair)
				state.stateMutex.Unlock()
			}
			return
		}
	}

	var currentPrice float64
	for _, pData := range consolidatedData.ProvidersData {
		if pData.Name == "kraken" {
			currentPrice = pData.CurrentPrice
			break
		}
	}
	if currentPrice == 0 {
		state.logger.LogError("ManagePosition [%s]: Could not find Kraken price in consolidated data.", pos.AssetPair)
		return
	}

	stratInstance := strategy.NewStrategy(state.logger)
	// [MODIFIED] Use the passed-in, sentiment-adjusted config.
	exitSignal, shouldExit := stratInstance.GenerateExitSignal(ctx, *consolidatedData, *cfg)
	if shouldExit && exitSignal.Direction == "sell" {
		state.logger.LogWarn("!!! [SELL] signal for %s (Strategy Exit). Reason: %s. Placing market sell order.", pos.AssetPair, exitSignal.Reason)
		orderID, err := state.broker.PlaceOrder(ctx, pos.AssetPair, "sell", "market", pos.TotalVolume, 0, 0, "")
		if err != nil {
			state.logger.LogError("ManagePosition [%s]: Failed to place market sell order for strategy exit: %v", pos.AssetPair, err)
		} else {
			state.stateMutex.Lock()
			state.pendingOrders[orderID] = pos.AssetPair
			// --- ADD THIS LINE ---
			state.sellReasons[orderID] = exitSignal.Reason
			_ = state.cache.SavePendingOrder(orderID, pos.AssetPair)
			state.stateMutex.Unlock()
		}
		return
	}

	// [MODIFIED] Use the passed-in, sentiment-adjusted config.
	if cfg.Trading.TrailingStopEnabled && pos.IsTrailingActive {
		if currentPrice > pos.PeakPriceSinceTP {
			pos.PeakPriceSinceTP = currentPrice
			state.logger.LogInfo("ManagePosition [%s]: Trailing stop active. New peak price: %.2f", pos.AssetPair, pos.PeakPriceSinceTP)
			if err := state.cache.SavePosition(pos); err != nil {
				state.logger.LogError("ManagePosition [%s]: Failed to save updated peak price to DB: %v", pos.AssetPair, err)
			}
		}
		// [MODIFIED] Use the sentiment-adjusted trailing stop deviation.
		trailingStopPrice := pos.PeakPriceSinceTP * (1.0 - (cfg.Trading.TrailingStopDeviation / 100.0))
		if currentPrice <= trailingStopPrice {
			state.logger.LogInfo("ManagePosition [%s]: TRAILING STOP-LOSS HIT at %.2f (Peak was %.2f). Placing market sell order.", pos.AssetPair, currentPrice, pos.PeakPriceSinceTP)
			orderID, err := state.broker.PlaceOrder(ctx, pos.AssetPair, "sell", "market", pos.TotalVolume, 0, 0, "")
			if err != nil {
				//...
			} else {
				state.stateMutex.Lock()
				state.pendingOrders[orderID] = pos.AssetPair
				// --- ADD THIS LINE ---
				state.sellReasons[orderID] = "Trailing Stop-Loss Hit"
				_ = state.cache.SavePendingOrder(orderID, pos.AssetPair)
				state.stateMutex.Unlock()
			}
			return
		}
	} else if currentPrice >= pos.CurrentTakeProfit && !pos.IsTrailingActive {
		// [MODIFIED] Use the passed-in, sentiment-adjusted config.
		if cfg.Trading.TrailingStopEnabled {
			partialSellPercent := cfg.Trading.TakeProfitPartialSellPercent / 100.0
			if partialSellPercent > 0 && partialSellPercent < 1.0 {
				volumeToSell := pos.TotalVolume * partialSellPercent
				state.logger.LogInfo("ManagePosition [%s]: HYBRID TAKE-PROFIT HIT at %.2f. Selling %.2f%% of position.", pos.AssetPair, currentPrice, cfg.Trading.TakeProfitPartialSellPercent)
				orderID, err := state.broker.PlaceOrder(ctx, pos.AssetPair, "sell", "market", volumeToSell, 0, 0, "")
				if err != nil {
					state.logger.LogError("ManagePosition [%s]: Failed to place partial market sell order: %v", pos.AssetPair, err)
					return
				}
				state.stateMutex.Lock()
				state.pendingOrders[orderID] = pos.AssetPair
				_ = state.cache.SavePendingOrder(orderID, pos.AssetPair)
				state.stateMutex.Unlock()
			}
			pos.IsTrailingActive = true
			pos.PeakPriceSinceTP = currentPrice
			state.logger.LogInfo("ManagePosition [%s]: Activating trailing stop-loss on remaining position.", pos.AssetPair)
			if err := state.cache.SavePosition(pos); err != nil {
				state.logger.LogError("ManagePosition [%s]: Failed to save activated trailing stop to DB: %v", pos.AssetPair, err)
			}
			return
		} else {
			state.logger.LogInfo("ManagePosition [%s]: TAKE-PROFIT HIT at %.2f. Placing market sell order (trailing stop disabled).", pos.AssetPair, currentPrice)
			orderID, err := state.broker.PlaceOrder(ctx, pos.AssetPair, "sell", "market", pos.TotalVolume, 0, 0, "")
			if err != nil {
				//...
			} else {
				state.stateMutex.Lock()
				state.pendingOrders[orderID] = pos.AssetPair
				// --- ADD THIS LINE ---
				state.sellReasons[orderID] = "Take-Profit Target Hit"
				_ = state.cache.SavePendingOrder(orderID, pos.AssetPair)
				state.stateMutex.Unlock()
			}
			return
		}
	}

	// [MODIFIED] Use the passed-in, sentiment-adjusted config for DCA logic.
	if pos.IsDcaActive && pos.FilledSafetyOrders < cfg.Trading.MaxSafetyOrders {
		var shouldPlaceSafetyOrder bool
		var nextSafetyOrderPrice float64
		if cfg.Trading.DcaSpacingMode == "atr" {
			state.logger.LogDebug("ManagePosition [%s]: Using ATR spacing mode.", pos.AssetPair)
			tf := cfg.Consensus.MultiTimeframe.BaseTimeframe
			krakenInterval, _ := utilities.ConvertTFToKrakenInterval(tf)
			bars, err := state.broker.GetLastNOHLCVBars(ctx, pos.AssetPair, krakenInterval, cfg.Trading.DcaAtrPeriod+1)
			if err != nil || len(bars) < cfg.Trading.DcaAtrPeriod+1 {
				state.logger.LogError("ManagePosition [%s]: Could not fetch enough bars for ATR calculation: %v", pos.AssetPair, err)
			} else {
				atrValue, err := strategy.CalculateATR(bars, cfg.Trading.DcaAtrPeriod)
				if err != nil {
					state.logger.LogError("ManagePosition [%s]: Error calculating ATR: %v", pos.AssetPair, err)
				} else {
					// [MODIFIED] Use the sentiment-adjusted ATR spacing multiplier.
					priceDeviation := atrValue * cfg.Trading.DcaAtrSpacingMultiplier * math.Pow(cfg.Trading.SafetyOrderStepScale, float64(pos.FilledSafetyOrders))
					lastFilledPrice := pos.AveragePrice
					if pos.FilledSafetyOrders == 0 {
						lastFilledPrice = pos.BaseOrderPrice
					}
					nextSafetyOrderPrice = lastFilledPrice - priceDeviation
					if currentPrice <= nextSafetyOrderPrice {
						shouldPlaceSafetyOrder = true
					}
				}
			}
		} else {
			state.logger.LogDebug("ManagePosition [%s]: Using percentage spacing mode.", pos.AssetPair)
			nextSONumber := pos.FilledSafetyOrders + 1
			var totalDeviationPercentage float64
			currentStep := cfg.Trading.PriceDeviationToOpenSafetyOrders
			for i := 0; i < nextSONumber; i++ {
				totalDeviationPercentage += currentStep
				currentStep *= cfg.Trading.SafetyOrderStepScale
			}
			nextSafetyOrderPrice = pos.BaseOrderPrice * (1 - (totalDeviationPercentage / 100.0))
			if currentPrice <= nextSafetyOrderPrice {
				shouldPlaceSafetyOrder = true
			}
		}

		if shouldPlaceSafetyOrder {
			strategicSafetyPrice := strategy.FindBestLimitPrice(consolidatedData.BrokerOrderBook, nextSafetyOrderPrice, 0.5)
			if strategicSafetyPrice != nextSafetyOrderPrice {
				state.logger.LogInfo("ManagePosition [%s]: Found strategic safety order price %.2f based on order book (Original: %.2f)", pos.AssetPair, strategicSafetyPrice, nextSafetyOrderPrice)
			}
			nextSONumber := pos.FilledSafetyOrders + 1
			orderSizeInQuote := pos.BaseOrderSize * math.Pow(cfg.Trading.SafetyOrderVolumeScale, float64(nextSONumber))
			orderSizeInBase := orderSizeInQuote / strategicSafetyPrice

			// --- ADDED: Balance Check for Safety Order ---
			orderTotalValue := orderSizeInBase * strategicSafetyPrice
			quoteCurrency := cfg.Trading.QuoteCurrency
			balance, balanceErr := state.broker.GetBalance(ctx, quoteCurrency)
			if balanceErr != nil {
				state.logger.LogError("ManagePosition [%s]: Could not verify account balance before placing safety order: %v. Aborting.", pos.AssetPair, balanceErr)
				return
			}
			if orderTotalValue > balance.Total {
				state.logger.LogWarn("ManagePosition [%s]: Insufficient funds to place Safety Order #%d. Required: ~%.2f %s, Available: %.2f %s. Skipping.",
					pos.AssetPair, nextSONumber, orderTotalValue, quoteCurrency, balance.Total, quoteCurrency)
				return
			}
			// --- END: Balance Check ---

			state.logger.LogInfo("ManagePosition [%s]: Price condition met for Safety Order #%d. Placing limit buy.", pos.AssetPair, nextSONumber)
			orderID, err := state.broker.PlaceOrder(ctx, pos.AssetPair, "buy", "limit", orderSizeInBase, strategicSafetyPrice, 0, "")
			if err != nil {
				state.logger.LogError("ManagePosition [%s]: Failed to place safety order: %v", pos.AssetPair, err)
			} else {
				state.logger.LogInfo("ManagePosition [%s]: Placed Safety Order #%d. ID: %s", pos.AssetPair, nextSONumber, orderID)
				state.stateMutex.Lock()
				state.pendingOrders[orderID] = pos.AssetPair
				_ = state.cache.SavePendingOrder(orderID, pos.AssetPair)
				pos.IsDcaActive = false
				state.openPositions[pos.AssetPair] = pos
				_ = state.cache.SavePosition(pos)
				state.stateMutex.Unlock()
			}
		}
	}
}

// seekEntryOpportunity evaluates signals to open a new position.
// This is the corrected version of the function.
func seekEntryOpportunity(ctx context.Context, state *TradingState, assetPair string, signals []strategy.StrategySignal, consolidatedData *strategy.ConsolidatedMarketPicture, cfg *utilities.AppConfig) {
	for _, sig := range signals {
		if strings.EqualFold(sig.Direction, "buy") || strings.EqualFold(sig.Direction, "predictive_buy") {
			state.logger.LogInfo("SeekEntry [%s]: %s signal confirmed. Calculating order...", assetPair, strings.ToUpper(sig.Direction))

			var orderPrice float64
			var orderSizeInBase float64

			var currentPrice float64
			for _, pData := range consolidatedData.ProvidersData {
				if pData.Name == "kraken" {
					currentPrice = pData.CurrentPrice
					break
				}
			}
			if currentPrice == 0 {
				state.logger.LogError("SeekEntry [%s]: Could not determine current price from broker. Aborting order.", assetPair)
				continue
			}

			if strings.EqualFold(sig.Direction, "buy") {
				orderPrice = sig.RecommendedPrice
				orderSizeInBase = sig.CalculatedSize
				if cfg.Trading.ConsensusBuyMultiplier > 1.0 {
					orderSizeInBase *= cfg.Trading.ConsensusBuyMultiplier
					state.logger.LogInfo("SeekEntry [%s]: Applying x%.2f multiplier to consensus buy. New size: %.4f", assetPair, cfg.Trading.ConsensusBuyMultiplier, orderSizeInBase)
				}
			} else { // This is a "predictive_buy"
				orderPrice = sig.RecommendedPrice
				orderSizeInBase = cfg.Trading.BaseOrderSize / orderPrice
				state.logger.LogInfo("SeekEntry [%s]: Predictive buy placing order at %.2f based on detected support level from strategy signal.", assetPair, orderPrice)
			}

			if orderSizeInBase <= 0 || orderPrice <= 0 {
				state.logger.LogError("SeekEntry [%s]: Invalid order parameters (size=%.4f, price=%.2f). Aborting.", assetPair, orderSizeInBase, orderPrice)
				continue
			}

			// --- CORRECTED: Pre-flight balance check ---
			orderTotalValue := orderSizeInBase * orderPrice
			quoteCurrency := cfg.Trading.QuoteCurrency

			// Use the specific GetBalance function for efficiency and correctness.
			balance, balanceErr := state.broker.GetBalance(ctx, quoteCurrency)
			if balanceErr != nil {
				state.logger.LogError("SeekEntry [%s]: Could not verify account balance for '%s' before placing order: %v. Aborting.", assetPair, quoteCurrency, balanceErr)
				continue // Skip to the next signal
			}

			// The 'balance' object has a 'Total' field. We assume Total is the available balance for trading.
			availableQuoteBalance := balance.Total

			if orderTotalValue > availableQuoteBalance {
				state.logger.LogWarn("SeekEntry [%s]: Insufficient funds to place buy order. Required: ~%.2f %s, Available: %.2f %s. The order will be skipped.",
					assetPair, orderTotalValue, quoteCurrency, availableQuoteBalance, quoteCurrency)
				continue // Skip this signal and avoid the PlaceOrder call
			}
			// --- END: Pre-flight balance check ---

			orderID, placeErr := state.broker.PlaceOrder(ctx, assetPair, "buy", "limit", orderSizeInBase, orderPrice, 0, "")
			if placeErr != nil {
				state.logger.LogError("SeekEntry [%s]: Place order failed: %v", assetPair, placeErr)
			} else {
				state.logger.LogInfo("SeekEntry [%s]: Placed order ID %s at %.2f. Now tracking.", assetPair, orderID, orderPrice)

				// --- Unified Discord Notification ---
				baseAsset := strings.Split(assetPair, "/")[0]
				quoteAsset := strings.Split(assetPair, "/")[1]
				var message string

				if strings.EqualFold(sig.Direction, "predictive_buy") {
					message = fmt.Sprintf("🧠 **Predictive Buy Order Placed**\n**Pair:** %s\n**Size:** `%.4f %s`\n**Price:** `%.2f %s`\n**Order ID:** `%s`\n**Reason:** %s",
						assetPair, orderSizeInBase, baseAsset, orderPrice, quoteAsset, orderID, sig.Reason)
				} else { // This handles the standard 'buy' signal
					message = fmt.Sprintf("✅ **Buy Order Placed**\n**Pair:** %s\n**Size:** `%.4f %s`\n**Price:** `%.2f %s`\n**Order ID:** `%s`\n**Reason:** %s",
						assetPair, orderSizeInBase, baseAsset, orderPrice, quoteAsset, orderID, sig.Reason)
				}
				state.discordClient.SendMessage(message)
				// --- End Unified Discord Notification ---

				state.stateMutex.Lock()
				state.pendingOrders[orderID] = assetPair
				_ = state.cache.SavePendingOrder(orderID, assetPair)
				state.stateMutex.Unlock()
			}
			break // Exit after processing the first valid buy signal
		}
	}
}
func gatherConsolidatedData(ctx context.Context, state *TradingState, assetPair string, currentPortfolioValue float64) (*strategy.ConsolidatedMarketPicture, error) {
	// --- 1. Initialize the data container ---
	state.stateMutex.RLock()
	consolidatedData := &strategy.ConsolidatedMarketPicture{
		AssetPair:          assetPair,
		ProvidersData:      make([]strategy.ProviderData, 0, 1+len(state.activeDPs)),
		BTCDominance:       state.lastBTCDominance,
		FearGreedIndex:     getCurrentFNG(),
		PortfolioValue:     currentPortfolioValue,
		PeakPortfolioValue: state.peakPortfolioValue,
		PrimaryOHLCVByTF:   make(map[string][]utilities.OHLCVBar),
	}
	state.stateMutex.RUnlock()

	// --- 2. Fetch primary data from the broker (Kraken) ---
	// Ticker for the most recent price.
	krakenTicker, tickerErr := state.broker.GetTicker(ctx, assetPair)
	if tickerErr != nil {
		// Log as a warning because other providers might have the price.
		state.logger.LogWarn("gatherConsolidatedData [%s]: could not get ticker from broker: %v", assetPair, tickerErr)
	}

	// Order book for liquidity analysis.
	krakenOrderBook, obErr := state.broker.GetOrderBook(ctx, assetPair, 20)
	if obErr != nil {
		state.logger.LogWarn("gatherConsolidatedData [%s]: could not get order book from broker: %v", assetPair, obErr)
	}
	consolidatedData.BrokerOrderBook = krakenOrderBook

	// --- 3. Fetch all required OHLCV timeframes from the broker ---
	tfCfg := state.config.Consensus.MultiTimeframe
	allTFs := append([]string{tfCfg.BaseTimeframe}, tfCfg.AdditionalTimeframes...)

	for idx, tfString := range allTFs {
		if idx >= len(tfCfg.TFLookbackLengths) {
			state.logger.LogWarn("gatherConsolidatedData [%s]: Mismatch between timeframes and lookback lengths in config. Stopping fetch.", assetPair)
			break
		}
		lookback := tfCfg.TFLookbackLengths[idx]
		krakenInterval, intervalErr := utilities.ConvertTFToKrakenInterval(tfString)
		if intervalErr != nil {
			state.logger.LogError("gatherConsolidatedData [%s]: Invalid interval '%s' for broker: %v", assetPair, tfString, intervalErr)
			continue
		}

		bars, barsErr := state.broker.GetLastNOHLCVBars(ctx, assetPair, krakenInterval, lookback)
		if barsErr != nil {
			state.logger.LogError("gatherConsolidatedData [%s]: Broker failed to provide OHLCV for %s: %v", assetPair, tfString, barsErr)
			continue // Skip this timeframe if it fails
		}
		// This map holds the data from the primary source (the broker).
		consolidatedData.PrimaryOHLCVByTF[tfString] = bars
	}

	// --- 4. Verify we have the essential base timeframe data from the broker ---
	if _, ok := consolidatedData.PrimaryOHLCVByTF[tfCfg.BaseTimeframe]; !ok {
		return nil, errors.New("could not fetch base timeframe OHLCV data from broker; cannot proceed")
	}

	// --- 5. Add the broker's complete data to the list of providers ---
	// This uses the new `OHLCVByTF` map field in the ProviderData struct.
	consolidatedData.ProvidersData = append(consolidatedData.ProvidersData, strategy.ProviderData{
		Name:         "kraken",
		Weight:       state.config.DataProviderWeights["kraken"],
		CurrentPrice: krakenTicker.LastPrice,
		OHLCVByTF:    consolidatedData.PrimaryOHLCVByTF,
	})

	// --- 6. Fetch data from all other active external providers (CoinGecko, CoinMarketCap, etc.) ---
	baseAssetSymbol := strings.Split(assetPair, "/")[0]
	for _, dp := range state.activeDPs {
		providerName := state.providerNames[dp]
		shouldThrottleCMC := providerName == "coinmarketcap" && time.Since(state.lastCMCFetch) < 30*time.Minute
		if shouldThrottleCMC {
			state.logger.LogInfo("gatherConsolidatedData [%s]: Throttling CMC—last pull was recent (%s ago). Relying on internal provider cache to avoid unnecessary API calls.", assetPair, time.Since(state.lastCMCFetch))
		}
		providerCoinID, idErr := dp.GetCoinID(ctx, baseAssetSymbol)
		if idErr != nil {
			state.logger.LogError("gatherConsolidatedData [%s]: Could not get coin ID for provider %s: %v", assetPair, providerName, idErr)
			continue
		}
		// Get the current price from this provider.
		extMarketData, mdErr := dp.GetMarketData(ctx, []string{providerCoinID}, state.config.Trading.QuoteCurrency)
		var currentPrice float64
		if mdErr == nil && len(extMarketData) > 0 {
			currentPrice = extMarketData[0].CurrentPrice
		} else if mdErr != nil {
			state.logger.LogWarn("gatherConsolidatedData [%s]: Provider %s failed to get market data: %v", assetPair, providerName, mdErr)
		}

		// **THE FIX**: For each external provider, fetch all configured timeframes.
		extOHLCVByTF := make(map[string][]utilities.OHLCVBar)
		for _, tfString := range allTFs {
			bars, ohlcvErr := dp.GetOHLCVHistorical(ctx, providerCoinID, state.config.Trading.QuoteCurrency, tfString)
			if ohlcvErr != nil {
				state.logger.LogWarn("gatherConsolidatedData [%s]: Provider %s failed to get OHLCV for %s: %v", assetPair, providerName, tfString, ohlcvErr)
				continue // Skip this timeframe, but try the next one
			}
			extOHLCVByTF[tfString] = bars
		}

		// Add this provider's complete data to the list.
		consolidatedData.ProvidersData = append(consolidatedData.ProvidersData, strategy.ProviderData{
			Name:         providerName,
			Weight:       state.config.DataProviderWeights[providerName],
			CurrentPrice: currentPrice,
			OHLCVByTF:    extOHLCVByTF,
		})

		// Update timestamp ONLY for CMC and if not throttled (i.e., an actual fetch likely occurred) and no major errors
		if providerName == "coinmarketcap" && !shouldThrottleCMC && mdErr == nil {
			// Additional check: Assume success if at least one timeframe fetched without error (simplified; could check all)
			state.stateMutex.Lock()
			state.lastCMCFetch = time.Now()
			state.stateMutex.Unlock()
		}
	}

	return consolidatedData, nil
}
func liquidateAllPositions(ctx context.Context, state *TradingState, reason string) {
	state.stateMutex.Lock()
	defer state.stateMutex.Unlock()

	if len(state.openPositions) == 0 {
		state.logger.LogInfo("CircuitBreaker: Triggered, but no open positions to liquidate.")
		return
	}

	state.discordClient.SendMessage(fmt.Sprintf("🚨 **CIRCUIT BREAKER TRIGGERED** 🚨\n**Reason:** %s\nAttempting to liquidate all open positions.", reason))

	assetPairsToLiquidate := make([]string, 0, len(state.openPositions))
	for assetPair := range state.openPositions {
		assetPairsToLiquidate = append(assetPairsToLiquidate, assetPair)
	}

	for _, assetPair := range assetPairsToLiquidate {
		position := state.openPositions[assetPair]
		if position.TotalVolume > 0 {
			state.logger.LogWarn("CircuitBreaker: Liquidating position for %s (Volume: %f)", assetPair, position.TotalVolume)
			_, err := state.broker.PlaceOrder(ctx, assetPair, "sell", "market", position.TotalVolume, 0, 0, "")
			if err != nil {
				state.logger.LogError("CircuitBreaker: Failed to place liquidation order for %s: %v. Manual check required.", assetPair, err)
				state.discordClient.SendMessage(fmt.Sprintf("🔥 **LIQUIDATION FAILED for %s! Manual intervention required!** Error: %v", assetPair, err))
			} else {
				state.logger.LogInfo("CircuitBreaker: Liquidation order successfully placed for %s.", assetPair)
				state.discordClient.SendMessage(fmt.Sprintf("✅ Liquidation order placed for %s.", assetPair))
			}
		}
		delete(state.openPositions, assetPair)
		_ = state.cache.DeletePosition(assetPair)
		for orderID, pair := range state.pendingOrders {
			if pair == assetPair {
				_ = state.broker.CancelOrder(ctx, orderID)
				delete(state.pendingOrders, orderID)
				_ = state.cache.DeletePendingOrder(orderID)
			}
		}
	}
}
func calculateFeeAwareTakeProfitPrice(pos *utilities.Position, state *TradingState, totalBuyCostWithFees float64) float64 {
	targetProfitRate := state.config.Trading.TakeProfitPercentage / 100.0

	// If totalBuyCostWithFees is not provided (i.e., for a live order), we estimate it using the live maker fee rate.
	// We assume all our limit buys are maker orders.
	if totalBuyCostWithFees == 0 {
		totalBuyCostWithFees = (pos.AveragePrice * pos.TotalVolume) * (1 + state.makerFeeRate)
	}

	// To be profitable, the proceeds from the sale (after the taker fee) must exceed the total buy cost.
	// Formula: SellPrice * Volume * (1 - TakerFee) > TotalBuyCost
	// Therefore, the break-even price is: TotalBuyCost / (Volume * (1 - TakerFee))
	breakEvenPrice := totalBuyCostWithFees / (pos.TotalVolume * (1 - state.takerFeeRate))

	// The final target price adds the desired profit margin to the break-even price.
	finalTargetPrice := breakEvenPrice * (1 + targetProfitRate)

	return finalTargetPrice
}

// --- web.AppController Interface Implementation ---

// GetDashboardData returns a snapshot of data needed for the web dashboard.
// GetDashboardData is updated to calculate and include P/L data itself for performance.
func (s *TradingState) GetDashboardData(ctx context.Context) web.DashboardData {
	// --- Step 1: Get live account value from the broker ---
	// MODIFIED: Use the 'ctx' from the function argument instead of context.TODO()
	currentValue, err := s.getFreshAccountValue(ctx)
	if err != nil {
		s.logger.LogError("GetDashboardData: Failed to get current account value from broker: %v", err)
		// Return last known peak value as a fallback to prevent the UI from looking broken.
		currentValue = s.peakPortfolioValue
	}

	// --- Step 2: Get a thread-safe copy of positions and other data ---
	s.stateMutex.RLock()
	positionsCopy := make(map[string]utilities.Position)
	assetPairs := make([]string, 0, len(s.openPositions))
	for k, v := range s.openPositions {
		positionsCopy[k] = *v
		assetPairs = append(assetPairs, k)
	}
	quoteCurrency := s.config.Trading.QuoteCurrency
	version := s.config.Version
	s.stateMutex.RUnlock() // Unlock early before the next network call

	// --- Step 3: Efficiently calculate P/L for all positions at once ---
	var totalPL float64
	if len(assetPairs) > 0 {
		// Use the GetTickers method for a single, efficient batch request.
		// MODIFIED: Pass the context to the broker call.
		tickers, tickerErr := s.broker.GetTickers(ctx, assetPairs)
		if tickerErr == nil {
			for pair, pos := range positionsCopy {
				if ticker, ok := tickers[pair]; ok {
					currentPosValue := pos.TotalVolume * ticker.LastPrice
					buyValue := pos.TotalVolume * pos.AveragePrice
					pl := currentPosValue - buyValue
					pos.UnrealizedPL = pl
					if buyValue > 0 {
						pos.UnrealizedPLPercent = (pl / buyValue) * 100
					}
					positionsCopy[pair] = pos // Update the copy with P/L info
					totalPL += pl
				}
			}
		} else {
			s.logger.LogError("GetDashboardData: Failed to get tickers for P/L calculation: %v", tickerErr)
		}
	}

	// --- Step 4: Assemble and return the final data structure ---
	return web.DashboardData{
		ActivePositions:   positionsCopy,
		PortfolioValue:    currentValue, // Use the live value
		QuoteCurrency:     quoteCurrency,
		Version:           version,
		TotalUnrealizedPL: totalPL,
	}
}

// GetAssetDetailData implements the missing method for the web.AppController interface.
// This resolves the compiler error.
func (s *TradingState) GetAssetDetailData(assetPair string) (web.AssetDetailData, error) {
	s.stateMutex.RLock()
	var positionCopy *utilities.Position
	if pos, ok := s.openPositions[assetPair]; ok {
		pCopy := *pos // Create a copy of the struct to avoid data races
		positionCopy = &pCopy
	}
	peakValue := s.peakPortfolioValue
	cfg := *s.config
	s.stateMutex.RUnlock()

	// Since this interface method doesn't receive a context, we use context.
	// This is a signal that we should consider refactoring the interface to accept one later.
	ctx := context.TODO()

	consolidatedData, err := gatherConsolidatedData(ctx, s, assetPair, peakValue)
	if err != nil {
		return web.AssetDetailData{}, fmt.Errorf("could not gather market data for %s: %w", assetPair, err)
	}

	// Use primary (broker) bars for final indicator calculation for the UI
	primaryBars, ok := consolidatedData.PrimaryOHLCVByTF[cfg.Consensus.MultiTimeframe.BaseTimeframe]
	if !ok {
		return web.AssetDetailData{}, fmt.Errorf("missing primary bars for base timeframe '%s'", cfg.Consensus.MultiTimeframe.BaseTimeframe)
	}

	// Calculate indicators to display
	rsi, stochRSI, macdHist, _, volSpike, liqHunt, _, _, _, _ := strategy.CalculateIndicators(primaryBars, cfg)

	// Populate the data structure required by the web package
	indicatorMap := make(map[string]string)
	indicatorMap["RSI"] = fmt.Sprintf("%.2f", rsi)
	indicatorMap["StochRSI"] = fmt.Sprintf("%.2f", stochRSI)
	indicatorMap["MACD Hist"] = fmt.Sprintf("%.4f", macdHist)

	// Create human-readable analysis points for the UI
	analysisSlice := []string{}
	if rsi > 70 {
		analysisSlice = append(analysisSlice, "RSI is overbought (> 70), indicating potential for a pullback.")
	} else if rsi < 30 {
		analysisSlice = append(analysisSlice, "RSI is oversold (< 30), indicating potential for a bounce.")
	} else {
		analysisSlice = append(analysisSlice, "RSI is in neutral territory.")
	}
	if volSpike {
		analysisSlice = append(analysisSlice, "A recent volume spike was detected.")
	}
	if liqHunt {
		analysisSlice = append(analysisSlice, "A potential liquidity hunt candle was detected.")
	}

	var currentPrice float64
	if len(consolidatedData.ProvidersData) > 0 {
		// Assumes the first provider is the broker (kraken) for the most accurate price
		currentPrice = consolidatedData.ProvidersData[0].CurrentPrice
	}

	detailData := web.AssetDetailData{
		AssetPair:       assetPair,
		Position:        positionCopy,
		CurrentPrice:    currentPrice,
		IndicatorValues: indicatorMap,
		Analysis:        analysisSlice,
	}

	return detailData, nil
}

// GetConfig returns a thread-safe copy of the current application config.
func (s *TradingState) GetConfig() utilities.AppConfig {
	s.stateMutex.RLock()
	defer s.stateMutex.RUnlock()
	return *s.config // Return a copy
}

// UpdateAndSaveConfig atomically updates the in-memory config and saves it to disk.
func (s *TradingState) UpdateAndSaveConfig(newConfig utilities.AppConfig) error {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	// Save the updated configuration to disk
	configPath, _ := filepath.Abs("config/config.json")
	file, err := os.Create(configPath)
	if err != nil {
		s.logger.LogError("Failed to open config file for writing: %v", err)
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // For pretty printing
	if err := encoder.Encode(newConfig); err != nil {
		s.logger.LogError("Failed to write new config to file: %v", err)
		return err
	}

	// Atomically swap the live config with our new one
	s.config = &newConfig

	s.logger.LogInfo("Configuration updated and saved successfully via web UI.")
	return nil
}

// Logger returns the application's logger instance.
func (s *TradingState) Logger() *utilities.Logger {
	return s.logger
}

// Mutex returns the application's main RWMutex.
func (s *TradingState) Mutex() *sync.RWMutex {
	return &s.stateMutex
}
