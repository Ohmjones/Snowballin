package app

import (
	"Snowballin/dataprovider"
	cg "Snowballin/dataprovider/coingecko"
	cmc "Snowballin/dataprovider/coinmarketcap"
	"Snowballin/notification/discord"
	"Snowballin/pkg/broker"
	krakenBroker "Snowballin/pkg/broker/kraken"
	"Snowballin/pkg/mapper"
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
	assetMapper             *mapper.AssetMapper
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
	cgTrendingData          []dataprovider.TrendingCoin
	cmcTrendingData         []dataprovider.TrendingCoin
	lastGainersLosersFetch  time.Time
	topGainers              []dataprovider.MarketData
	topLosers               []dataprovider.MarketData
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

// startSentimentUpdater is a background goroutine to fetch and cache trending data
func startSentimentUpdater(ctx context.Context, state *TradingState, updateInterval time.Duration) {
	fetchSentimentData := func() {
		state.logger.LogInfo("Sentiment Updater: Fetching trending data from providers...")
		var wg sync.WaitGroup
		for _, dp := range state.activeDPs {
			wg.Add(1)
			go func(provider dataprovider.DataProvider) {
				defer wg.Done()
				providerName := state.providerNames[provider]
				trending, err := provider.GetTrendingSearches(ctx)
				if err != nil {
					state.logger.LogWarn("Sentiment Updater: Failed to fetch trending data from %s: %v", providerName, err)
					return
				}

				state.stateMutex.Lock()
				switch providerName {
				case "coingecko":
					state.cgTrendingData = trending
				case "coinmarketcap":
					state.cmcTrendingData = trending
				}
				state.stateMutex.Unlock()
				state.logger.LogInfo("Sentiment Updater: Successfully updated trending data from %s (%d coins).", providerName, len(trending))
			}(dp)
		}
		wg.Wait()
	}

	// Fetch immediately on startup
	go fetchSentimentData()

	// Then fetch periodically
	ticker := time.NewTicker(updateInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fetchSentimentData()
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
	// 1. Create the underlying Kraken Client first. This instance will be shared.
	krakenClient := krakenBroker.NewClient(&cfg.Kraken, sharedHTTPClient, logger)

	// 2. Pass the client into the Adapter.
	krakenAdapter, krakenErr := krakenBroker.NewAdapter(&cfg.Kraken, krakenClient, logger, sqliteCache)
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
	var cgClient, cmcClient dataprovider.DataProvider // Keep separate handles for the mapper

	if cfg.Coingecko != nil && cfg.Coingecko.APIKey != "" {
		cg, err := cg.NewClient(cfg, logger, sqliteCache)
		if err == nil && cg != nil {
			cgClient = cg
			configuredDPs = append(configuredDPs, cgClient)
			providerNames[cgClient] = "coingecko"
		}
	}
	if cfg.Coinmarketcap != nil && cfg.Coinmarketcap.APIKey != "" {
		cmc, err := cmc.NewClient(cfg, logger, sqliteCache)
		if err == nil && cmc != nil {
			cmcClient = cmc
			configuredDPs = append(configuredDPs, cmcClient)
			providerNames[cmcClient] = "coinmarketcap"
		}
	}

	logger.LogInfo("Pre-Flight: Initializing the Asset Mapper...")
	assetMapper := mapper.NewAssetMapper(sqliteCache, logger, krakenClient, cgClient, cmcClient, cfg)

	// --- PRE-FLIGHT ASSET MAPPING ---
	logger.LogInfo("Pre-Flight: Verifying asset identities for configured pairs...")
	for _, pair := range cfg.Trading.AssetPairs {
		symbol := strings.Split(pair, "/")[0]
		_, err := assetMapper.GetIdentity(ctx, symbol)
		if err != nil {
			// This is a fatal error because the bot cannot function without knowing its assets.
			return fmt.Errorf("pre-flight check failed: could not map asset '%s'. Please check symbol. Error: %w", symbol, err)
		}
	}
	logger.LogInfo("Pre-Flight: All configured assets successfully mapped.")

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

	// 3. For each orphaned pair, fetch its specific trade history and reconstruct it.
	for _, assetPair := range orphanedPairs {
		logger.LogInfo("Reconciliation: Found orphaned pair %s. Fetching its trade history...", assetPair)
		lookbackDuration := 90 * 24 * time.Hour
		startTime := time.Now().Add(-lookbackDuration)

		// Fetch history ONLY for the specific orphaned pair.
		pairTrades, err := krakenAdapter.GetTrades(ctx, assetPair, startTime)
		if err != nil {
			logger.LogFatal("Reconciliation failed for %s: could not get its trade history from broker: %v. Manual intervention required.", assetPair, err)
			continue // Using LogFatal will stop the app, which is safer here. Use LogError to just skip.
		}
		logger.LogInfo("Reconciliation: Fetched %d trades for %s to attempt reconstruction.", len(pairTrades), assetPair)

		// Find the current balance for this pair.
		var actualBalance float64
		baseCurrency := strings.Split(assetPair, "/")[0]
		for _, bal := range allBalances {
			if bal.Currency == baseCurrency {
				actualBalance = bal.Total
				break
			}
		}

		// Reconstruct using the fetched trades. No filtering is needed now.
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
		assetMapper:             assetMapper,
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

	// --- NEW: Start the sentiment updater ---
	startSentimentUpdater(ctx, state, 30*time.Minute)

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

				if err := state.broker.CancelOrder(ctx, orderID); err != nil {
					state.logger.LogError("PendingOrders: Failed to cancel partially filled order %s before re-placing: %v", orderID, err)
				}

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

	//
	// Pre-Cycle State & Safety Checks
	//

	state.stateMutex.RLock()
	if state.isCircuitBreakerTripped {
		state.stateMutex.RUnlock()
		state.logger.LogWarn("Circuit breaker is active. Halting all new trading operations.")
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

	// Circuit Breaker Drawdown Check
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

	//
	// Data Fetching & Maintenance
	//

	// Fetch Global Market Data (less frequently)
	if time.Since(state.lastGlobalMetricsFetch) > 30*time.Minute {
		state.logger.LogInfo("GlobalMetrics: Fetching updated global market data...")
		if len(state.activeDPs) > 0 {
			provider := state.activeDPs[0] // Use primary provider for this
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

	// Fetch Gainers/Losers for Market Sentiment (less frequently to save API credits)
	if time.Since(state.lastGainersLosersFetch) > 25*time.Minute {
		state.logger.LogInfo("Market Sentiment: Updating top gainers and losers from CoinMarketCap...")
		var cmcProvider dataprovider.DataProvider
		for dp, name := range state.providerNames {
			if name == "coinmarketcap" {
				cmcProvider = dp
				break
			}
		}

		if cmcProvider != nil {
			// Fetch top 50 to get a good sample size for the ratio calculation
			gainers, losers, err := cmcProvider.GetGainersAndLosers(ctx, state.config.Trading.QuoteCurrency, 50)
			if err != nil {
				state.logger.LogError("Market Sentiment: Failed to fetch gainers/losers: %v", err)
			} else {
				state.stateMutex.Lock()
				state.topGainers = gainers
				state.topLosers = losers
				state.lastGainersLosersFetch = time.Now()
				state.stateMutex.Unlock()
				state.logger.LogInfo("Market Sentiment: Successfully updated gainers/losers data.")
			}
		}
	}

	processPendingOrders(ctx, state)
	reapStaleOrders(ctx, state)

	//
	// Asset Scanning & Strategy Execution
	//

	currentFNGValue := getCurrentFNG().Value

	// --- Thread-Safe Configuration & Asset List Snapshot ---
	state.stateMutex.RLock()
	cycleConfig := *state.config
	staticAssetPairs := make([]string, len(cycleConfig.Trading.AssetPairs))
	copy(staticAssetPairs, cycleConfig.Trading.AssetPairs)
	state.stateMutex.RUnlock()

	finalAssetPairs := staticAssetPairs // Start with the static list from config

	if cycleConfig.Trading.UseDynamicAssetScanning {
		state.logger.LogInfo("Dynamic Scanning: Enabled. Fetching top %d assets.", cycleConfig.Trading.DynamicAssetScanTopN)
		var provider dataprovider.DataProvider
		// Prefer CoinGecko for this as it's generally faster for market cap lists
		for dp, name := range state.providerNames {
			if name == "coingecko" {
				provider = dp
				break
			}
		}

		if provider != nil {
			dynamicAssetPairs, err := provider.GetTopAssetsByMarketCap(ctx, cycleConfig.Trading.QuoteCurrency, cycleConfig.Trading.DynamicAssetScanTopN)
			if err != nil {
				state.logger.LogError("Dynamic Scanning: Failed to fetch top assets: %v. Using static list only.", err)
			} else {
				uniquePairs := make(map[string]bool)
				for _, pair := range staticAssetPairs {
					uniquePairs[pair] = true
				}
				for _, pair := range dynamicAssetPairs {
					if _, exists := uniquePairs[pair]; !exists {
						// Pre-flight check the discovered asset before adding it to the trading list
						symbol := strings.Split(pair, "/")[0]
						_, mapErr := state.assetMapper.GetIdentity(ctx, symbol)
						if mapErr != nil {
							state.logger.LogWarn("Dynamic Scanning: Could not map discovered asset '%s', skipping. Reason: %v", pair, mapErr)
							continue
						}
						finalAssetPairs = append(finalAssetPairs, pair)
						uniquePairs[pair] = true
					}
				}
				state.logger.LogInfo("Dynamic Scanning: Combined asset list now contains %d unique pairs.", len(finalAssetPairs))
			}
		} else {
			state.logger.LogWarn("Dynamic Scanning: No suitable provider (CoinGecko) found to perform scan.")
		}
	}

	// --- CRITICAL FIX: Iterate over the 'finalAssetPairs' list ---
	for _, assetPair := range finalAssetPairs {
		// Modulate the config for this specific trading cycle based on Fear & Greed index
		activeAppConfig := cycleConfig
		activeAppConfig.Trading = modulateConfigBySentiment(cycleConfig.Trading, currentFNGValue, state.logger)

		consolidatedData, err := gatherConsolidatedData(ctx, state, assetPair, currentPortfolioValue)
		if err != nil {
			state.logger.LogError("Cycle [%s]: Failed to gather consolidated data: %v", assetPair, err)
			continue
		}

		stratInstance := strategy.NewStrategy(state.logger)
		signals, _ := stratInstance.GenerateSignals(ctx, *consolidatedData, activeAppConfig)

		// Logging the outcome of the signal generation
		if len(signals) > 0 && signals[0].Direction != "hold" {
			mainSignal := signals[0]
			state.logger.LogInfo("Signal [%s]: %s -> Reason: %s", assetPair, utilities.ColorCyan+strings.ToUpper(mainSignal.Direction)+utilities.ColorReset, mainSignal.Reason)
		} else {
			state.logger.LogInfo("Signal [%s]: %s", assetPair, utilities.ColorWhite+"HOLD"+utilities.ColorReset)
		}

		state.stateMutex.RLock()
		position, hasPosition := state.openPositions[assetPair]
		state.stateMutex.RUnlock()

		// Check if the position is effectively dust, allowing a new entry to be sought
		isEffectivelyDust := false
		if hasPosition {
			var currentPrice float64
			for _, pData := range consolidatedData.ProvidersData {
				// Use the broker's price for the most accurate dust calculation
				if pData.Name == "kraken" {
					currentPrice = pData.CurrentPrice
					break
				}
			}
			if currentPrice > 0 {
				positionValueUSD := position.TotalVolume * currentPrice
				dustThreshold := activeAppConfig.Trading.DustThresholdUSD
				if positionValueUSD < dustThreshold {
					isEffectivelyDust = true
					state.logger.LogInfo("Position for %s is considered dust (value: $%.4f). Allowing new entry check.", assetPair, positionValueUSD)
				}
			}
		}

		if hasPosition && !isEffectivelyDust {
			manageOpenPosition(ctx, state, position, signals, consolidatedData, &activeAppConfig)
		} else {
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

		// === SMA-BASED DCA LOGIC ===
		state.logger.LogDebug("ManagePosition [%s]: Using SMA-based DCA spacing.", pos.AssetPair)

		// Fetch the necessary OHLCV bars for the SMA calculation.
		tf := cfg.Consensus.MultiTimeframe.BaseTimeframe
		krakenInterval, _ := utilities.ConvertTFToKrakenInterval(tf)
		bars, err := state.broker.GetLastNOHLCVBars(ctx, pos.AssetPair, krakenInterval, cfg.Trading.SMAPeriodForDCA)
		if err != nil || len(bars) < cfg.Trading.SMAPeriodForDCA {
			state.logger.LogError("ManagePosition [%s]: Could not fetch enough bars for SMA calculation: %v", pos.AssetPair, err)
		} else {
			// Extract the close prices and calculate the SMA.
			closePrices := strategy.ExtractCloses(bars)
			smaValue := strategy.CalculateSMA(closePrices, cfg.Trading.SMAPeriodForDCA)

			// The trigger for a new safety order is a dip to the SMA level.
			// The price for the order is set at a small deviation below the SMA to act as a limit order.
			smaDeviationPercent := cfg.Trading.PriceDeviationToOpenSafetyOrders
			nextSafetyOrderPrice = smaValue * (1 - (smaDeviationPercent / 100.0))

			// Check if the current price is at or below this target.
			if currentPrice <= nextSafetyOrderPrice {
				shouldPlaceSafetyOrder = true
				state.logger.LogInfo("ManagePosition [%s]: DCA trigger. Price (%.2f) at or below SMA target (%.2f).", pos.AssetPair, currentPrice, nextSafetyOrderPrice)
			}
		}

		// === END OF NEW LOGIC ===
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
func gatherConsolidatedData(ctx context.Context, state *TradingState, assetPair string, portfolioValue float64) (*strategy.ConsolidatedMarketPicture, error) {
	// --- 1. Get the unified Asset Identity from the mapper ---
	baseAssetSymbol := strings.Split(assetPair, "/")[0]
	identity, err := state.assetMapper.GetIdentity(ctx, baseAssetSymbol)
	if err != nil {
		return nil, fmt.Errorf("gatherConsolidatedData: could not get asset identity for %s: %w", baseAssetSymbol, err)
	}

	// --- 2. Initialize data containers and get a config snapshot ---
	state.stateMutex.RLock()
	cfg := *state.config
	// Pre-allocate slice capacity for better performance
	providersData := make([]strategy.ProviderData, 0, 1+len(state.activeDPs))
	// Grab data that is updated by background tasks
	btcDominance := state.lastBTCDominance
	peakPortfolioValue := state.peakPortfolioValue
	gainers := state.topGainers
	losers := state.topLosers
	cgTrending := state.cgTrendingData
	cmcTrending := state.cmcTrendingData
	state.stateMutex.RUnlock()

	// --- 3. Fetch all primary data from the broker (Kraken) ---
	// This data is critical, so we fetch it first and fail fast if it's unavailable.
	krakenTicker, tickerErr := state.broker.GetTicker(ctx, assetPair)
	if tickerErr != nil {
		// A live price is essential, so we return an error if it fails.
		return nil, fmt.Errorf("could not get ticker from broker for %s: %w", assetPair, tickerErr)
	}

	krakenOrderBook, obErr := state.broker.GetOrderBook(ctx, assetPair, 50) // Deeper book for better analysis
	if obErr != nil {
		state.logger.LogWarn("gatherConsolidatedData [%s]: could not get order book from broker: %v", assetPair, obErr)
	}

	// Fetch all required OHLCV timeframes from the broker.
	tfCfg := cfg.Consensus.MultiTimeframe
	allTFs := append([]string{tfCfg.BaseTimeframe}, tfCfg.AdditionalTimeframes...)
	krakenOHLCVByTF := make(map[string][]utilities.OHLCVBar)
	for idx, tfString := range allTFs {
		if idx >= len(tfCfg.TFLookbackLengths) {
			break
		}
		lookback := tfCfg.TFLookbackLengths[idx]
		krakenInterval, _ := utilities.ConvertTFToKrakenInterval(tfString)
		bars, err := state.broker.GetLastNOHLCVBars(ctx, assetPair, krakenInterval, lookback)
		if err != nil {
			state.logger.LogWarn("gatherConsolidatedData [%s]: broker failed to provide '%s' OHLCV: %v", assetPair, tfString, err)
			continue
		}
		krakenOHLCVByTF[tfString] = bars
	}

	// Add the complete broker data as the first provider.
	providersData = append(providersData, strategy.ProviderData{
		Name:         "kraken",
		Weight:       cfg.DataProviderWeights["kraken"],
		CurrentPrice: krakenTicker.LastPrice,
		OHLCVByTF:    krakenOHLCVByTF,
	})

	// --- 4. Concurrently fetch data from all other external providers ---
	var wg sync.WaitGroup
	var providerMutex sync.Mutex // To safely append to the providersData slice

	for _, dp := range state.activeDPs {
		wg.Add(1)
		go func(provider dataprovider.DataProvider) {
			defer wg.Done()
			providerName := state.providerNames[provider]
			var providerCoinID string

			switch providerName {
			case "coingecko":
				providerCoinID = identity.CoinGeckoID
			case "coinmarketcap":
				providerCoinID = identity.CoinMarketCapID
			default:
				return // Skip unknown providers
			}

			if providerCoinID == "N/A" || providerCoinID == "" {
				return
			}

			// Fetch Market Data (Current Price)
			extMarketData, mdErr := provider.GetMarketData(ctx, []string{providerCoinID}, cfg.Trading.QuoteCurrency)
			if mdErr != nil || len(extMarketData) == 0 {
				state.logger.LogWarn("gatherConsolidatedData [%s]: Provider %s failed to get market data for ID %s: %v", assetPair, providerName, providerCoinID, mdErr)
				return
			}

			// Fetch OHLCV data for all timeframes
			extOHLCVByTF := make(map[string][]utilities.OHLCVBar)
			for _, tfString := range allTFs {
				bars, ohlcvErr := provider.GetOHLCVHistorical(ctx, providerCoinID, cfg.Trading.QuoteCurrency, tfString)
				if ohlcvErr != nil {
					continue // Skip timeframe on error
				}
				extOHLCVByTF[tfString] = bars
			}

			providerMutex.Lock()
			providersData = append(providersData, strategy.ProviderData{
				Name:         providerName,
				Weight:       cfg.DataProviderWeights[providerName],
				CurrentPrice: extMarketData[0].CurrentPrice,
				OHLCVByTF:    extOHLCVByTF,
			})
			providerMutex.Unlock()
		}(dp)
	}
	wg.Wait() // Wait for all external providers to finish

	// --- 5. Perform Analyses & Assemble Final Picture ---
	// Fetch cross-exchange liquidity data from CoinGecko
	var aggregatedMetrics strategy.AggregatedLiquidityMetrics
	for _, dp := range state.activeDPs {
		if state.providerNames[dp] == "coingecko" {
			tickers, tickersErr := dp.GetAllTickersForAsset(ctx, identity.CoinGeckoID)
			if tickersErr != nil {
				state.logger.LogWarn("Could not fetch all tickers for %s to perform liquidity analysis: %v", assetPair, tickersErr)
			} else {
				aggregatedMetrics = strategy.CalculateAggregatedLiquidity(tickers, "Kraken") // Use Kraken as the reference exchange name
			}
			break
		}
	}

	// Calculate the market sentiment score from cached gainer/loser data
	marketSentiment := strategy.CalculateMarketSentiment(gainers, losers, state.logger)

	// Assemble the final data structure
	consolidatedData := &strategy.ConsolidatedMarketPicture{
		AssetPair:           assetPair,
		ProvidersData:       providersData,
		BTCDominance:        btcDominance,
		FearGreedIndex:      getCurrentFNG(),
		PortfolioValue:      portfolioValue,
		PeakPortfolioValue:  peakPortfolioValue,
		BrokerOrderBook:     krakenOrderBook,
		PrimaryOHLCVByTF:    krakenOHLCVByTF,       // Storing broker's data separately for direct access
		DailyOHLCV:          krakenOHLCVByTF["1d"], // Use broker's daily data for consistency
		AggregatedLiquidity: aggregatedMetrics,
		CgTrendingData:      cgTrending,
		CmcTrendingData:     cmcTrending,
		MarketSentiment:     marketSentiment, // Include the new sentiment score
	}

	// --- 6. Final Verification ---
	if _, ok := consolidatedData.PrimaryOHLCVByTF[tfCfg.BaseTimeframe]; !ok {
		return nil, errors.New("could not fetch base timeframe OHLCV data from broker; cannot proceed")
	}

	return consolidatedData, nil
}

// seekEntryOpportunity evaluates signals to open a new position.
// This is the corrected version of the function.
func seekEntryOpportunity(ctx context.Context, state *TradingState, assetPair string, signals []strategy.StrategySignal, consolidatedData *strategy.ConsolidatedMarketPicture, cfg *utilities.AppConfig) {
	for _, sig := range signals {
		if strings.EqualFold(sig.Direction, "buy") || strings.EqualFold(sig.Direction, "predictive_buy") {
			state.logger.LogInfo("SeekEntry [%s]: %s signal confirmed. Calculating order...", assetPair, strings.ToUpper(sig.Direction))
			// Get the primary bars from the consolidated data to check for bearish confluence.
			primaryBars, ok := consolidatedData.PrimaryOHLCVByTF[cfg.Consensus.MultiTimeframe.BaseTimeframe]
			if !ok {
				state.logger.LogError("SeekEntry [%s]: Missing primary bars for bearish check. Aborting buy.", assetPair)
				continue
			}

			// Perform the bearish confluence check.
			isBearish, reason := strategy.CheckBearishConfluence(primaryBars, *cfg)
			if isBearish {
				state.logger.LogWarn("SeekEntry [%s]: Buy signal ignored. Reason: Bearish confluence detected: %s", assetPair, reason)
				continue // Skip the rest of the loop and don't place a buy order.
			}

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

			// Check for a buy signal and then apply the SMA filter
			if strings.EqualFold(sig.Direction, "buy") {
				// Extract close prices for SMA calculation.
				// You'll need to define a SMA period in your config.json, let's use a 50-period SMA as an example.
				closePrices := strategy.ExtractCloses(primaryBars)
				sma50 := strategy.CalculateSMA(closePrices, 50)

				// Define a tolerance for the SMA check, e.g., 2% of the SMA value.
				smaTolerance := sma50 * 0.02

				if currentPrice <= (sma50+smaTolerance) && currentPrice >= (sma50-smaTolerance) {
					// If the price is near the SMA, set the order price to the SMA level.
					orderPrice = sma50
					orderSizeInBase = cfg.Trading.BaseOrderSize / orderPrice
					state.logger.LogInfo("SeekEntry [%s]: SMA confluence found. Placing order at SMA50: %.2f", assetPair, orderPrice)
				} else {
					state.logger.LogInfo("SeekEntry [%s]: Buy signal met, but price not near SMA50. Holding.", assetPair)
					continue
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
