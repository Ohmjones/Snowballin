package app

import (
	"Snowballin/dataprovider"
	cg "Snowballin/dataprovider/coingecko"
	cmc "Snowballin/dataprovider/coinmarketcap"
	"Snowballin/notification/discord"
	"Snowballin/pkg/broker"
	krakenBroker "Snowballin/pkg/broker/kraken"
	"Snowballin/pkg/optimizer"
	"Snowballin/strategy"
	"Snowballin/utilities"
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
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
	isCircuitBreakerTripped bool
}

var (
	fngMutex   sync.RWMutex
	currentFNG dataprovider.FearGreedIndex
)

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

	if len(activeDPs) > 0 {
		logger.LogInfo("AppRun: Initializing and starting the parameter optimizer with '%s'.", providerNames[activeDPs[0]])
		optimizer := optimizer.NewOptimizer(logger, sqliteCache, cfg, activeDPs[0])
		go optimizer.StartScheduledOptimization(ctx)
	} else {
		logger.LogWarn("AppRun: No external data providers are active. The parameter optimizer will be disabled.")
	}

	loadedPositions, err := sqliteCache.LoadPositions()
	if err != nil {
		return fmt.Errorf("failed to load open positions from db: %w", err)
	}
	loadedPendingOrders, err := sqliteCache.LoadPendingOrders()
	if err != nil {
		return fmt.Errorf("failed to load pending orders from db: %w", err)
	}
	logger.LogInfo("AppRun: Loaded %d open position(s) and %d pending order(s) from database.", len(loadedPositions), len(loadedPendingOrders))

	// --- [THE FIX] This entire reconciliation block is rewritten for robustness. ---
	logger.LogInfo("Reconciliation: Verifying consistency between database state and exchange balances...")
	tempStateForRecon := &TradingState{broker: krakenAdapter, logger: logger, config: cfg}

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

	// 2. Iterate over the REAL balances from the exchange.
	for _, balance := range allBalances {
		if balance.Total < 1e-8 { // Skip zero or dust balances
			continue
		}

		// 3. Construct the pair name (e.g., "ETH/USD") and check if we are configured to trade it.
		assetPair := fmt.Sprintf("%s/%s", balance.Currency, cfg.Trading.QuoteCurrency)
		if !configuredPairs[assetPair] {
			logger.LogDebug("Reconciliation: Skipping balance for %s as it is not a configured trading pair.", balance.Currency)
			continue
		}

		// 4. If we have a balance for a configured pair but no position in our DB, reconstruct it.
		_, hasPositionInDB := loadedPositions[assetPair]
		if !hasPositionInDB {
			reconstructedPos, reconErr := ReconstructOrphanedPosition(ctx, tempStateForRecon, assetPair, balance.Total)
			if reconErr != nil {
				logger.LogFatal("ORPHANED POSITION DETECTED for %s, but reconstruction failed: %v. Manual intervention required.", assetPair, reconErr)
			}

			if err := sqliteCache.SavePosition(reconstructedPos); err != nil {
				logger.LogFatal("Failed to save reconstructed position for %s to database: %v. Halting.", assetPair, err)
			}
			loadedPositions[assetPair] = reconstructedPos
		}
	}
	logger.LogInfo("Reconciliation: State verification complete.")
	// --- End of Fix ---

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
	}

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
func ReconstructOrphanedPosition(ctx context.Context, state *TradingState, assetPair string, actualBalance float64) (*utilities.Position, error) {
	state.logger.LogWarn("Reconstruction: Attempting to reconstruct orphaned position for %s. Target balance: %f", assetPair, actualBalance)

	ninetyDaysAgo := time.Now().Add(-90 * 24 * time.Hour)
	tradeHistory, err := state.broker.GetTrades(ctx, assetPair, ninetyDaysAgo)
	if err != nil {
		return nil, fmt.Errorf("failed to get trade history for %s: %w", assetPair, err)
	}

	if len(tradeHistory) == 0 {
		return nil, fmt.Errorf("no trade history found for this asset in the last 90 days, cannot reconstruct")
	}

	sort.Slice(tradeHistory, func(i, j int) bool {
		return tradeHistory[i].Timestamp.After(tradeHistory[j].Timestamp)
	})

	var positionTrades []broker.Trade
	accumulatedVolume := 0.0
	const tolerance = 1e-8

	for _, trade := range tradeHistory {
		if accumulatedVolume >= actualBalance-tolerance {
			break
		}

		if trade.Side == "buy" {
			positionTrades = append([]broker.Trade{trade}, positionTrades...)
			accumulatedVolume += trade.Volume
		} else if trade.Side == "sell" {
			break
		}
	}

	if len(positionTrades) == 0 {
		return nil, fmt.Errorf("found trade history for %s, but could not isolate a sequence of buys for the current balance of %f", assetPair, actualBalance)
	}

	var totalCost float64
	baseOrderTrade := positionTrades[0]

	for _, trade := range positionTrades {
		totalCost += trade.Cost
	}

	finalVolume := actualBalance
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
	reconstructedPosition.CurrentTakeProfit = reconstructedPosition.AveragePrice * (1 + state.config.Trading.TakeProfitPercentage)

	state.logger.LogInfo("Reconstruction SUCCESS for %s. Avg Price: %.2f, Volume: %.8f (matches exchange), SOs Filled: %d",
		assetPair, reconstructedPosition.AveragePrice, reconstructedPosition.TotalVolume, reconstructedPosition.FilledSafetyOrders)

	return reconstructedPosition, nil
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

	currentPortfolioValue, valErr := state.broker.GetAccountValue(ctx, state.config.Trading.QuoteCurrency)
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

	for _, assetPair := range state.config.Trading.AssetPairs {
		consolidatedData, err := gatherConsolidatedData(ctx, state, assetPair, currentPortfolioValue)
		if err != nil {
			state.logger.LogError("Cycle [%s]: Failed to gather consolidated data: %v", assetPair, err)
			continue
		}

		stratInstance := strategy.NewStrategy(state.logger)
		signals, _ := stratInstance.GenerateSignals(ctx, *consolidatedData, *state.config)

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

		// --- DUST LOGIC FIX ---
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
				dustThreshold := 1.0 // Default to $1 if not set in config
				if state.config.Trading.DustThresholdUSD > 0 {
					dustThreshold = state.config.Trading.DustThresholdUSD
				}
				if positionValueUSD < dustThreshold {
					isEffectivelyDust = true
					state.logger.LogInfo("Position for %s is considered dust (value: $%.4f). Allowing new entry check.", assetPair, positionValueUSD)
				}
			}
		}

		// If a position exists AND it's not dust, manage it.
		// Otherwise, seek a new entry.
		if hasPosition && !isEffectivelyDust {
			manageOpenPosition(ctx, state, position, signals, consolidatedData)
		} else {
			seekEntryOpportunity(ctx, state, assetPair, signals, consolidatedData)
		}
		// --- END OF DUST LOGIC FIX ---
	}
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
			newPosition.CurrentTakeProfit = newPosition.AveragePrice * (1 + state.config.Trading.TakeProfitPercentage)
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
			position.CurrentTakeProfit = position.AveragePrice * (1 + state.config.Trading.TakeProfitPercentage)
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
				profit := (order.Price - pos.AveragePrice) * pos.TotalVolume
				profitPercent := ((order.Price - pos.AveragePrice) / pos.AveragePrice) * 100
				state.discordClient.NotifyOrderFilled(order, fmt.Sprintf("💰 Position Closed\nProfit: %.2f %s (%.2f%%)", profit, state.config.Trading.QuoteCurrency, profitPercent))
				delete(state.openPositions, assetPair)
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
func manageOpenPosition(ctx context.Context, state *TradingState, pos *utilities.Position, signals []strategy.StrategySignal, consolidatedData *strategy.ConsolidatedMarketPicture) {
	state.logger.LogInfo("ManagePosition [%s]: Managing position. AvgPrice: %.2f, Vol: %.8f, SOs: %d, TP: %.2f",
		pos.AssetPair, pos.AveragePrice, pos.TotalVolume, pos.FilledSafetyOrders, pos.CurrentTakeProfit)

	// --- NEW LOGIC: Prioritize adding to the position on a new BUY signal ---
	for _, sig := range signals {
		if sig.Direction == "buy" || sig.Direction == "predictive_buy" {
			state.logger.LogInfo("ManagePosition [%s]: New '%s' signal received. Adding to existing position.", pos.AssetPair, strings.ToUpper(sig.Direction))

			orderPrice := sig.RecommendedPrice
			orderSizeInBase := sig.CalculatedSize

			if orderSizeInBase <= 0 {
				state.logger.LogWarn("ManagePosition [%s]: Calculated size for add-on order is zero. Falling back to base order size.", pos.AssetPair)
				orderSizeInBase = state.config.Trading.BaseOrderSize / orderPrice
			}
			if orderPrice <= 0 {
				state.logger.LogError("ManagePosition [%s]: Invalid add-on order price (<= 0). Aborting.", pos.AssetPair)
				return // Abort this action
			}

			orderID, placeErr := state.broker.PlaceOrder(ctx, pos.AssetPair, "buy", "limit", orderSizeInBase, orderPrice, 0, "")
			if placeErr != nil {
				state.logger.LogError("ManagePosition [%s]: Failed to place add-on buy order: %v", pos.AssetPair, placeErr)
			} else {
				state.logger.LogInfo("ManagePosition [%s]: Placed add-on order ID %s at %.2f.", pos.AssetPair, orderID, orderPrice)

				// Send a specific Discord message for this action
				baseAsset := strings.Split(pos.AssetPair, "/")[0]
				quoteAsset := strings.Split(pos.AssetPair, "/")[1]
				message := fmt.Sprintf("➕ **Adding to Position**\n**Pair:** %s\n**Size:** `%.4f %s`\n**Price:** `%.2f %s`\n**Reason:** %s",
					pos.AssetPair, orderSizeInBase, baseAsset, orderPrice, quoteAsset, sig.Reason)
				state.discordClient.SendMessage(message)

				state.stateMutex.Lock()
				state.pendingOrders[orderID] = pos.AssetPair
				_ = state.cache.SavePendingOrder(orderID, pos.AssetPair)
				state.stateMutex.Unlock()
			}
			// Exit after placing the add-on order. We don't want to also place a safety order in the same cycle.
			return
		}
	}
	// --- END OF NEW LOGIC ---

	// If no new buy signals, proceed with standard management (TP, SL, DCA)
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

	// [ ... The rest of the take-profit, trailing stop, and safety order logic remains exactly the same ... ]
	// Check for a specific "emergency" exit signal.
	stratInstance := strategy.NewStrategy(state.logger)
	exitSignal, shouldExit := stratInstance.GenerateExitSignal(ctx, *consolidatedData, *state.config)
	if shouldExit && exitSignal.Direction == "sell" {
		state.logger.LogWarn("!!! [SELL] signal for %s (Strategy Exit). Reason: %s. Placing market sell order.", pos.AssetPair, exitSignal.Reason)
		orderID, err := state.broker.PlaceOrder(ctx, pos.AssetPair, "sell", "market", pos.TotalVolume, 0, 0, "")
		if err != nil {
			state.logger.LogError("ManagePosition [%s]: Failed to place market sell order for strategy exit: %v", pos.AssetPair, err)
		} else {
			state.stateMutex.Lock()
			state.pendingOrders[orderID] = pos.AssetPair
			_ = state.cache.SavePendingOrder(orderID, pos.AssetPair)
			state.stateMutex.Unlock()
		}
		return
	}

	// Standard Take-Profit and Trailing Stop logic.
	if state.config.Trading.TrailingStopEnabled && pos.IsTrailingActive {
		if currentPrice > pos.PeakPriceSinceTP {
			pos.PeakPriceSinceTP = currentPrice
			state.logger.LogInfo("ManagePosition [%s]: Trailing stop active. New peak price: %.2f", pos.AssetPair, pos.PeakPriceSinceTP)
			if err := state.cache.SavePosition(pos); err != nil {
				state.logger.LogError("ManagePosition [%s]: Failed to save updated peak price to DB: %v", pos.AssetPair, err)
			}
		}
		trailingStopPrice := pos.PeakPriceSinceTP * (1.0 - (state.config.Trading.TrailingStopDeviation / 100.0))
		if currentPrice <= trailingStopPrice {
			state.logger.LogInfo("ManagePosition [%s]: TRAILING STOP-LOSS HIT at %.2f (Peak was %.2f). Placing market sell order.", pos.AssetPair, currentPrice, pos.PeakPriceSinceTP)
			orderID, err := state.broker.PlaceOrder(ctx, pos.AssetPair, "sell", "market", pos.TotalVolume, 0, 0, "")
			if err != nil {
				state.logger.LogError("ManagePosition [%s]: Failed to place market sell order for trailing stop: %v", pos.AssetPair, err)
			} else {
				state.stateMutex.Lock()
				state.pendingOrders[orderID] = pos.AssetPair
				_ = state.cache.SavePendingOrder(orderID, pos.AssetPair)
				state.stateMutex.Unlock()
			}
			return
		}
	} else if currentPrice >= pos.CurrentTakeProfit && !pos.IsTrailingActive {
		if state.config.Trading.TrailingStopEnabled {
			partialSellPercent := state.config.Trading.TakeProfitPartialSellPercent / 100.0
			if partialSellPercent > 0 && partialSellPercent < 1.0 {
				volumeToSell := pos.TotalVolume * partialSellPercent
				state.logger.LogInfo("ManagePosition [%s]: HYBRID TAKE-PROFIT HIT at %.2f. Selling %.2f%% of position.", pos.AssetPair, currentPrice, state.config.Trading.TakeProfitPartialSellPercent)
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
				state.logger.LogError("ManagePosition [%s]: Failed to place market sell order for take-profit: %v", pos.AssetPair, err)
			} else {
				state.stateMutex.Lock()
				state.pendingOrders[orderID] = pos.AssetPair
				_ = state.cache.SavePendingOrder(orderID, pos.AssetPair)
				state.stateMutex.Unlock()
			}
			return
		}
	}

	// Safety Order (DCA) logic.
	if pos.IsDcaActive && pos.FilledSafetyOrders < state.config.Trading.MaxSafetyOrders {
		var shouldPlaceSafetyOrder bool
		var nextSafetyOrderPrice float64
		if state.config.Trading.DcaSpacingMode == "atr" {
			state.logger.LogDebug("ManagePosition [%s]: Using ATR spacing mode.", pos.AssetPair)
			tf := state.config.Consensus.MultiTimeframe.BaseTimeframe
			krakenInterval, _ := utilities.ConvertTFToKrakenInterval(tf)
			bars, err := state.broker.GetLastNOHLCVBars(ctx, pos.AssetPair, krakenInterval, state.config.Trading.DcaAtrPeriod+1)
			if err != nil || len(bars) < state.config.Trading.DcaAtrPeriod+1 {
				state.logger.LogError("ManagePosition [%s]: Could not fetch enough bars for ATR calculation: %v", pos.AssetPair, err)
			} else {
				atrValue, err := strategy.CalculateATR(bars, state.config.Trading.DcaAtrPeriod)
				if err != nil {
					state.logger.LogError("ManagePosition [%s]: Error calculating ATR: %v", pos.AssetPair, err)
				} else {
					priceDeviation := atrValue * state.config.Trading.DcaAtrSpacingMultiplier * math.Pow(state.config.Trading.SafetyOrderStepScale, float64(pos.FilledSafetyOrders))
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
			currentStep := state.config.Trading.PriceDeviationToOpenSafetyOrders
			for i := 0; i < nextSONumber; i++ {
				totalDeviationPercentage += currentStep
				currentStep *= state.config.Trading.SafetyOrderStepScale
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
			orderSizeInQuote := pos.BaseOrderSize * math.Pow(state.config.Trading.SafetyOrderVolumeScale, float64(nextSONumber))
			orderSizeInBase := orderSizeInQuote / strategicSafetyPrice
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
func seekEntryOpportunity(ctx context.Context, state *TradingState, assetPair string, signals []strategy.StrategySignal, consolidatedData *strategy.ConsolidatedMarketPicture) {
	for _, sig := range signals {
		// Check for both standard "buy" and the new "predictive_buy" signals.
		if sig.Direction == "buy" || sig.Direction == "predictive_buy" {
			state.logger.LogInfo("SeekEntry [%s]: %s signal confirmed. Calculating order...", assetPair, strings.ToUpper(sig.Direction))

			var orderPrice float64
			var orderSizeInBase float64

			// Get the current price from the broker data for calculations.
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

			// --- UNIFIED MARTINGALE LOGIC ---
			if sig.Direction == "predictive_buy" && state.config.Trading.UseMartingaleForPredictive {
				// For predictive buys, place the order at a discount to the current price.
				deviation := state.config.Trading.PredictiveBuyDeviationPercent / 100.0
				orderPrice = currentPrice * (1 - deviation)
				// Use the standard Martingale base order size.
				orderSizeInBase = state.config.Trading.BaseOrderSize / orderPrice
				state.logger.LogInfo("SeekEntry [%s]: Predictive buy will be placed at %.2f (%.2f%% below current price).", assetPair, orderPrice, state.config.Trading.PredictiveBuyDeviationPercent)
			} else {
				// For standard "buy" signals, use the recommended price from the strategy.
				orderPrice = sig.RecommendedPrice
				orderSizeInBase = state.config.Trading.BaseOrderSize / orderPrice
			}
			// --- END OF UNIFIED LOGIC ---

			if orderSizeInBase <= 0 || orderPrice <= 0 {
				state.logger.LogError("SeekEntry [%s]: Invalid order parameters (size=%.4f, price=%.2f). Aborting.", assetPair, orderSizeInBase, orderPrice)
				continue
			}

			orderID, placeErr := state.broker.PlaceOrder(ctx, assetPair, "buy", "limit", orderSizeInBase, orderPrice, 0, "")
			if placeErr != nil {
				state.logger.LogError("SeekEntry [%s]: Place order failed: %v", assetPair, placeErr)
			} else {
				state.logger.LogInfo("SeekEntry [%s]: Placed order ID %s at %.2f. Now tracking.", assetPair, orderID, orderPrice)
				// ... (Discord notification logic remains the same) ...
				state.stateMutex.Lock()
				state.pendingOrders[orderID] = assetPair
				_ = state.cache.SavePendingOrder(orderID, assetPair)
				state.stateMutex.Unlock()
			}
			break
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
