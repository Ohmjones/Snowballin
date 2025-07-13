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

	sqliteCache, err := dataprovider.NewSQLiteCache(cfg.DB)
	if err != nil {
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

	// --- [MODIFICATION] Self-healing logic added to the startup sequence. ---
	logger.LogInfo("Reconciliation: Verifying consistency between database state and exchange balances...")
	tempStateForRecon := &TradingState{broker: krakenAdapter, logger: logger, config: cfg}

	for _, pair := range cfg.Trading.AssetPairs {
		baseAsset := strings.Split(pair, "/")[0]
		_, hasPositionInDB := loadedPositions[pair]

		balanceInfo, err := krakenAdapter.GetBalance(ctx, baseAsset)
		if err != nil {
			logger.LogDebug("Reconciliation: Could not get balance for %s, likely not held. Skipping.", baseAsset)
			continue
		}

		if balanceInfo.Total > 0.00001 && !hasPositionInDB {
			reconstructedPos, reconErr := ReconstructOrphanedPosition(ctx, tempStateForRecon, pair)
			if reconErr != nil {
				logger.LogFatal("ORPHANED POSITION DETECTED for %s, but reconstruction failed: %v. Manual intervention required.", pair, reconErr)
			}

			if err := sqliteCache.SavePosition(reconstructedPos); err != nil {
				logger.LogFatal("Failed to save reconstructed position for %s to database: %v. Halting.", pair, err)
			}
			loadedPositions[pair] = reconstructedPos
		}
	}
	logger.LogInfo("Reconciliation: State verification complete.")
	// --- End of modification ---

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

// --- [NEW] This function reconstructs an orphaned position by analyzing the exchange's trade history. ---
func ReconstructOrphanedPosition(ctx context.Context, state *TradingState, assetPair string) (*utilities.Position, error) {
	state.logger.LogWarn("Reconstruction: Attempting to reconstruct orphaned position for %s from trade history.", assetPair)

	ninetyDaysAgo := time.Now().Add(-90 * 24 * time.Hour)
	tradeHistory, err := state.broker.GetTrades(ctx, assetPair, ninetyDaysAgo)
	if err != nil {
		return nil, fmt.Errorf("failed to get trade history for %s: %w", assetPair, err)
	}

	if len(tradeHistory) == 0 {
		return nil, errors.New("no trade history found for this asset, cannot reconstruct")
	}

	var positionTrades []broker.Trade
	lastSellIndex := -1
	for i := len(tradeHistory) - 1; i >= 0; i-- {
		if tradeHistory[i].Side == "sell" {
			lastSellIndex = i
			break
		}
	}
	positionTrades = tradeHistory[lastSellIndex+1:]

	if len(positionTrades) == 0 {
		return nil, fmt.Errorf("found trade history for %s, but could not isolate a sequence of buys for the current position", assetPair)
	}

	var totalCost, totalVolume float64
	baseOrderTrade := positionTrades[0]

	for _, trade := range positionTrades {
		if trade.Side == "buy" {
			totalCost += trade.Cost
			totalVolume += trade.Volume
		}
	}

	if totalVolume == 0 {
		return nil, errors.New("isolated trades have zero total volume, cannot reconstruct")
	}

	filledSafetyOrders := len(positionTrades) - 1

	reconstructedPosition := &utilities.Position{
		AssetPair:          assetPair,
		EntryTimestamp:     baseOrderTrade.Timestamp,
		AveragePrice:       totalCost / totalVolume,
		TotalVolume:        totalVolume,
		BaseOrderPrice:     baseOrderTrade.Price,
		BaseOrderSize:      baseOrderTrade.Cost,
		FilledSafetyOrders: filledSafetyOrders,
		IsDcaActive:        true,
		BrokerOrderID:      baseOrderTrade.OrderID,
	}
	reconstructedPosition.CurrentTakeProfit = reconstructedPosition.AveragePrice * (1 + state.config.Trading.TakeProfitPercentage)

	state.logger.LogInfo("Reconstruction SUCCESS for %s. Avg Price: %.2f, Volume: %.4f, SOs Filled: %d",
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
		state.stateMutex.RLock()
		position, hasPosition := state.openPositions[assetPair]
		state.stateMutex.RUnlock()
		if hasPosition {
			manageOpenPosition(ctx, state, position)
		} else {
			seekEntryOpportunity(ctx, state, assetPair, currentPortfolioValue)
		}
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
	} else {
		if pos, ok := state.openPositions[assetPair]; ok {
			if math.Abs(pos.TotalVolume-order.ExecutedVol) < 1e-9 {
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

func manageOpenPosition(ctx context.Context, state *TradingState, pos *utilities.Position) {
	state.logger.LogInfo("ManagePosition [%s]: Managing position. AvgPrice: %.2f, Vol: %.8f, SOs: %d, TP: %.2f",
		pos.AssetPair, pos.AveragePrice, pos.TotalVolume, pos.FilledSafetyOrders, pos.CurrentTakeProfit)

	ticker, err := state.broker.GetTicker(ctx, pos.AssetPair)
	if err != nil {
		state.logger.LogError("ManagePosition [%s]: Could not get ticker: %v", pos.AssetPair, err)
		return
	}
	currentPrice := ticker.LastPrice

	portfolioValue, _ := state.broker.GetAccountValue(ctx, state.config.Trading.QuoteCurrency)
	consolidatedData, err := gatherConsolidatedData(ctx, state, pos.AssetPair, portfolioValue)
	if err != nil {
		state.logger.LogError("ManagePosition [%s]: Could not gather data for exit signal check: %v", pos.AssetPair, err)
	} else {
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
	}

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

func seekEntryOpportunity(ctx context.Context, state *TradingState, assetPair string, currentPortfolioValue float64) {
	consolidatedData, err := gatherConsolidatedData(ctx, state, assetPair, currentPortfolioValue)
	if err != nil {
		state.logger.LogError("SeekEntry [%s]: Failed to gather data for signal generation: %v", assetPair, err)
		return
	}

	stratInstance := strategy.NewStrategy(state.logger)
	signals, _ := stratInstance.GenerateSignals(ctx, *consolidatedData, *state.config)

	for _, sig := range signals {
		if sig.Direction == "buy" {
			state.logger.LogInfo("+++ [BUY] signal for %s. Reason: %s. Placing order.", assetPair, sig.Reason)
			orderPrice := sig.RecommendedPrice
			orderSizeInBase := sig.CalculatedSize
			if orderSizeInBase <= 0 {
				state.logger.LogWarn("SeekEntry [%s]: Calculated size is zero. Falling back to fixed base order size from config.", assetPair)
				orderSizeInBase = state.config.Trading.BaseOrderSize / orderPrice
			}
			orderID, placeErr := state.broker.PlaceOrder(ctx, assetPair, "buy", "limit", orderSizeInBase, orderPrice, 0, "")
			if placeErr != nil {
				state.logger.LogError("SeekEntry [%s]: Place order failed: %v", assetPair, placeErr)
			} else {
				state.logger.LogInfo("SeekEntry [%s]: Placed order ID %s at %.2f. Now tracking.", assetPair, orderID, orderPrice)
				state.stateMutex.Lock()
				state.pendingOrders[orderID] = assetPair
				if err := state.cache.SavePendingOrder(orderID, assetPair); err != nil {
					state.logger.LogError("SeekEntry [%s]: Failed to save pending order %s to DB: %v", assetPair, orderID, err)
				}
				state.stateMutex.Unlock()
			}
		}
	}
}

func gatherConsolidatedData(ctx context.Context, state *TradingState, assetPair string, currentPortfolioValue float64) (*strategy.ConsolidatedMarketPicture, error) {
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

	krakenTicker, tickerErr := state.broker.GetTicker(ctx, assetPair)
	if tickerErr != nil {
		state.logger.LogWarn("gatherConsolidatedData [%s]: could not get ticker: %v", assetPair, tickerErr)
	}

	krakenOrderBook, obErr := state.broker.GetOrderBook(ctx, assetPair, 20)
	if obErr != nil {
		state.logger.LogWarn("gatherConsolidatedData [%s]: could not get order book: %v", assetPair, obErr)
	}
	consolidatedData.BrokerOrderBook = krakenOrderBook

	tfCfg := state.config.Consensus.MultiTimeframe
	allTFs := append([]string{tfCfg.BaseTimeframe}, tfCfg.AdditionalTimeframes...)
	var krakenBaseBars []utilities.OHLCVBar
	baseBarsFound := false

	for idx, tfString := range allTFs {
		if idx >= len(tfCfg.TFLookbackLengths) {
			break
		}
		lookback := tfCfg.TFLookbackLengths[idx]
		krakenInterval, intervalErr := utilities.ConvertTFToKrakenInterval(tfString)
		if intervalErr != nil {
			state.logger.LogError("gatherConsolidatedData [%s]: bad interval %s: %v", assetPair, tfString, intervalErr)
			continue
		}

		bars, barsErr := state.broker.GetLastNOHLCVBars(ctx, assetPair, krakenInterval, lookback)
		if barsErr != nil {
			state.logger.LogError("gatherConsolidatedData [%s]: could not get OHLCV for %s: %v", assetPair, tfString, barsErr)
			continue
		}
		consolidatedData.PrimaryOHLCVByTF[tfString] = bars
		if tfString == tfCfg.BaseTimeframe {
			krakenBaseBars = bars
			if len(bars) > 0 {
				baseBarsFound = true
			}
		}
	}

	if !baseBarsFound {
		return nil, errors.New("could not fetch base timeframe OHLCV data from broker")
	}

	consolidatedData.ProvidersData = append(consolidatedData.ProvidersData, strategy.ProviderData{
		Name: "kraken", Weight: state.config.DataProviderWeights["kraken"],
		CurrentPrice: krakenTicker.LastPrice, OHLCVBars: krakenBaseBars,
	})

	baseAssetSymbol := strings.Split(assetPair, "/")[0]
	for _, dp := range state.activeDPs {
		providerName := state.providerNames[dp]
		providerCoinID, idErr := dp.GetCoinID(ctx, baseAssetSymbol)
		if idErr != nil {
			state.logger.LogError("gatherConsolidatedData [%s]: could not get %s ID: %v", assetPair, providerName, idErr)
			continue
		}
		extMarketData, mdErr := dp.GetMarketData(ctx, []string{providerCoinID}, state.config.Trading.QuoteCurrency)
		var extOHLCVBars []utilities.OHLCVBar
		if providerName == "coingecko" {
			var ohlcvErr error
			extOHLCVBars, ohlcvErr = dp.GetOHLCVHistorical(ctx, providerCoinID, state.config.Trading.QuoteCurrency, tfCfg.BaseTimeframe)
			if ohlcvErr != nil {
				state.logger.LogWarn("gatherConsolidatedData [%s]: %s GetOHLCVHistorical failed: %v", assetPair, providerName, ohlcvErr)
			}
		}
		var currentPrice float64
		if mdErr == nil && len(extMarketData) > 0 {
			currentPrice = extMarketData[0].CurrentPrice
		} else if mdErr != nil {
			state.logger.LogWarn("gatherConsolidatedData [%s]: %s GetMarketData failed: %v", assetPair, providerName, mdErr)
		}
		consolidatedData.ProvidersData = append(consolidatedData.ProvidersData, strategy.ProviderData{
			Name: providerName, Weight: state.config.DataProviderWeights[providerName],
			CurrentPrice: currentPrice, OHLCVBars: extOHLCVBars,
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
