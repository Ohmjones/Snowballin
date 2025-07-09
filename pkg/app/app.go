package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"Snowballin/dataprovider"
	cg "Snowballin/dataprovider/coingecko"
	cmc "Snowballin/dataprovider/coinmarketcap"
	"Snowballin/notification/discord"
	"Snowballin/pkg/broker"
	krakenBroker "Snowballin/pkg/broker/kraken"
	"Snowballin/strategy"
	"Snowballin/utilities"
)

type TradingState struct {
	broker             broker.Broker
	logger             *utilities.Logger
	config             *utilities.AppConfig
	discordClient      *discord.Client
	cache              *dataprovider.SQLiteCache
	activeDPs          []dataprovider.DataProvider
	providerNames      map[dataprovider.DataProvider]string
	peakPortfolioValue float64
	openPositions      map[string]*utilities.Position
	pendingOrders      map[string]string
	stateMutex         sync.RWMutex
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
	// --- 1. Pre-Flight Configuration Checks ---
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

	// --- 2. Initialize Services (Cache, HTTP Client) ---
	sqliteCache, err := dataprovider.NewSQLiteCache(cfg.DB)
	if err != nil {
		return fmt.Errorf("pre-flight check failed: sqlite cache init failed: %w", err)
	}
	defer sqliteCache.Close()
	go sqliteCache.StartScheduledCleanup(24*time.Hour, "CoinGecko")
	go sqliteCache.StartScheduledCleanup(24*time.Hour, "CoinMarketCap")
	go sqliteCache.StartScheduledCleanup(24*time.Hour, "kraken")

	sharedHTTPClient := &http.Client{Timeout: time.Duration(15 * time.Second)}

	// --- 3. Initialize and Verify Broker ---
	logger.LogInfo("Pre-Flight: Initializing and verifying broker (Kraken)...")
	krakenAdapter, krakenErr := krakenBroker.NewAdapter(&cfg.Kraken, sharedHTTPClient, logger, sqliteCache)
	if krakenErr != nil {
		return fmt.Errorf("pre-flight check failed: could not initialize Kraken adapter: %w", krakenErr)
	}
	initialPortfolioValue, portfolioErr := krakenAdapter.GetAccountValue(ctx, cfg.Trading.QuoteCurrency)
	if portfolioErr != nil {
		return fmt.Errorf("pre-flight check failed: could not get account value from broker. Check API keys and permissions: %w", portfolioErr)
	}
	if err := krakenAdapter.RefreshAssetInfo(ctx); err != nil {
		return fmt.Errorf("pre-flight check failed: could not refresh broker asset info: %w", err)
	}
	logger.LogInfo("Pre-Flight: Broker verification passed. Initial portfolio value: %.2f %s", initialPortfolioValue, cfg.Trading.QuoteCurrency)

	// --- 4. Initialize, Verify, and Prime Data Providers ---
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
		// PrimeCache now serves as our connectivity and data-readiness check.
		if err := dp.PrimeCache(ctx); err != nil {
			logger.LogError("Pre-Flight: Failed to prime cache for %s: %v. This provider will be disabled.", providerName, err)
			discordClient.SendMessage(fmt.Sprintf("⚠️ **Warning:** Data provider '%s' failed pre-flight check and will be disabled for this session.", providerName))
		} else {
			logger.LogInfo("Pre-Flight: %s provider primed successfully.", providerName)
			activeDPs = append(activeDPs, dp)
		}
	}

	// --- 5. Initialize Remaining Services and Final State ---
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

	state := &TradingState{
		broker:             krakenAdapter,
		logger:             logger,
		config:             cfg,
		discordClient:      discordClient,
		cache:              sqliteCache,
		activeDPs:          activeDPs,
		providerNames:      providerNames,
		peakPortfolioValue: initialPortfolioValue,
		openPositions:      loadedPositions,
		pendingOrders:      loadedPendingOrders,
	}

	// --- 6. Start Main Trading Loop ---
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

// In processTradingCycle, add the call to reapStaleOrders
func processTradingCycle(ctx context.Context, state *TradingState) {
	state.logger.LogInfo("-------------------- New Trading Cycle --------------------")
	currentPortfolioValue, valErr := state.broker.GetAccountValue(ctx, state.config.Trading.QuoteCurrency)
	if valErr != nil {
		state.logger.LogError("Cycle: Could not update portfolio value: %v", valErr)
		currentPortfolioValue = state.peakPortfolioValue
	}
	state.stateMutex.Lock()
	if currentPortfolioValue > state.peakPortfolioValue {
		state.peakPortfolioValue = currentPortfolioValue
	}
	state.stateMutex.Unlock()

	processPendingOrders(ctx, state)
	reapStaleOrders(ctx, state) // <-- ADD THIS CALL

	for _, assetPair := range state.config.Trading.AssetPairs {
		state.stateMutex.RLock()
		position, hasPosition := state.openPositions[assetPair]
		state.stateMutex.RUnlock()

		if hasPosition {
			manageOpenPosition(ctx, state, position, currentPortfolioValue)
		} else {
			seekEntryOpportunity(ctx, state, assetPair, currentPortfolioValue)
		}
	}
}

// Add this new function to the end of the file
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
		return // Feature disabled
	}

	for _, orderID := range orderIDsToCheck {
		order, err := state.broker.GetOrderStatus(ctx, orderID)
		if err != nil {
			continue // Error handled in processPendingOrders, skip here
		}

		// Check if the order is still open and has exceeded its max age
		if order.Status == "open" && time.Since(order.TimePlaced) > maxAge {
			state.logger.LogWarn("Reaper: Order %s for %s is stale (age: %s). Cancelling.", orderID, order.Pair, time.Since(order.TimePlaced))
			err := state.broker.CancelOrder(ctx, orderID)
			if err != nil {
				state.logger.LogError("Reaper: Failed to cancel stale order %s: %v", orderID, err)
			} else {
				state.logger.LogInfo("Reaper: Successfully cancelled stale order %s.", orderID)
				// The order will be removed from pendingOrders by processPendingOrders in the next cycle when its status shows "canceled".
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

	stratInstance := strategy.NewStrategy(state.logger)

	for _, orderID := range orderIDsToCheck {
		order, err := state.broker.GetOrderStatus(ctx, orderID)
		if err != nil {
			state.logger.LogError("PendingOrders: Could not get status for order %s: %v", orderID, err)
			continue
		}

		if strings.EqualFold(order.Status, "closed") { // "closed" is Kraken's status for filled/completed
			state.logger.LogInfo("PendingOrders: Order %s for %s filled!", orderID, order.Pair)
			state.stateMutex.Lock()
			assetPair := state.pendingOrders[orderID]

			if strings.EqualFold(order.Side, "buy") {
				// A buy order was filled, so we are opening a new position.
				krakenAdapter, _ := state.broker.(*krakenBroker.Adapter)
				krakenClient, _ := krakenAdapter.GetInternalClient()
				krakenSpecificPairName, _ := krakenClient.GetKrakenPairName(ctx, assetPair)
				baseTF, _ := utilities.ConvertTFToKrakenInterval(state.config.Consensus.MultiTimeframe.BaseTimeframe)
				lookback := state.config.Consensus.MultiTimeframe.TFLookbackLengths[0]
				bars, _ := state.broker.GetLastNOHLCVBars(ctx, krakenSpecificPairName, baseTF, lookback)

				avgFillPrice := order.Price
				sl, tp, _ := stratInstance.CalculateATRBasedSLTP(bars, avgFillPrice, *state.config)

				newPosition := &utilities.Position{
					AssetPair:         assetPair,
					EntryPrice:        avgFillPrice,
					Volume:            order.ExecutedVol,
					EntryTimestamp:    order.TimeCompleted,
					CurrentStopLoss:   sl,
					CurrentTakeProfit: tp,
					BrokerOrderID:     orderID,
				}
				state.openPositions[assetPair] = newPosition
				state.discordClient.NotifyOrderFilled(order, "Position Opened")

				// Persist the new position to the database
				if err := state.cache.SavePosition(newPosition); err != nil {
					state.logger.LogError("PendingOrders: Failed to save new position to DB for %s: %v", assetPair, err)
				}

			} else { // A "sell" order was filled, closing a position.
				state.discordClient.NotifyOrderFilled(order, "Position Closed")
			}

			// The order is complete, remove it from pending state in memory and DB.
			delete(state.pendingOrders, orderID)
			if err := state.cache.DeletePendingOrder(orderID); err != nil {
				state.logger.LogError("PendingOrders: Failed to delete pending order %s from DB: %v", orderID, err)
			}
			state.stateMutex.Unlock()

		} else if strings.EqualFold(order.Status, "canceled") || strings.EqualFold(order.Status, "expired") {
			state.logger.LogWarn("PendingOrders: Order %s for %s has failed (status: %s).", orderID, state.pendingOrders[orderID], order.Status)
			state.discordClient.SendMessage(fmt.Sprintf("⚠️ Order for %s failed! Status: %s, ID: `%s`", state.pendingOrders[orderID], order.Status, orderID))

			state.stateMutex.Lock()
			delete(state.pendingOrders, orderID)
			if err := state.cache.DeletePendingOrder(orderID); err != nil {
				state.logger.LogError("PendingOrders: Failed to delete failed order %s from DB: %v", orderID, err)
			}
			state.stateMutex.Unlock()
		}
	}
}

func manageOpenPosition(ctx context.Context, state *TradingState, pos *utilities.Position, currentPortfolioValue float64) {
	state.logger.LogInfo("ManagePosition [%s]: Managing position. Vol: %.8f, Entry: %.2f, SL: %.2f, TP: %.2f",
		pos.AssetPair, pos.Volume, pos.EntryPrice, pos.CurrentStopLoss, pos.CurrentTakeProfit)

	ticker, err := state.broker.GetTicker(ctx, pos.AssetPair)
	if err != nil {
		state.logger.LogError("ManagePosition [%s]: Could not get ticker: %v", pos.AssetPair, err)
		return
	}
	currentPrice := ticker.LastPrice

	// Check for SL/TP hit
	if currentPrice <= pos.CurrentStopLoss {
		state.logger.LogWarn("ManagePosition [%s]: STOP-LOSS HIT at %.2f. Placing market sell order.", pos.AssetPair, currentPrice)
		orderID, err := state.broker.PlaceOrder(ctx, pos.AssetPair, "sell", "market", pos.Volume, 0, 0, "")
		if err != nil {
			state.logger.LogError("ManagePosition [%s]: Failed to place market sell order for stop-loss: %v", pos.AssetPair, err)
		} else {
			state.stateMutex.Lock()
			delete(state.openPositions, pos.AssetPair)
			_ = state.cache.DeletePosition(pos.AssetPair) // Persist deletion
			state.pendingOrders[orderID] = pos.AssetPair
			_ = state.cache.SavePendingOrder(orderID, pos.AssetPair) // Persist new pending order
			state.stateMutex.Unlock()
		}
		return
	}
	if currentPrice >= pos.CurrentTakeProfit {
		state.logger.LogInfo("ManagePosition [%s]: TAKE-PROFIT HIT at %.2f. Placing market sell order.", pos.AssetPair, currentPrice)
		orderID, err := state.broker.PlaceOrder(ctx, pos.AssetPair, "sell", "market", pos.Volume, 0, 0, "")
		if err != nil {
			state.logger.LogError("ManagePosition [%s]: Failed to place market sell order for take-profit: %v", pos.AssetPair, err)
		} else {
			state.stateMutex.Lock()
			delete(state.openPositions, pos.AssetPair)
			_ = state.cache.DeletePosition(pos.AssetPair)
			state.pendingOrders[orderID] = pos.AssetPair
			_ = state.cache.SavePendingOrder(orderID, pos.AssetPair)
			state.stateMutex.Unlock()
		}
		return
	}

	// Apply Trailing Stop-Loss
	if state.config.Trading.TrailingStopEnabled && currentPrice > pos.EntryPrice {
		krakenAdapter, _ := state.broker.(*krakenBroker.Adapter)
		krakenClient, _ := krakenAdapter.GetInternalClient()
		krakenSpecificPairName, _ := krakenClient.GetKrakenPairName(ctx, pos.AssetPair)
		baseTF, _ := utilities.ConvertTFToKrakenInterval(state.config.Consensus.MultiTimeframe.BaseTimeframe)
		lookback := state.config.Consensus.MultiTimeframe.TFLookbackLengths[0]
		bars, err := state.broker.GetLastNOHLCVBars(ctx, krakenSpecificPairName, baseTF, lookback)
		if err == nil && len(bars) > 0 {
			atr, _ := strategy.CalculateATR(bars, state.config.Indicators.ATRPeriod)
			newTrailingStop := currentPrice - (atr * state.config.Trading.ATRStopLossMultiplier)
			if newTrailingStop > pos.CurrentStopLoss {
				state.logger.LogInfo("ManagePosition [%s]: Trailing stop loss. Updating from %.2f to %.2f.", pos.AssetPair, pos.CurrentStopLoss, newTrailingStop)
				state.stateMutex.Lock()
				pos.CurrentStopLoss = newTrailingStop
				state.openPositions[pos.AssetPair] = pos
				_ = state.cache.SavePosition(pos) // Persist the updated stop-loss
				state.stateMutex.Unlock()
			}
		}
	}

	// Check for strategy-based exit signal
	consolidatedData, err := gatherConsolidatedData(ctx, state, pos.AssetPair, currentPortfolioValue)
	if err != nil {
		state.logger.LogError("ManagePosition [%s]: Failed to gather data for exit signal check: %v", pos.AssetPair, err)
		return
	}
	stratInstance := strategy.NewStrategy(state.logger)
	signals, _ := stratInstance.GenerateSignals(ctx, *consolidatedData, *state.config)
	for _, sig := range signals {
		if sig.Direction == "sell" {
			state.logger.LogInfo("ManagePosition [%s]: SELL signal received from strategy. Placing limit sell order.", pos.AssetPair)
			orderID, placeErr := state.broker.PlaceOrder(ctx, pos.AssetPair, "sell", "limit", pos.Volume, sig.RecommendedPrice, 0, "")
			if placeErr != nil {
				state.logger.LogError("ManagePosition [%s]: Failed to place limit sell order for exit signal: %v", pos.AssetPair, placeErr)
			} else {
				state.logger.LogInfo("ManagePosition [%s]: Placed limit sell order %s to exit position.", pos.AssetPair, orderID)
				state.stateMutex.Lock()
				delete(state.openPositions, pos.AssetPair)
				_ = state.cache.DeletePosition(pos.AssetPair)
				state.pendingOrders[orderID] = pos.AssetPair
				_ = state.cache.SavePendingOrder(orderID, pos.AssetPair)
				state.stateMutex.Unlock()
			}
			return
		}
	}
}

func seekEntryOpportunity(ctx context.Context, state *TradingState, assetPair string, currentPortfolioValue float64) {
	consolidatedData, err := gatherConsolidatedData(ctx, state, assetPair, currentPortfolioValue)
	if err != nil {
		state.logger.LogError("SeekEntry [%s]: Failed to gather data: %v", assetPair, err)
		return
	}

	stratInstance := strategy.NewStrategy(state.logger)
	signals, _ := stratInstance.GenerateSignals(ctx, *consolidatedData, *state.config)
	for _, sig := range signals {
		if sig.Direction == "buy" {
			state.logger.LogInfo("SeekEntry [%s]: BUY signal received. Confidence: %.2f. Placing order.", assetPair, sig.Confidence)
			orderID, placeErr := state.broker.PlaceOrder(ctx, assetPair, sig.Direction, "limit", sig.CalculatedSize, sig.RecommendedPrice, 0, "")
			if placeErr != nil {
				state.logger.LogError("SeekEntry [%s]: Place BUY order failed: %v", assetPair, placeErr)
			} else {
				state.logger.LogInfo("SeekEntry [%s]: Placed BUY order ID %s. Now tracking.", assetPair, orderID)
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
	krakenAdapter, ok := state.broker.(*krakenBroker.Adapter)
	if !ok {
		return nil, errors.New("broker is not a kraken adapter")
	}
	krakenClient, _ := krakenAdapter.GetInternalClient()
	krakenSpecificPairName, err := krakenClient.GetKrakenPairName(ctx, assetPair)
	if err != nil {
		return nil, fmt.Errorf("failed to get kraken pair name for %s: %w", assetPair, err)
	}

	state.stateMutex.RLock()
	consolidatedData := &strategy.ConsolidatedMarketPicture{
		AssetPair:          assetPair,
		ProvidersData:      make([]strategy.ProviderData, 0, 1+len(state.activeDPs)),
		FearGreedIndex:     getCurrentFNG(),
		PortfolioValue:     currentPortfolioValue,
		PeakPortfolioValue: state.peakPortfolioValue,
	}
	state.stateMutex.RUnlock()

	krakenTicker, tickerErr := state.broker.GetTicker(ctx, krakenSpecificPairName)
	if tickerErr != nil {
		state.logger.LogWarn("gatherConsolidatedData [%s]: could not get ticker: %v", assetPair, tickerErr)
		// Continue with a zero-value ticker, price will be 0. Logic must handle this.
	}

	krakenOrderBook, obErr := state.broker.GetOrderBook(ctx, krakenSpecificPairName, 20)
	if obErr != nil {
		state.logger.LogWarn("gatherConsolidatedData [%s]: could not get order book: %v", assetPair, obErr)
	}
	consolidatedData.BrokerOrderBook = krakenOrderBook

	tfCfg := state.config.Consensus.MultiTimeframe
	allTFs := append([]string{tfCfg.BaseTimeframe}, tfCfg.AdditionalTimeframes...)
	krakenBaseBars, krakenOHLCVByTF := []utilities.OHLCVBar{}, make(map[string][]utilities.OHLCVBar)
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
		bars, barsErr := state.broker.GetLastNOHLCVBars(ctx, krakenSpecificPairName, krakenInterval, lookback)
		if barsErr != nil {
			state.logger.LogError("gatherConsolidatedData [%s]: could not get OHLCV for %s: %v", assetPair, tfString, barsErr)
			continue // Skip this timeframe if it fails
		}
		krakenOHLCVByTF[tfString] = bars
		if tfString == tfCfg.BaseTimeframe {
			krakenBaseBars = bars
			if len(bars) > 0 {
				baseBarsFound = true
			}
		}
	}
	consolidatedData.PrimaryOHLCVByTF = krakenOHLCVByTF
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
		extOHLCVBars, ohlcvErr := dp.GetOHLCVHistorical(ctx, providerCoinID, state.config.Trading.QuoteCurrency, tfCfg.BaseTimeframe)

		var currentPrice float64
		if mdErr == nil && len(extMarketData) > 0 {
			currentPrice = extMarketData[0].CurrentPrice
		} else if mdErr != nil {
			state.logger.LogWarn("gatherConsolidatedData [%s]: %s GetMarketData failed: %v", assetPair, providerName, mdErr)
		}

		if ohlcvErr != nil {
			state.logger.LogWarn("gatherConsolidatedData [%s]: %s GetOHLCVHistorical failed: %v", assetPair, providerName, ohlcvErr)
		}

		consolidatedData.ProvidersData = append(consolidatedData.ProvidersData, strategy.ProviderData{
			Name: providerName, Weight: state.config.DataProviderWeights[providerName],
			CurrentPrice: currentPrice, OHLCVBars: extOHLCVBars,
		})
	}

	return consolidatedData, nil
}
