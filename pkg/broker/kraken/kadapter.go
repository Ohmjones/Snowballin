package kraken

import (
	"Snowballin/dataprovider"
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"Snowballin/pkg/broker"
	"Snowballin/utilities"
)

// Adapter provides a broker-agnostic interface for interacting with Kraken via the Client.
// It handles common-to-Kraken translations, caching, and logging.
// This struct implements the broker.Broker interface to ensure full compatibility with the DCA bot's requirements.
type Adapter struct {
	appConfig  *utilities.AppConfig      // Configuration for Kraken-specific settings
	cache      *dataprovider.SQLiteCache // Cache for OHLCV, tickers, and fees
	client     *Client                   // Underlying Kraken API client
	logger     *utilities.Logger         // Logger for adapter operations
	assetPairs []string                  // List of common asset pairs from config (e.g., ["BTC/USD"])
}

// NewAdapter creates a new Kraken Adapter instance.
// It initializes with provided config, the list of configured asset pairs, the client, logger, and cache.
// The assetPairs are required to correctly initialize the client's asset mapping.
func NewAdapter(appCfg *utilities.AppConfig, assetPairs []string, client *Client, logger *utilities.Logger, cache *dataprovider.SQLiteCache) (*Adapter, error) {
	if appCfg == nil {
		return nil, errors.New("kraken adapter: AppConfig cannot be nil")
	}
	if client == nil {
		return nil, errors.New("kraken adapter: client cannot be nil")
	}
	if logger == nil {
		logger = utilities.NewLogger(utilities.Info)
		logger.LogWarn("Kraken.Adapter: Logger fallback used for adapter.")
	}
	if cache == nil {
		return nil, errors.New("kraken adapter: SQLiteCache cannot be nil")
	}
	if len(assetPairs) == 0 {
		return nil, errors.New("kraken adapter: assetPairs cannot be empty for initialization")
	}

	logger.LogInfo("Initializing Kraken Adapter...")

	adapter := &Adapter{
		appConfig:  appCfg,
		cache:      cache,
		client:     client,
		logger:     logger,
		assetPairs: assetPairs,
	}

	logger.LogInfo("Kraken Adapter initialized successfully.")
	return adapter, nil
}

// CancelOrder cancels an existing order on Kraken by its ID.
// It implements the broker.Broker interface for order management.
func (a *Adapter) CancelOrder(ctx context.Context, orderID string) error {
	if orderID == "" {
		return errors.New("CancelOrder: orderID cannot be empty")
	}

	if err := a.client.CancelOrderAPI(ctx, orderID); err != nil {
		return fmt.Errorf("CancelOrder: API call failed: %w", err)
	}

	a.logger.LogInfo("CancelOrder: Successfully requested cancellation for order %s", orderID)
	return nil
}

// GetAllBalances retrieves all non-zero account balances from Kraken.
// Balances are converted to common asset names (e.g., XBT -> BTC).
// This method is essential for portfolio-level tracking and drawdown calculations.
func (a *Adapter) GetAllBalances(ctx context.Context) ([]broker.Balance, error) {
	balances, err := a.client.GetBalancesAPI(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetAllBalances: failed to get balances from API: %w", err)
	}

	var allBalances []broker.Balance
	for krakenName, balanceStr := range balances {
		commonName, err := a.client.GetCommonAssetName(ctx, krakenName)
		if err != nil {
			a.logger.LogDebug("GetAllBalances: could not get common name for Kraken asset '%s', skipping. Error: %v", krakenName, err)
			continue
		}

		bal, parseErr := strconv.ParseFloat(balanceStr, 64)
		if parseErr != nil {
			a.logger.LogWarn("GetAllBalances: could not parse balance string '%s' for %s, skipping: %v", balanceStr, commonName, parseErr)
			continue
		}

		if bal > 0 {
			allBalances = append(allBalances, broker.Balance{
				Currency:  commonName,
				Total:     bal,
				Available: bal,
			})
		}
	}
	return allBalances, nil
}

// GetBalance retrieves the balance for a specific currency from Kraken.
// Returns zero balance if the currency is not found or has zero value.
func (a *Adapter) GetBalance(ctx context.Context, currency string) (broker.Balance, error) {
	balances, err := a.client.GetBalancesAPI(ctx)
	if err != nil {
		return broker.Balance{}, fmt.Errorf("GetBalance: failed to get balances from API: %w", err)
	}

	upperCurrency := strings.ToUpper(currency)

	for krakenName, balanceStr := range balances {
		commonName, err := a.client.GetCommonAssetName(ctx, krakenName)
		if err != nil {
			a.logger.LogDebug("GetBalance: could not get common name for Kraken asset '%s', skipping. Error: %v", krakenName, err)
			continue
		}

		if strings.EqualFold(commonName, upperCurrency) {
			bal, parseErr := strconv.ParseFloat(balanceStr, 64)
			if parseErr != nil {
				return broker.Balance{}, fmt.Errorf("GetBalance: could not parse balance string '%s' for %s: %w", balanceStr, currency, parseErr)
			}
			return broker.Balance{
				Currency:  currency,
				Total:     bal,
				Available: bal,
			}, nil
		}
	}

	a.logger.LogDebug("GetBalance: No balance found for currency '%s' after checking all returned assets.", currency)
	return broker.Balance{Currency: currency, Total: 0, Available: 0}, nil
}

// GetLastNOHLCVBars retrieves the last N OHLCV bars for a trading pair from cache or API.
// Critical for technical indicators and volatility assessments.
// Parameters:
//   - ctx: Context for handling cancellation and timeouts.
//   - pair: The trading pair (e.g., "BTC/USD").
//   - intervalMinutes: The timeframe (e.g., "1h", "4h", "1d").
//   - nBars: The number of bars to retrieve.
//
// Returns:
//   - A slice of OHLCVBar structs containing the requested data.
//   - An error if the operation fails.
func (a *Adapter) GetLastNOHLCVBars(ctx context.Context, pair string, intervalMinutes string, nBars int) ([]utilities.OHLCVBar, error) {
	const cacheProvider = "kraken"
	cacheKey := fmt.Sprintf("%s-%s", strings.ReplaceAll(pair, "/", "-"), intervalMinutes)

	// Convert the interval to Kraken's format (e.g., "1h" to "60").
	interval, err := utilities.ConvertTFToKrakenInterval(intervalMinutes)
	if err != nil {
		return nil, fmt.Errorf("invalid interval format: %w", err)
	}

	// Parse the interval as a duration for cache lookup (e.g., "60" -> 60 minutes).
	intervalDuration, err := time.ParseDuration(interval + "m")
	if err != nil {
		return nil, fmt.Errorf("could not parse interval to duration: %w", err)
	}

	// Calculate the lookback period to ensure we fetch enough bars.
	lookbackDuration := time.Duration(nBars+5) * intervalDuration
	startTime := time.Now().Add(-lookbackDuration)

	// Check the cache for existing bars.
	cachedBars, err := a.cache.GetBars(cacheProvider, cacheKey, startTime.UnixMilli(), time.Now().UnixMilli())
	if err == nil && len(cachedBars) >= nBars {
		// Verify the cache is fresh (within 2x the interval duration).
		if time.Since(time.UnixMilli(cachedBars[len(cachedBars)-1].Timestamp)) < (2 * intervalDuration) {
			a.logger.LogInfo("kadapter GetLastNOHLCVBars [%s]: Cache hit. Returning %d bars.", pair, nBars)
			if len(cachedBars) > nBars {
				return cachedBars[len(cachedBars)-nBars:], nil
			}
			return cachedBars, nil
		}
	}

	// Cache miss or stale data; fetch from the Kraken API.
	a.logger.LogInfo("kadapter GetLastNOHLCVBars [%s]: Cache miss or insufficient data. Fetching from API.", pair)

	// Get the Kraken-specific pair name (e.g., "BTC/USD" -> "XXBTZUSD").
	krakenPair, err := a.client.GetPrimaryKrakenPairName(ctx, pair)
	if err != nil {
		return nil, fmt.Errorf("GetLastNOHLCVBars: failed to get Kraken pair name for %s: %w", pair, err)
	}

	// Fetch OHLCV from Kraken (OHLC endpoint uses primary pair name).
	ohlcv, err := a.client.GetOHLCV(ctx, krakenPair, interval, int64(lookbackDuration.Seconds()/60)) // since= minutes
	if err != nil {
		return nil, fmt.Errorf("GetLastNOHLCVBars: API fetch failed: %w", err)
	}

	// Parse into OHLCVBar slices.
	bars := make([]utilities.OHLCVBar, 0, len(ohlcv))
	for _, bar := range ohlcv {
		bars = append(bars, utilities.OHLCVBar{
			Timestamp: bar.Timestamp,
			Open:      bar.Open,
			High:      bar.High,
			Low:       bar.Low,
			Close:     bar.Close,
			Volume:    bar.Volume,
		})
	}

	// Sort by timestamp ascending (Kraken returns descending).
	utilities.SortBarsByTimestamp(bars)

	// Cache the bars (commented out until dataprovider supports StoreBars)
	// if err := a.cache.StoreBars(cacheProvider, cacheKey, bars); err != nil {
	// 	a.logger.LogWarn("GetLastNOHLCVBars: failed to cache bars for %s: %v", pair, err)
	// }

	// Return the last N bars.
	if len(bars) > nBars {
		return bars[len(bars)-nBars:], nil
	}
	return bars, nil
}

// PlaceOrder places a new order on Kraken.
// Supports limit, market, stop-loss, and stop-loss-limit order types.
func (a *Adapter) PlaceOrder(ctx context.Context, assetPair string, side string, orderType string, volume float64, price float64, stopPrice float64, clientOrderID string) (string, error) {
	krakenPair, err := a.client.GetTradeableKrakenPairName(ctx, assetPair)
	if err != nil {
		return "", fmt.Errorf("PlaceOrder: failed to get Kraken pair name for %s: %w", assetPair, err)
	}

	volumeStr := fmt.Sprintf("%.8f", volume)
	priceStr := ""
	if price > 0 {
		priceStr = fmt.Sprintf("%.2f", price)
	}
	stopPriceStr := ""
	if stopPrice > 0 {
		stopPriceStr = fmt.Sprintf("%.2f", stopPrice)
	}

	params := url.Values{
		"pair":      {krakenPair},
		"type":      {strings.ToLower(side)},
		"ordertype": {strings.ToLower(orderType)},
		"volume":    {volumeStr},
	}

	switch strings.ToLower(orderType) {
	case "limit":
		if priceStr == "" {
			return "", errors.New("PlaceOrder: price required for limit order")
		}
		params.Set("price", priceStr)
	case "stop-loss":
		if stopPriceStr == "" {
			return "", errors.New("PlaceOrder: stopPrice required for stop-loss order")
		}
		params.Set("price", stopPriceStr)
	case "stop-loss-limit":
		if stopPriceStr == "" || priceStr == "" {
			return "", errors.New("PlaceOrder: both price (limit) and stopPrice (trigger) required for stop-loss-limit")
		}
		params.Set("price", stopPriceStr) // trigger price
		params.Set("price2", priceStr)    // limit price
	}

	if clientOrderID != "" {
		params.Set("userref", clientOrderID)
	}

	orderID, err := a.client.AddOrderAPI(ctx, params)
	if err != nil {
		return "", fmt.Errorf("PlaceOrder: API call failed: %w", err)
	}

	a.logger.LogInfo("PlaceOrder: Successfully placed order %s for %s", orderID, assetPair)
	return orderID, nil
}

// GetOrderStatus retrieves the status of a specific order.
// This is vital for monitoring DCA safety orders and take-profit executions.
func (a *Adapter) GetOrderStatus(ctx context.Context, orderID string) (broker.Order, error) {
	orders, err := a.client.QueryOrdersAPI(ctx, orderID)
	if err != nil {
		return broker.Order{}, fmt.Errorf("GetOrderStatus: failed to query orders: %w", err)
	}

	kOrder, ok := orders[orderID]
	if !ok {
		return broker.Order{}, fmt.Errorf("GetOrderStatus: order %s not found", orderID)
	}

	commonPair, err := a.client.GetCommonPairName(ctx, kOrder.Descr.Pair)
	if err != nil {
		// Fallback for pairs that might not be in the primary map (e.g., historical)
		commonPair = kOrder.Descr.Pair
		a.logger.LogWarn("GetOrderStatus: could not get common pair name for %s. Using raw name.", kOrder.Descr.Pair)
	}

	volume, _ := strconv.ParseFloat(kOrder.Volume, 64)
	filledVolume, _ := strconv.ParseFloat(kOrder.VolExec, 64)
	cost, _ := strconv.ParseFloat(kOrder.Cost, 64)
	fee, _ := strconv.ParseFloat(kOrder.Fee, 64)
	price, _ := strconv.ParseFloat(kOrder.Descr.Price, 64)
	stopPrice, _ := strconv.ParseFloat(kOrder.StopPrice, 64)
	if stopPrice == 0 {
		stopPrice, _ = strconv.ParseFloat(kOrder.Descr.Price2, 64)
	}
	avgFillPrice, _ := strconv.ParseFloat(kOrder.Price, 64)

	parsedOrder := broker.Order{
		ID:            orderID,
		ClientOrderID: fmt.Sprintf("%v", kOrder.Userref),
		Pair:          commonPair,
		Side:          kOrder.Descr.Type,
		Type:          kOrder.Descr.OrderType,
		Status:        kOrder.Status,
		Price:         price,
		StopPrice:     stopPrice,
		Volume:        volume,
		FilledVolume:  filledVolume,
		AvgFillPrice:  avgFillPrice,
		Cost:          cost,
		Fee:           fee,
		CreatedAt:     time.Unix(int64(kOrder.Opentm), 0),
		UpdatedAt:     time.Unix(int64(kOrder.Closetm), 0),
		Reason:        kOrder.Reason,
	}

	return parsedOrder, nil
}

// GetAccountValue retrieves the total portfolio value in the specified quote currency.
// This supports circuit breaker logic and drawdown management.
func (a *Adapter) GetAccountValue(ctx context.Context, quoteCurrency string) (float64, error) {
	balances, err := a.GetAllBalances(ctx)
	if err != nil {
		return 0, fmt.Errorf("GetAccountValue: failed to get balances: %w", err)
	}

	quoteUpper := strings.ToUpper(quoteCurrency)
	totalValue := 0.0

	for _, bal := range balances {
		if bal.Total == 0 {
			continue
		}

		if strings.EqualFold(bal.Currency, quoteUpper) {
			totalValue += bal.Total
			continue
		}

		// Skip if below dust threshold
		if bal.Total < a.appConfig.Trading.DustThresholdUSD/bal.Total { // Approximate, but to skip small
			continue
		}

		pair := fmt.Sprintf("%s/%s", strings.ToUpper(bal.Currency), quoteUpper)
		ticker, err := a.GetTicker(ctx, pair)
		if err != nil {
			a.logger.LogWarn("GetAccountValue: failed to get ticker for %s, cannot value asset. Error: %v", pair, err)
			continue
		}

		totalValue += bal.Total * ticker.LastPrice
	}

	return totalValue, nil
}

// GetTicker retrieves ticker data for a specific trading pair.
// Essential for real-time pricing and decision making.
func (a *Adapter) GetTicker(ctx context.Context, pair string) (broker.TickerData, error) {
	krakenPair, err := a.client.GetTradeableKrakenPairName(ctx, pair)
	if err != nil {
		return broker.TickerData{}, fmt.Errorf("GetTicker: failed to get Kraken pair name for %s: %w", pair, err)
	}

	tickers, err := a.client.GetTickerAPI(ctx, krakenPair)
	if err != nil {
		return broker.TickerData{}, fmt.Errorf("GetTicker: API call failed: %w", err)
	}

	// The key in the response may be the primary name, not the alias we queried with.
	for _, tickerInfo := range tickers {
		return a.client.ParseTicker(tickerInfo, pair)
	}

	return broker.TickerData{}, fmt.Errorf("GetTicker: no ticker found for %s", krakenPair)
}

// GetTickers retrieves ticker data for multiple trading pairs in a single batch call.
// Optimizes API calls for scanning multiple assets.
func (a *Adapter) GetTickers(ctx context.Context, pairs []string) (map[string]broker.TickerData, error) {
	krakenPairs := make([]string, 0, len(pairs))
	commonToKraken := make(map[string]string)

	for _, p := range pairs {
		kp, err := a.client.GetTradeableKrakenPairName(ctx, p)
		if err != nil {
			a.logger.LogWarn("GetTickers: failed to get Kraken pair name for %s: %v", p, err)
			continue
		}
		krakenPairs = append(krakenPairs, kp)
		commonToKraken[kp] = p
	}

	if len(krakenPairs) == 0 {
		return nil, errors.New("GetTickers: no valid Kraken pairs could be resolved")
	}

	krakenPairStr := strings.Join(krakenPairs, ",")
	tickers, err := a.client.GetTickerAPI(ctx, krakenPairStr)
	if err != nil {
		return nil, fmt.Errorf("GetTickers: API call failed: %w", err)
	}

	result := make(map[string]broker.TickerData)
	for kp, tickerInfo := range tickers {
		commonPair, ok := commonToKraken[kp]
		if !ok {
			// This can happen if the API returns an alias key
			commonPair, err = a.client.GetCommonPairName(ctx, kp)
			if err != nil {
				a.logger.LogWarn("GetTickers: could not resolve common pair for kraken pair %s", kp)
				continue
			}
		}

		parsed, err := a.client.ParseTicker(tickerInfo, commonPair)
		if err != nil {
			a.logger.LogWarn("GetTickers: failed to parse ticker for %s: %v", commonPair, err)
			continue
		}
		result[commonPair] = parsed
	}

	return result, nil
}

// RefreshAssetInfo refreshes asset and pair information from the Kraken API.
// It is essential to call this on startup to ensure all mappings are correct.
func (a *Adapter) RefreshAssetInfo(ctx context.Context) error {
	a.logger.LogInfo("Adapter: Refreshing Kraken asset and pair information...")
	// The call to client.RefreshAssets() is removed as all logic is now consolidated
	// into the RefreshAssetPairs function for efficiency.
	if err := a.client.RefreshAssetPairs(ctx, a.assetPairs); err != nil {
		return fmt.Errorf("adapter failed to refresh Kraken asset pairs: %w", err)
	}
	a.logger.LogInfo("Adapter: Kraken asset information refreshed successfully.")
	return nil
}

func (a *Adapter) GetPairDetail(ctx context.Context, krakenPair string) (AssetPairInfo, error) {
	detail, ok := a.client.pairDetailsCache[krakenPair]
	if !ok {
		return AssetPairInfo{}, fmt.Errorf("pair detail for %s not found in map; ensure it is part of the initial configured pairs", krakenPair)
	}
	return detail, nil
}

// GetTradeFees retrieves the cached maker and taker fees for a given common pair.
func (a *Adapter) GetTradeFees(ctx context.Context, commonPair string) (float64, float64, error) {
	a.client.dataMu.RLock()
	defer a.client.dataMu.RUnlock()

	detail, ok := a.client.pairDetailsCache[commonPair]
	if !ok {
		return 0, 0, fmt.Errorf("fee details for %s not found; ensure RefreshAssetPairs ran successfully", commonPair)
	}
	return detail.MakerFee, detail.TakerFee, nil
}

// GetOrderBook retrieves the order book for a specific trading pair.
func (a *Adapter) GetOrderBook(ctx context.Context, pair string, depth int) (broker.OrderBookData, error) {
	krakenPair, err := a.client.GetTradeableKrakenPairName(ctx, pair)
	if err != nil {
		return broker.OrderBookData{}, fmt.Errorf("GetOrderBook: failed to get Kraken pair name for %s: %w", pair, err)
	}

	rawBook, err := a.client.GetOrderBookAPI(ctx, krakenPair, depth)
	if err != nil {
		return broker.OrderBookData{}, fmt.Errorf("GetOrderBook: API call failed for %s: %w", pair, err)
	}

	orderBook := broker.OrderBookData{
		Pair:      pair,
		Timestamp: time.Now().UTC(),
	}

	// Helper function to parse levels
	parseLevels := func(levels []interface{}) ([]broker.OrderBookLevel, error) {
		parsed := make([]broker.OrderBookLevel, 0, len(levels))
		for _, item := range levels {
			if level, ok := item.([]interface{}); ok && len(level) >= 2 {
				price, err1 := strconv.ParseFloat(level[0].(string), 64)
				volume, err2 := strconv.ParseFloat(level[1].(string), 64)
				if err1 != nil || err2 != nil {
					return nil, fmt.Errorf("could not parse order book level: %v", item)
				}
				parsed = append(parsed, broker.OrderBookLevel{Price: price, Volume: volume})
			}
		}
		return parsed, nil
	}

	if asks, ok := rawBook["asks"].([]interface{}); ok {
		orderBook.Asks, err = parseLevels(asks)
		if err != nil {
			return broker.OrderBookData{}, err
		}
	}

	if bids, ok := rawBook["bids"].([]interface{}); ok {
		orderBook.Bids, err = parseLevels(bids)
		if err != nil {
			return broker.OrderBookData{}, err
		}
	}

	return orderBook, nil
}

// GetTrades retrieves the trade history for a specific trading pair.
// It fetches recent trades and filters them by the 'since' timestamp.
func (a *Adapter) GetTrades(ctx context.Context, pair string, since time.Time) ([]broker.Trade, error) {
	krakenPair, err := a.client.GetTradeableKrakenPairName(ctx, pair)
	if err != nil {
		return nil, fmt.Errorf("GetTrades: failed to get Kraken pair name for %s: %w", pair, err)
	}

	// Kraken's 'since' is a transaction ID, not a timestamp. We fetch recent trades and filter.
	rawResult, err := a.client.GetTradesAPI(ctx, krakenPair, "")
	if err != nil {
		return nil, fmt.Errorf("GetTrades: API call failed for %s: %w", pair, err)
	}

	// The result is nested under the pair name key, containing a slice of trades.
	resultData, ok := rawResult.(map[string]interface{})
	if !ok {
		return nil, errors.New("unexpected format for trades API response")
	}

	var rawTrades []interface{}
	for _, v := range resultData {
		// Find the actual list of trades, ignoring the "last" ID field
		if trades, isSlice := v.([]interface{}); isSlice {
			rawTrades = trades
			break
		}
	}

	var trades []broker.Trade
	for i, item := range rawTrades {
		if tradeData, ok := item.([]interface{}); ok && len(tradeData) >= 6 {
			// Format: [price, volume, time, side, type, misc]
			price, err1 := strconv.ParseFloat(tradeData[0].(string), 64)
			volume, err2 := strconv.ParseFloat(tradeData[1].(string), 64)
			tsFloat, err3 := strconv.ParseFloat(tradeData[2].(string), 64)
			if err1 != nil || err2 != nil || err3 != nil {
				a.logger.LogWarn("GetTrades: Could not parse trade data for item %d: %v", i, tradeData)
				continue
			}

			timestamp := time.Unix(int64(tsFloat), 0)
			if timestamp.Before(since) {
				continue // Filter out trades older than the 'since' parameter
			}

			side := "unknown"
			if s, ok := tradeData[3].(string); ok {
				if s == "b" {
					side = "buy"
				} else if s == "s" {
					side = "sell"
				}
			}

			// For simplicity, we create a pseudo-ID. A real implementation might need a more robust approach.
			tradeID := fmt.Sprintf("%s-%d", krakenPair, int64(tsFloat*1e6))

			trades = append(trades, broker.Trade{
				ID:        tradeID,
				Pair:      pair,
				Price:     price,
				Volume:    volume,
				Side:      side,
				Timestamp: timestamp,
				Cost:      price * volume,
				// Fee data is not available in the public trades endpoint
			})
		}
	}
	return trades, nil
}
