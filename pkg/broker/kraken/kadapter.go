// File: pkg/broker/kraken/kadapter.go
package kraken

import (
	"Snowballin/dataprovider"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
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
	appConfig  *utilities.KrakenConfig   // Configuration for Kraken-specific settings
	cache      *dataprovider.SQLiteCache // Cache for OHLCV, tickers, and fees
	client     *Client                   // Underlying Kraken API client
	logger     *utilities.Logger         // Logger for adapter operations
	assetPairs []string                  // List of common asset pairs from config (e.g., ["BTC/USD"])
}

// NewAdapter creates a new Kraken Adapter instance.
// It initializes with provided config, the list of configured asset pairs, the client, logger, and cache.
// The assetPairs are required to correctly initialize the client's asset mapping.
func NewAdapter(appCfg *utilities.KrakenConfig, assetPairs []string, client *Client, logger *utilities.Logger, cache *dataprovider.SQLiteCache) (*Adapter, error) {
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

	// Build the public request for OHLC (no auth needed for public endpoints).
	// NOTE: This logic was previously creating a new request, but the client already has a method for this.
	// We will now use the client's built-in `GetOHLCVAPI` method for consistency and to simplify this function.
	rawBars, err := a.client.GetOHLCVAPI(ctx, krakenPair, interval, startTime.Unix())
	if err != nil {
		return nil, fmt.Errorf("GetLastNOHLCVBars: API call failed: %w", err)
	}

	// Log the raw response for debugging.
	responseJSON, _ := json.Marshal(rawBars)
	a.logger.LogDebug("kadapter GetLastNOHLCVBars [%s]: Raw API response: %s", pair, string(responseJSON))

	// Parse the OHLCV bars.
	var bars []utilities.OHLCVBar
	for _, rawBar := range rawBars {
		bar, err := a.client.ParseOHLCV(rawBar)
		if err != nil {
			a.logger.LogWarn("GetLastNOHLCVBars [%s]: Failed to parse an OHLCV bar, skipping: %v", pair, err)
			continue
		}
		bars = append(bars, bar)
	}

	// Check if any valid bars were parsed.
	if len(bars) == 0 {
		// This can be a valid state if there's no data for the requested period, so we return an empty slice instead of an error.
		a.logger.LogInfo("GetLastNOHLCVBars: no valid OHLCV bars were returned or parsed for pair %s.", pair)
		return []utilities.OHLCVBar{}, nil
	}

	// Sort bars by timestamp to ensure ascending order.
	utilities.SortBarsByTimestamp(bars)

	// Save the newly fetched bars to the cache one by one.
	for _, bar := range bars {
		if saveErr := a.cache.SaveBar(cacheProvider, cacheKey, bar); saveErr != nil {
			// Log the error but don't fail the entire operation.
			a.logger.LogWarn("GetLastNOHLCVBars [%s]: Failed to save a single bar to cache: %v", pair, saveErr)
		}
	}

	// Return the last N bars, trimming if necessary.
	if len(bars) > nBars {
		bars = bars[len(bars)-nBars:]
	}

	// Log success and return the bars.
	a.logger.LogInfo("kadapter GetLastNOHLCVBars [%s]: Successfully fetched and parsed %d bars.", pair, len(bars))
	return bars, nil
}

// GetOrderBook retrieves the order book snapshot for a trading pair from Kraken.
// Supports liquidity hunt and predictive order placement logic.
func (a *Adapter) GetOrderBook(ctx context.Context, pair string, depth int) (broker.OrderBookData, error) {
	krakenPair, err := a.client.GetPrimaryKrakenPairName(ctx, pair)
	if err != nil {
		return broker.OrderBookData{}, fmt.Errorf("GetOrderBook: failed to get Kraken pair name for %s: %w", pair, err)
	}

	rawBook, err := a.client.GetOrderBookAPI(ctx, krakenPair, depth)
	if err != nil {
		return broker.OrderBookData{}, fmt.Errorf("GetOrderBook: API call failed: %w", err)
	}

	return a.client.ParseOrderBook(rawBook, pair)
}

// GetTradeFees retrieves the maker and taker fees for a trading pair from cache or API.
// Fees are crucial for calculating profit and loss accurately.
func (a *Adapter) GetTradeFees(ctx context.Context, commonPair string) (makerFee float64, takerFee float64, err error) {
	krakenPair, err := a.client.GetPrimaryKrakenPairName(ctx, commonPair)
	if err != nil {
		return 0, 0, fmt.Errorf("GetTradeFees: failed to get Kraken pair name for %s: %w", commonPair, err)
	}

	makerFee, takerFee, fresh, cacheErr := a.cache.GetCachedFees(krakenPair)
	if cacheErr == nil && fresh {
		a.logger.LogInfo("GetTradeFees [%s]: Cache hit (fresh). Maker: %.4f, Taker: %.4f", commonPair, makerFee, takerFee)
		return makerFee, takerFee, nil
	}
	a.logger.LogDebug("GetTradeFees [%s]: Cache miss or stale. Fetching from API.", commonPair)

	params := url.Values{"pair": {krakenPair}, "fee-info": {"true"}}
	tradeVolumeResult, apiErr := a.client.QueryTradeVolumeAPI(ctx, params)
	if apiErr != nil {
		return 0, 0, fmt.Errorf("GetTradeFees: client API call failed: %w", apiErr)
	}

	if feeInfo, ok := tradeVolumeResult.Fees[krakenPair]; ok {
		parsedFee, parseErr := strconv.ParseFloat(feeInfo.Fee, 64)
		if parseErr != nil {
			return 0, 0, fmt.Errorf("GetTradeFees: could not parse taker fee '%s': %w", feeInfo.Fee, parseErr)
		}
		takerFee = parsedFee / 100.0
	} else {
		return 0, 0, fmt.Errorf("GetTradeFees: no taker fee info found for pair %s in API response", krakenPair)
	}

	if feeInfo, ok := tradeVolumeResult.FeesMaker[krakenPair]; ok {
		parsedFee, parseErr := strconv.ParseFloat(feeInfo.Fee, 64)
		if parseErr != nil {
			return 0, 0, fmt.Errorf("GetTradeFees: could not parse maker fee '%s': %w", feeInfo.Fee, parseErr)
		}
		makerFee = parsedFee / 100.0
	} else {
		a.logger.LogWarn("GetTradeFees: no maker-specific fee info for %s, falling back to using taker fee for both.", krakenPair)
		makerFee = takerFee
	}
	if saveErr := a.cache.SaveFees(krakenPair, makerFee, takerFee); saveErr != nil {
		a.logger.LogWarn("GetTradeFees [%s]: Failed to cache fees: %v", commonPair, saveErr)
	}

	return makerFee, takerFee, nil
}

// GetTrades retrieves trade history for a trading pair since a given time.
// Used for post-trade analysis and performance tracking.
func (a *Adapter) GetTrades(ctx context.Context, pair string, since time.Time) ([]broker.Trade, error) {
	krakenPair, err := a.client.GetPrimaryKrakenPairName(ctx, pair)
	if err != nil {
		return nil, fmt.Errorf("GetTrades: failed to get Kraken pair name for %s: %w", pair, err)
	}

	params := url.Values{"pair": {krakenPair}}
	if !since.IsZero() {
		params.Set("start", strconv.FormatInt(since.Unix(), 10))
	}

	krakenTrades, _, err := a.client.QueryTradesAPI(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("GetTrades: API call failed: %w", err)
	}

	var trades []broker.Trade
	for tradeID, kTrade := range krakenTrades {
		commonPair, err := a.client.GetCommonPairName(ctx, kTrade.Pair)
		if err != nil {
			a.logger.LogWarn("GetTrades: Could not resolve common pair for %s, skipping trade %s", kTrade.Pair, tradeID)
			continue
		}

		price, _ := strconv.ParseFloat(kTrade.Price, 64)
		volume, _ := strconv.ParseFloat(kTrade.Vol, 64)
		cost, _ := strconv.ParseFloat(kTrade.Cost, 64)
		fee, _ := strconv.ParseFloat(kTrade.Fee, 64)
		// Fee currency is not directly in the trade history, but can be inferred from the quote currency of the pair.
		quoteCurrency := strings.Split(commonPair, "/")[1]

		trades = append(trades, broker.Trade{
			ID:          tradeID,
			OrderID:     kTrade.Ordtxid,
			Pair:        commonPair,
			Side:        kTrade.Type,
			Price:       price,
			Volume:      volume,
			Cost:        cost,
			Fee:         fee,
			FeeCurrency: quoteCurrency,
			Timestamp:   time.Unix(int64(kTrade.Time), 0),
		})
	}

	sort.Slice(trades, func(i, j int) bool {
		return trades[i].Timestamp.Before(trades[j].Timestamp)
	})

	return trades, nil
}

// PlaceOrder submits a new order to Kraken.
// This is a core method for executing the trading strategy.
func (a *Adapter) PlaceOrder(ctx context.Context, assetPair, side, orderType string, volume, price, stopPrice float64, clientOrderID string) (string, error, string) {
	krakenPair, err := a.client.GetTradeableKrakenPairName(ctx, assetPair)
	if err != nil {
		return "", fmt.Errorf("PlaceOrder: failed to get Kraken pair name for %s: %w", assetPair, err), ""
	}

	pairDetail, err := a.client.GetPairDetail(ctx, krakenPair)
	if err != nil {
		return "", fmt.Errorf("PlaceOrder: could not get pair details for %s: %w", krakenPair, err), ""
	}

	minOrder, minErr := strconv.ParseFloat(pairDetail.OrderMin, 64)
	if minErr != nil {
		return "", fmt.Errorf("PlaceOrder: invalid min order for %s: %w", krakenPair, minErr), ""
	}
	if volume < minOrder {
		return "", fmt.Errorf("PlaceOrder: volume %.8f below min %.8f for %s", volume, minOrder, assetPair), ""
	}

	volumeStr := fmt.Sprintf("%."+strconv.Itoa(pairDetail.LotDecimals)+"f", volume)
	priceStr := ""
	if price > 0 {
		priceStr = fmt.Sprintf("%."+strconv.Itoa(pairDetail.PairDecimals)+"f", price)
	}

	stopPriceStr := ""
	if stopPrice > 0 {
		stopPriceStr = fmt.Sprintf("%."+strconv.Itoa(pairDetail.PairDecimals)+"f", stopPrice)
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
			return "", errors.New("PlaceOrder: price required for limit order"), ""
		}
		params.Set("price", priceStr)
	case "stop-loss":
		if stopPriceStr == "" {
			return "", errors.New("PlaceOrder: stopPrice required for stop-loss order"), ""
		}
		params.Set("price", stopPriceStr)
	case "stop-loss-limit":
		if stopPriceStr == "" || priceStr == "" {
			return "", errors.New("PlaceOrder: both price (limit) and stopPrice (trigger) required for stop-loss-limit"), ""
		}
		params.Set("price", stopPriceStr) // trigger price
		params.Set("price2", priceStr)    // limit price
	}

	if clientOrderID != "" {
		params.Set("userref", clientOrderID)
	}

	orderID, descr, err := a.client.AddOrderAPI(ctx, params)
	if err != nil {
		return "", fmt.Errorf("PlaceOrder: API call failed: %w", err), ""
	}

	a.logger.LogInfo("PlaceOrder: Successfully placed order %s for %s (%s)", orderID, assetPair, descr)
	return orderID, nil, descr
}

// RefreshAssetInfo refreshes asset and pair information from the Kraken API.
// It is essential to call this on startup to ensure all mappings are correct.
func (a *Adapter) RefreshAssetInfo(ctx context.Context) error {
	a.logger.LogInfo("Adapter: Refreshing Kraken asset information...")
	if err := a.client.RefreshAssets(ctx); err != nil {
		return fmt.Errorf("adapter failed to refresh Kraken assets: %w", err)
	}
	if err := a.client.RefreshAssetPairs(ctx, a.assetPairs); err != nil {
		return fmt.Errorf("adapter failed to refresh Kraken asset pairs: %w", err)
	}
	a.logger.LogInfo("Adapter: Kraken asset information refreshed.")
	return nil
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

	totalValue := 0.0
	quoteUpper := strings.ToUpper(quoteCurrency)

	for _, bal := range balances {
		if strings.EqualFold(bal.Currency, quoteUpper) {
			totalValue += bal.Total
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
	krakenPair, err := a.client.GetPrimaryKrakenPairName(ctx, pair)
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
		kp, err := a.client.GetPrimaryKrakenPairName(ctx, p)
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
