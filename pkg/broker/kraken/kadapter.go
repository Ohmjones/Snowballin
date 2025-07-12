// File: pkg/broker/kraken/kadapter.go
package kraken

import (
	"Snowballin/dataprovider"
	"context"
	"errors"
	"fmt" // Added
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"Snowballin/pkg/broker"
	"Snowballin/utilities" // For logger and AppConfig
)

type Adapter struct {
	client    *Client
	logger    *utilities.Logger
	appConfig *utilities.KrakenConfig
	cache     *dataprovider.SQLiteCache
}

func NewAdapter(appCfg *utilities.KrakenConfig, HTTPClient *http.Client, logger *utilities.Logger, cache *dataprovider.SQLiteCache) (*Adapter, error) { // <-- ADD cache PARAMETER
	if appCfg == nil {
		return nil, errors.New("kraken adapter: AppConfig cannot be nil")
	}
	if logger == nil {
		logger = utilities.NewLogger(utilities.Info)
		logger.LogWarn("Kraken.Adapter: Logger fallback used for adapter.")
	}
	if cache == nil {
		return nil, errors.New("kraken adapter: SQLiteCache cannot be nil")
	}

	logger.LogInfo("Initializing Kraken Adapter...")
	krakenClient := NewClient(appCfg, HTTPClient, logger)

	adapter := &Adapter{
		client:    krakenClient,
		logger:    logger,
		appConfig: appCfg,
		cache:     cache, // <-- ADD THIS LINE
	}

	logger.LogInfo("Kraken Adapter initialized successfully.")
	return adapter, nil
}

// GetInternalClient exposes the underlying Kraken client.
func (a *Adapter) GetInternalClient() (*Client, bool) {
	if a.client != nil {
		return a.client, true
	}
	return nil, false
}

// RefreshAssetInfo ensures that the adapter's underlying client has the latest asset and pair information.
func (a *Adapter) RefreshAssetInfo(ctx context.Context) error {
	a.logger.LogInfo("Adapter: Refreshing Kraken asset information...")

	// First, refresh the assets to get the name mapping (e.g., XXBT -> XBT).
	// This is critical and must happen before refreshing pairs.
	if err := a.client.RefreshAssets(ctx); err != nil {
		return fmt.Errorf("adapter failed to refresh Kraken assets: %w", err)
	}

	// THEN, refresh the pairs. This function uses the asset map we just fetched
	// to build the translation layer correctly.
	if err := a.client.RefreshAssetPairs(ctx); err != nil {
		return fmt.Errorf("adapter failed to refresh Kraken asset pairs: %w", err)
	}

	a.logger.LogInfo("Adapter: Kraken asset information refreshed.")
	return nil
}

func (a *Adapter) GetAccountValue(ctx context.Context, quoteCurrency string) (float64, error) {
	a.logger.LogInfo("Fetching account balances from Kraken to calculate total portfolio value in %s...", quoteCurrency)
	balances, err := a.client.GetBalancesAPI(ctx)
	if err != nil {
		return 0, fmt.Errorf("GetAccountValue: failed to get balances: %w", err)
	}

	totalValue := 0.0
	quoteCurrencyUpper := strings.ToUpper(quoteCurrency)

	for krakenAssetName, balanceStr := range balances {
		if strings.HasSuffix(krakenAssetName, ".F") {
			continue
		}

		balance, err := strconv.ParseFloat(balanceStr, 64)
		if err != nil {
			a.logger.LogWarn("GetAccountValue: could not parse balance for %s ('%s'): %v", krakenAssetName, balanceStr, err)
			continue
		}

		if balance == 0 {
			continue
		}

		commonName, err := a.client.GetCommonAssetName(ctx, krakenAssetName)
		if err != nil {
			a.logger.LogWarn("GetAccountValue: could not get common name for Kraken asset %s: %v. This balance will be skipped.", krakenAssetName, err)
			continue
		}

		if strings.EqualFold(commonName, quoteCurrencyUpper) {
			totalValue += balance
			a.logger.LogDebug("GetAccountValue: Added %.2f from cash balance (%s).", balance, commonName)
		} else {
			commonPairToFetch := commonName + "/" + quoteCurrencyUpper
			krakenPairForTicker, err := a.client.GetKrakenPairName(ctx, commonPairToFetch)
			if err != nil {
				a.logger.LogWarn("GetAccountValue: could not get Kraken pair for %s to fetch ticker: %v. Cannot value asset %s.", commonPairToFetch, err, commonName)
				continue // Skip if pair is not tradable
			}

			tickerInfo, err := a.client.GetTickerAPI(ctx, krakenPairForTicker)

			// --- CRITICAL FIX: All-or-Nothing Logic ---
			if err != nil {
				// If we fail to get a price for any asset, the entire calculation is invalid.
				// Return an error to stop the trading cycle from proceeding with bad data.
				return 0, fmt.Errorf("failed to get ticker for %s (asset %s) during portfolio valuation: %w", krakenPairForTicker, commonName, err)
			}
			if len(tickerInfo.LastTradeClosed) == 0 {
				return 0, fmt.Errorf("ticker for %s (asset %s) returned no price data", krakenPairForTicker, commonName)
			}
			// --- END OF FIX ---

			lastPrice, err := strconv.ParseFloat(tickerInfo.LastTradeClosed[0], 64)
			if err != nil {
				return 0, fmt.Errorf("could not parse price for %s: %w", commonName, err)
			}

			assetValueInQuote := balance * lastPrice
			totalValue += assetValueInQuote
			a.logger.LogDebug("GetAccountValue: Valued %f of %s at %.2f %s/COIN, adding %.2f to total.", balance, commonName, lastPrice, quoteCurrencyUpper, assetValueInQuote)
		}
	}

	a.logger.LogInfo("Calculated total account value: %.2f %s", totalValue, quoteCurrencyUpper)
	return totalValue, nil
}

func (a *Adapter) GetOrderBook(ctx context.Context, commonPair string, depth int) (broker.OrderBookData, error) {
	krakenPair, err := a.client.GetKrakenPairName(ctx, commonPair)
	if err != nil {
		return broker.OrderBookData{}, fmt.Errorf("GetOrderBook: failed to get Kraken pair name for %s: %w", commonPair, err)
	}

	rawOrderBook, err := a.client.GetOrderBookAPI(ctx, krakenPair, depth) // New method in client
	if err != nil {
		return broker.OrderBookData{}, err
	}

	bids := make([]broker.OrderBookLevel, len(rawOrderBook.Bids))
	for i, b := range rawOrderBook.Bids {
		price, pErr := utilities.ParseFloatFromInterface(b[0])
		volume, vErr := utilities.ParseFloatFromInterface(b[1])
		if pErr != nil || vErr != nil {
			return broker.OrderBookData{}, fmt.Errorf("error parsing bid level %d: priceErr=%v, volErr=%v", i, pErr, vErr)
		}
		bids[i] = broker.OrderBookLevel{Price: price, Volume: volume}
	}

	asks := make([]broker.OrderBookLevel, len(rawOrderBook.Asks))
	for i, ask := range rawOrderBook.Asks {
		price, pErr := utilities.ParseFloatFromInterface(ask[0])
		volume, vErr := utilities.ParseFloatFromInterface(ask[1])
		if pErr != nil || vErr != nil {
			return broker.OrderBookData{}, fmt.Errorf("error parsing ask level %d: priceErr=%v, volErr=%v", i, pErr, vErr)
		}
		asks[i] = broker.OrderBookLevel{Price: price, Volume: volume}
	}

	return broker.OrderBookData{
		Pair:      commonPair, // Return common pair name
		Bids:      bids,
		Asks:      asks,
		Timestamp: time.Now().UTC(), // Timestamp of when this conversion happened
	}, nil
}

func (a *Adapter) GetLastNOHLCVBars(ctx context.Context, pair string, intervalMinutes string, nBars int) ([]utilities.OHLCVBar, error) {
	// For caching, we define a provider name and a unique ID for the data.
	// The pair and interval make a good unique ID for Kraken's data.
	cacheProvider := "kraken"
	cacheCoinID := fmt.Sprintf("%s-%s", pair, intervalMinutes)

	// Define lookback period for cache retrieval
	intervalDuration, _ := time.ParseDuration(fmt.Sprintf("%sm", intervalMinutes))
	lookbackDuration := time.Duration(nBars) * intervalDuration
	startTime := time.Now().Add(-lookbackDuration)

	// 1. Try to fetch from cache
	cachedBars, err := a.cache.GetBars(cacheProvider, cacheCoinID, startTime.UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		a.logger.LogWarn("kadapter GetLastNOHLCVBars [%s]: Failed to get bars from cache: %v", pair, err)
	}
	// Use cache if we have enough recent data
	if len(cachedBars) >= nBars {
		a.logger.LogInfo("kadapter GetLastNOHLCVBars [%s]: Using %d bars from cache.", pair, len(cachedBars))
		return cachedBars, nil
	}

	// 2. If cache miss or insufficient, fetch from API
	a.logger.LogInfo("kadapter GetLastNOHLCVBars [%s]: Cache miss or insufficient data. Fetching from API.", pair)
	krakenPair, err := a.client.GetKrakenPairName(ctx, pair)
	if err != nil {
		return nil, fmt.Errorf("GetLastNOHLCVBars: failed to get Kraken pair name for %s: %w", pair, err)
	}

	krakenOHLCV, err := a.client.GetOHLCVAPI(ctx, krakenPair, intervalMinutes, 0, nBars)
	if err != nil {
		return nil, fmt.Errorf("GetLastNOHLCVBars: client call failed for %s: %w", pair, err)
	}

	var ohlcvBars []utilities.OHLCVBar
	for _, kBar := range krakenOHLCV {
		if len(kBar) < 7 {
			continue
		}
		ts, errTs := utilities.ParseFloatFromInterface(kBar[0])
		open, errO := utilities.ParseFloatFromInterface(kBar[1])
		high, errH := utilities.ParseFloatFromInterface(kBar[2])
		low, errL := utilities.ParseFloatFromInterface(kBar[3])
		closeVal, errC := utilities.ParseFloatFromInterface(kBar[4])
		volume, errV := utilities.ParseFloatFromInterface(kBar[6])
		if errTs != nil || errO != nil || errH != nil || errL != nil || errC != nil || errV != nil {
			continue
		}
		bar := utilities.OHLCVBar{
			Timestamp: int64(ts) * 1000, // Kraken uses seconds, convert to milliseconds for consistency
			Open:      open, High: high, Low: low, Close: closeVal, Volume: volume,
		}
		ohlcvBars = append(ohlcvBars, bar)

		// 3. Save the newly fetched bar to the cache
		if err := a.cache.SaveBar(cacheProvider, cacheCoinID, bar); err != nil {
			a.logger.LogWarn("kadapter GetLastNOHLCVBars [%s]: Failed to save bar to cache: %v", pair, err)
		}
	}

	sort.Slice(ohlcvBars, func(i, j int) bool {
		return ohlcvBars[i].Timestamp < ohlcvBars[j].Timestamp
	})

	if len(ohlcvBars) > nBars {
		ohlcvBars = ohlcvBars[len(ohlcvBars)-nBars:]
	}

	return ohlcvBars, nil
}

// CalculateMarketCap computes the market cap using Kraken price and external circulating supply.
func (ka *Adapter) CalculateMarketCap(ctx context.Context, pair string, circulatingSupply float64) (float64, error) {
	ticker, err := ka.client.GetTickerAPI(ctx, pair)
	if err != nil {
		ka.logger.LogError("CalculateMarketCap: Failed to fetch ticker for %s: %v", pair, err)
		return 0, err
	}

	price, err := strconv.ParseFloat(ticker.LastTradeClosed[0], 64)
	if err != nil {
		ka.logger.LogError("CalculateMarketCap: Price conversion error for %s: %v", pair, err)
		return 0, err
	}

	marketCap := price * circulatingSupply
	ka.logger.LogDebug("CalculateMarketCap: Kraken market cap for %s calculated as %f", pair, marketCap)
	return marketCap, nil
}

// Ensure other methods from broker.Broker interface are implemented
// PlaceOrder, CancelOrder, GetOrderStatus, GetBalance, GetTicker, GetTrades are already in your kadapter.go

func (a *Adapter) PlaceOrder(ctx context.Context, assetPair, side, orderType string, volume, price, stopPrice float64, clientOrderID string) (string, error) {
	krakenPair, err := a.client.GetKrakenPairName(ctx, assetPair)
	if err != nil {
		return "", err
	}

	pairDetail, err := a.client.GetPairDetail(ctx, krakenPair)
	if err != nil {
		return "", err
	}

	params := url.Values{
		"pair":      {krakenPair},
		"type":      {strings.ToLower(side)},
		"ordertype": {strings.ToLower(orderType)},
		"volume":    {strconv.FormatFloat(volume, 'f', pairDetail.LotDecimals, 64)},
	}

	if strings.Contains(orderType, "limit") {
		params.Set("price", strconv.FormatFloat(price, 'f', pairDetail.PairDecimals, 64))
	}
	if strings.Contains(orderType, "stop") {
		params.Set("price", strconv.FormatFloat(stopPrice, 'f', pairDetail.PairDecimals, 64))
	}
	if clientOrderID != "" {
		params.Set("userref", clientOrderID)
	}

	return a.client.AddOrderAPI(ctx, params)
}

func (a *Adapter) CancelOrder(ctx context.Context, orderID string) error {
	if orderID == "" {
		return errors.New("CancelOrder: orderID cannot be empty")
	}
	return a.client.CancelOrderAPI(ctx, orderID)
}

func (a *Adapter) GetOrderStatus(ctx context.Context, orderID string) (broker.Order, error) {
	if orderID == "" {
		return broker.Order{}, errors.New("GetOrderStatus: orderID empty")
	}

	ordersInfo, err := a.client.QueryOrdersAPI(ctx, orderID)
	if err != nil {
		return broker.Order{}, err
	}

	order, ok := ordersInfo[orderID]
	if !ok {
		return broker.Order{}, errors.New("GetOrderStatus: order not found")
	}

	price, err := strconv.ParseFloat(order.Price, 64)
	if err != nil {
		return broker.Order{}, err
	}
	volume, err := strconv.ParseFloat(order.Volume, 64)
	if err != nil {
		return broker.Order{}, err
	}
	executedVol, err := strconv.ParseFloat(order.VolExec, 64)
	if err != nil {
		return broker.Order{}, err
	}
	fee, err := strconv.ParseFloat(order.Fee, 64)
	if err != nil {
		return broker.Order{}, err
	}

	return broker.Order{
		ID:            orderID,
		Status:        order.Status,
		Pair:          order.Descr.Pair,
		Type:          order.Descr.Type,
		OrderType:     order.Descr.OrderType,
		Price:         price,
		Volume:        volume,
		ExecutedVol:   executedVol,
		Fee:           fee,
		TimePlaced:    time.Unix(int64(order.Opentm), 0),
		TimeCompleted: time.Unix(int64(order.Closetm), 0),
	}, nil
}

func (a *Adapter) GetBalance(ctx context.Context, currency string) (broker.Balance, error) {
	balances, err := a.client.GetBalancesAPI(ctx)
	if err != nil {
		return broker.Balance{}, err
	}
	balanceStr, exists := balances[currency]
	if !exists {
		return broker.Balance{}, errors.New("GetBalance: currency not found")
	}
	bal, err := strconv.ParseFloat(balanceStr, 64)
	if err != nil {
		return broker.Balance{}, err
	}
	return broker.Balance{
		Currency:  currency,
		Total:     bal,
		Available: bal,
	}, nil
}

func (a *Adapter) GetTicker(ctx context.Context, pair string) (broker.TickerData, error) {
	krakenPair, err := a.client.GetKrakenPairName(ctx, pair)
	if err != nil {
		return broker.TickerData{}, err
	}

	tickerInfo, err := a.client.GetTickerAPI(ctx, krakenPair)
	if err != nil {
		return broker.TickerData{}, err
	}

	lastPrice, err := strconv.ParseFloat(tickerInfo.LastTradeClosed[0], 64)
	if err != nil {
		return broker.TickerData{}, err
	}
	highPrice, err := strconv.ParseFloat(tickerInfo.High[1], 64) // 24-hour high
	if err != nil {
		return broker.TickerData{}, err
	}
	lowPrice, err := strconv.ParseFloat(tickerInfo.Low[1], 64) // 24-hour low
	if err != nil {
		return broker.TickerData{}, err
	}
	volume, err := strconv.ParseFloat(tickerInfo.Volume[1], 64) // 24-hour volume
	if err != nil {
		return broker.TickerData{}, err
	}

	return broker.TickerData{
		Pair:      pair,
		LastPrice: lastPrice,
		High:      highPrice,
		Low:       lowPrice,
		Volume:    volume,
		Timestamp: time.Now(),
	}, nil
}

func (a *Adapter) GetTrades(ctx context.Context, pair string, since time.Time) ([]broker.Trade, error) {
	params := url.Values{}
	if !since.IsZero() {
		params.Set("start", strconv.FormatInt(since.Unix(), 10))
	}

	tradesMap, _, err := a.client.QueryTradesAPI(ctx, params)
	if err != nil {
		return nil, err
	}

	var trades []broker.Trade
	for tradeID, trade := range tradesMap {
		if pair == "" || strings.EqualFold(trade.Pair, pair) {
			price, err := strconv.ParseFloat(trade.Price, 64)
			if err != nil {
				return nil, err
			}
			volume, err := strconv.ParseFloat(trade.Vol, 64)
			if err != nil {
				return nil, err
			}
			fee, err := strconv.ParseFloat(trade.Fee, 64)
			if err != nil {
				return nil, err
			}
			cost, err := strconv.ParseFloat(trade.Cost, 64)
			if err != nil {
				return nil, err
			}

			trades = append(trades, broker.Trade{
				ID:          tradeID,
				OrderID:     trade.Ordtxid,
				Pair:        trade.Pair,
				Side:        trade.Type,
				Price:       price,
				Volume:      volume,
				Cost:        cost,
				Fee:         fee,
				FeeCurrency: strings.Split(trade.Pair, "/")[1], // Usually the quote currency
				Timestamp:   time.Unix(int64(trade.Time), 0),
			})
		}
	}
	return trades, nil
}
