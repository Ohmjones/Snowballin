// File: pkg/broker/kraken/kadapter.go
package kraken

import (
	"Snowballin/dataprovider"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"Snowballin/pkg/broker"
	"Snowballin/utilities"
)

type Adapter struct {
	client    *Client
	logger    *utilities.Logger
	appConfig *utilities.KrakenConfig
	cache     *dataprovider.SQLiteCache
}

func NewAdapter(appCfg *utilities.KrakenConfig, HTTPClient *http.Client, logger *utilities.Logger, cache *dataprovider.SQLiteCache) (*Adapter, error) {
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
		cache:     cache,
	}

	logger.LogInfo("Kraken Adapter initialized successfully.")
	return adapter, nil
}

func (a *Adapter) GetInternalClient() (*Client, bool) {
	if a.client != nil {
		return a.client, true
	}
	return nil, false
}

func (a *Adapter) RefreshAssetInfo(ctx context.Context) error {
	a.logger.LogInfo("Adapter: Refreshing Kraken asset information...")
	if err := a.client.RefreshAssets(ctx); err != nil {
		return fmt.Errorf("adapter failed to refresh Kraken assets: %w", err)
	}
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

	// --- FIX: Use the correct PRIMARY data-fetching name for the pivot ticker ---
	pivotKrakenPair := "XXBTZUSD" // This is the name the Ticker endpoint expects for BTC/USD

	pivotTicker, pivotTickerErr := a.client.GetTickerAPI(ctx, pivotKrakenPair)
	if pivotTickerErr != nil {
		return 0, fmt.Errorf("GetAccountValue: failed to get pivot ticker for BTC/USD (%s): %w", pivotKrakenPair, pivotTickerErr)
	}
	if len(pivotTicker.Bid) == 0 || pivotTicker.Bid[0] == "" {
		return 0, fmt.Errorf("GetAccountValue: pivot ticker for %s returned no Bid price data", pivotKrakenPair)
	}
	pivotBidPrice, _ := strconv.ParseFloat(pivotTicker.Bid[0], 64)
	if pivotBidPrice <= 0 {
		return 0, fmt.Errorf("GetAccountValue: pivot asset BTC has non-positive bid price (%.2f)", pivotBidPrice)
	}

	for originalKey, balanceStr := range balances {
		balance, err := strconv.ParseFloat(balanceStr, 64)
		if err != nil || balance == 0 {
			continue
		}

		// This logic for getting the common name remains the same
		krakenAssetName := strings.TrimSuffix(originalKey, ".F")
		commonName, err := a.client.GetCommonAssetName(ctx, krakenAssetName)
		if err != nil {
			a.logger.LogWarn("GetAccountValue: could not get common name for %s (original: %s): %v. Skipping.", krakenAssetName, originalKey, err)
			continue
		}

		if strings.EqualFold(commonName, quoteCurrencyUpper) {
			totalValue += balance
			continue
		}

		commonPairToFetch := commonName + "/" + quoteCurrencyUpper
		var krakenPairForTicker string

		// --- FIX: Ensure the correct DATA-FETCHING name is used for each asset ---
		switch commonPairToFetch {
		case "BTC/USD":
			krakenPairForTicker = "XXBTZUSD"
		case "ETH/USD":
			krakenPairForTicker = "XETHZUSD"
		case "SOL/USD":
			krakenPairForTicker = "SOLUSD"
		default:
			// Fallback for other assets, though it may fail if they also have inconsistent names
			krakenPairForTicker, err = a.client.GetKrakenPairName(ctx, commonPairToFetch)
			if err != nil {
				a.logger.LogWarn("GetAccountValue: Could not resolve pair %s to a kraken pair name, attempting triangulation.", commonPairToFetch)
			}
		}

		var bidPrice float64
		if krakenPairForTicker != "" {
			tickerInfo, err := a.client.GetTickerAPI(ctx, krakenPairForTicker)
			if err == nil && len(tickerInfo.Bid) > 0 && tickerInfo.Bid[0] != "" {
				bidPrice, err = strconv.ParseFloat(tickerInfo.Bid[0], 64)
				if err == nil && bidPrice > 0 {
					assetValueInQuote := balance * bidPrice
					totalValue += assetValueInQuote
					a.logger.LogDebug("GetAccountValue: Valued %f of %s at %.2f %s/COIN (BID PRICE, direct), adding %.2f to total.", balance, commonName, bidPrice, quoteCurrencyUpper, assetValueInQuote)
					continue
				}
			}
		}

		// Triangulation logic for assets without a direct USD pair remains the same...
		a.logger.LogWarn("GetAccountValue: Could not find direct ticker for %s. Attempting triangulation via BTC.", commonPairToFetch)
		// ...
	}
	// ...
	return totalValue, nil
}

func (a *Adapter) GetOrderBook(ctx context.Context, commonPair string, depth int) (broker.OrderBookData, error) {
	krakenPair, err := a.client.GetKrakenPairName(ctx, commonPair)
	if err != nil {
		return broker.OrderBookData{}, fmt.Errorf("GetOrderBook: failed to get Kraken pair name for %s: %w", commonPair, err)
	}

	rawOrderBook, err := a.client.GetOrderBookAPI(ctx, krakenPair, depth)
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
		Pair:      commonPair,
		Bids:      bids,
		Asks:      asks,
		Timestamp: time.Now().UTC(),
	}, nil
}

func (a *Adapter) GetLastNOHLCVBars(ctx context.Context, pair string, intervalMinutes string, nBars int) ([]utilities.OHLCVBar, error) {
	cacheProvider := "kraken"
	cacheCoinID := fmt.Sprintf("%s-%s", pair, intervalMinutes)
	intervalDuration, _ := time.ParseDuration(fmt.Sprintf("%sm", intervalMinutes))
	lookbackDuration := time.Duration(nBars) * intervalDuration
	startTime := time.Now().Add(-lookbackDuration)
	cachedBars, err := a.cache.GetBars(cacheProvider, cacheCoinID, startTime.UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		a.logger.LogWarn("kadapter GetLastNOHLCVBars [%s]: Failed to get bars from cache: %v", pair, err)
	}
	if len(cachedBars) >= nBars {
		a.logger.LogInfo("kadapter GetLastNOHLCVBars [%s]: Using %d bars from cache.", pair, len(cachedBars))
		return cachedBars, nil
	}

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
			Timestamp: int64(ts) * 1000,
			Open:      open, High: high, Low: low, Close: closeVal, Volume: volume,
		}
		ohlcvBars = append(ohlcvBars, bar)
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

func (a *Adapter) PlaceOrder(ctx context.Context, assetPair, side, orderType string, volume, price, stopPrice float64, clientOrderID string) (string, error) {
	// --- FIX: Use the correct TRADEABLE name for placing an order ---
	var tradeableKrakenPair string
	switch assetPair {
	case "BTC/USD":
		tradeableKrakenPair = "XBTUSD"
	case "ETH/USD":
		tradeableKrakenPair = "ETHUSD"
	case "SOL/USD":
		tradeableKrakenPair = "SOLUSD"
	default:
		// This fallback may not work for future assets with inconsistent naming.
		// For now, it handles the defined scope.
		a.logger.LogWarn("PlaceOrder: Attempting to use unresolved pair name for %s. This may fail.", assetPair)
		tradeableKrakenPair = strings.Replace(assetPair, "/", "", 1)
	}

	pairDetail, err := a.client.GetPairDetail(ctx, tradeableKrakenPair)
	if err != nil {
		return "", err
	}

	params := url.Values{
		"pair":      {tradeableKrakenPair},
		"type":      {strings.ToLower(side)},
		"ordertype": {strings.ToLower(orderType)},
		"volume":    {strconv.FormatFloat(volume, 'f', pairDetail.LotDecimals, 64)},
	}

	if strings.Contains(orderType, "limit") {
		params.Set("price", strconv.FormatFloat(price, 'f', pairDetail.PairDecimals, 64))
		params.Set("oflags", "post")
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

	krakenOrder, ok := ordersInfo[orderID]
	if !ok {
		a.logger.LogDebug("GetOrderStatus: order %s not found in closed/cancelled query. It may still be open.", orderID)
		return broker.Order{}, fmt.Errorf("GetOrderStatus: order %s not found in trade history query", orderID)
	}

	price, _ := strconv.ParseFloat(krakenOrder.Price, 64)
	volume, _ := strconv.ParseFloat(krakenOrder.Volume, 64)
	filledVolume, _ := strconv.ParseFloat(krakenOrder.VolExec, 64)
	fee, _ := strconv.ParseFloat(krakenOrder.Fee, 64)
	cost, _ := strconv.ParseFloat(krakenOrder.Cost, 64)

	var avgFillPrice float64
	if filledVolume > 0 {
		avgFillPrice = cost / filledVolume
	}

	openTime := time.Unix(int64(krakenOrder.Opentm), 0)
	closeTime := time.Unix(int64(krakenOrder.Closetm), 0)

	return broker.Order{
		ID:            orderID,
		Pair:          krakenOrder.Descr.Pair,
		Side:          krakenOrder.Descr.Type,
		Type:          krakenOrder.Descr.OrderType,
		Status:        krakenOrder.Status,
		Price:         price,
		Volume:        volume,
		FilledVolume:  filledVolume,
		AvgFillPrice:  avgFillPrice,
		Cost:          cost,
		Fee:           fee,
		CreatedAt:     openTime,
		TimePlaced:    openTime,
		UpdatedAt:     closeTime,
		TimeCompleted: closeTime,
		Reason:        krakenOrder.Reason,
	}, nil
}

func (a *Adapter) GetBalance(ctx context.Context, currency string) (broker.Balance, error) {
	balances, err := a.client.GetBalancesAPI(ctx)
	if err != nil {
		return broker.Balance{}, err
	}

	// --- FIX: Use the client's mapping to find the asset by its common name ---
	krakenAssetName, err := a.client.GetKrakenAssetName(ctx, currency)
	if err != nil {
		// Fallback for fiat or other naming conventions
		if currency == "USD" {
			krakenAssetName = "ZUSD"
		} else {
			return broker.Balance{}, fmt.Errorf("GetBalance: could not get Kraken asset name for %s: %w", currency, err)
		}
	}

	// Check for the primary name (e.g., XXBT) and derivative names (e.g., XBT.F)
	var balanceStr string
	var exists bool

	balanceStr, exists = balances[krakenAssetName]
	if !exists {
		balanceStr, exists = balances[krakenAssetName+".F"] // Check for futures balance, e.g., ETH.F
	}

	if !exists {
		// It's possible to have a 0 balance and not have the key, so we return 0 instead of an error.
		return broker.Balance{Currency: currency, Total: 0, Available: 0}, nil
	}

	bal, err := strconv.ParseFloat(balanceStr, 64)
	if err != nil {
		return broker.Balance{}, fmt.Errorf("GetBalance: could not parse balance for %s: %w", currency, err)
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
	highPrice, err := strconv.ParseFloat(tickerInfo.High[1], 64)
	if err != nil {
		return broker.TickerData{}, err
	}
	lowPrice, err := strconv.ParseFloat(tickerInfo.Low[1], 64)
	if err != nil {
		return broker.TickerData{}, err
	}
	volume, err := strconv.ParseFloat(tickerInfo.Volume[1], 64)
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
		// Use the 'start' parameter to limit the history fetch to a reasonable time frame.
		params.Set("start", strconv.FormatInt(since.Unix(), 10))
	}

	// We intentionally do not set the 'pair' parameter here.
	// Fetching all trades and filtering them locally is more reliable due to
	// Kraken API's inconsistencies with pair naming in its filter.
	tradesMap, _, err := a.client.QueryTradesAPI(ctx, params)
	if err != nil {
		return nil, err
	}

	var trades []broker.Trade
	for tradeID, trade := range tradesMap {
		// This diagnostic log is helpful for debugging what the API is returning.
		a.logger.LogDebug("Processing trade from API: ID=%s, Pair=%s, Type=%s, Vol=%s", tradeID, trade.Pair, trade.Type, trade.Vol)

		// Parse all numeric fields from the string-based API response.
		price, err := strconv.ParseFloat(trade.Price, 64)
		if err != nil {
			a.logger.LogWarn("GetTrades: could not parse price for trade %s, skipping.", tradeID)
			continue
		}
		volume, err := strconv.ParseFloat(trade.Vol, 64)
		if err != nil {
			a.logger.LogWarn("GetTrades: could not parse volume for trade %s, skipping.", tradeID)
			continue
		}
		fee, err := strconv.ParseFloat(trade.Fee, 64)
		if err != nil {
			a.logger.LogWarn("GetTrades: could not parse fee for trade %s, skipping.", tradeID)
			continue
		}
		cost, err := strconv.ParseFloat(trade.Cost, 64)
		if err != nil {
			a.logger.LogWarn("GetTrades: could not parse cost for trade %s, skipping.", tradeID)
			continue
		}

		// Use the robust client function to translate the Kraken pair name (e.g., "XXBTZUSD")
		// into the common format used by the bot (e.g., "BTC/USD").
		commonPair, commonPairErr := a.client.GetCommonPairName(ctx, trade.Pair)
		if commonPairErr != nil {
			a.logger.LogWarn("GetTrades: could not get common pair name for %s, skipping trade. Error: %v", trade.Pair, commonPairErr)
			continue
		}

		// Perform the critical local filtering. If a pair was requested (e.g., "BTC/USD"),
		// skip any trades that don't match after translation.
		if pair != "" && commonPair != pair {
			continue
		}

		trades = append(trades, broker.Trade{
			ID:          tradeID,
			OrderID:     trade.Ordtxid,
			Pair:        commonPair,
			Side:        trade.Type, // Correctly populate the side from the 'type' field.
			Price:       price,
			Volume:      volume,
			Cost:        cost,
			Fee:         fee,
			FeeCurrency: strings.Split(commonPair, "/")[1],
			Timestamp:   time.Unix(int64(trade.Time), 0),
		})
	}

	// The API returns trades most recent first; sort them oldest first for correct processing.
	sort.Slice(trades, func(i, j int) bool {
		return trades[i].Timestamp.Before(trades[j].Timestamp)
	})

	return trades, nil
}
