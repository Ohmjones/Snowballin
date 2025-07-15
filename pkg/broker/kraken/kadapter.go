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

// --- [THE FIX] This new function gets all balances and is used by the new reconciliation logic. ---
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

func intervalToDuration(interval string) (time.Duration, error) {
	minutes, err := strconv.Atoi(interval)
	if err != nil {
		return 0, fmt.Errorf("invalid interval format: %s", interval)
	}
	return time.Duration(minutes) * time.Minute, nil
}

func (a *Adapter) GetLastNOHLCVBars(ctx context.Context, pair string, intervalMinutes string, nBars int) ([]utilities.OHLCVBar, error) {
	const cacheProvider = "kraken"
	cacheKey := fmt.Sprintf("%s-%s", strings.ReplaceAll(pair, "/", "-"), intervalMinutes)

	interval, err := intervalToDuration(intervalMinutes)
	if err != nil {
		return nil, fmt.Errorf("could not determine lookback for cache: %w", err)
	}
	lookbackDuration := time.Duration(nBars+5) * interval
	startTime := time.Now().Add(-lookbackDuration)

	cachedBars, err := a.cache.GetBars(cacheProvider, cacheKey, startTime.UnixMilli(), time.Now().UnixMilli())
	if err == nil && len(cachedBars) >= nBars {
		if time.Since(time.UnixMilli(cachedBars[len(cachedBars)-1].Timestamp)) < (2 * interval) {
			a.logger.LogInfo("kadapter GetLastNOHLCVBars [%s]: Cache hit. Returning %d bars.", pair, nBars)
			if len(cachedBars) > nBars {
				return cachedBars[len(cachedBars)-nBars:], nil
			}
			return cachedBars, nil
		}
	}

	a.logger.LogInfo("kadapter GetLastNOHLCVBars [%s]: Cache miss or insufficient data. Fetching from API.", pair)
	krakenPair, err := a.client.GetPrimaryKrakenPairName(ctx, pair)
	if err != nil {
		return nil, fmt.Errorf("GetLastNOHLCVBars: failed to get Kraken pair name for %s: %w", pair, err)
	}

	sinceTimestamp := time.Now().Add(-lookbackDuration).Unix()
	krakenOHLCV, err := a.client.GetOHLCVAPI(ctx, krakenPair, intervalMinutes, sinceTimestamp, 0)
	if err != nil {
		return nil, fmt.Errorf("GetLastNOHLCVBars: client call failed for %s: %w", pair, err)
	}

	if len(krakenOHLCV) == 0 {
		return nil, fmt.Errorf("no OHLCV data returned from API for %s", pair)
	}

	var fetchedBars []utilities.OHLCVBar
	for _, kBar := range krakenOHLCV {
		bar, parseErr := a.client.ParseOHLCV(kBar)
		if parseErr != nil {
			a.logger.LogWarn("kadapter GetLastNOHLCVBars [%s]: Failed to parse a bar, skipping: %v", pair, parseErr)
			continue
		}
		fetchedBars = append(fetchedBars, bar)
	}

	for _, bar := range fetchedBars {
		if err := a.cache.SaveBar(cacheProvider, cacheKey, bar); err != nil {
			a.logger.LogWarn("kadapter GetLastNOHLCVBars [%s]: Failed to save new bar to cache: %v", pair, err)
		}
	}
	a.logger.LogInfo("kadapter GetLastNOHLCVBars [%s]: Fetched and cached %d new bar(s).", pair, len(fetchedBars))

	if len(fetchedBars) < nBars {
		a.logger.LogWarn("kadapter GetLastNOHLCVBars [%s]: API returned fewer bars (%d) than requested (%d).", pair, len(fetchedBars), nBars)
		return fetchedBars, nil
	}

	return fetchedBars[len(fetchedBars)-nBars:], nil
}

func (a *Adapter) GetTrades(ctx context.Context, pair string, since time.Time) ([]broker.Trade, error) {
	params := url.Values{}
	if !since.IsZero() {
		params.Set("start", strconv.FormatInt(since.Unix(), 10))
	}

	if pair != "" {
		krakenPair, err := a.client.GetPrimaryKrakenPairName(ctx, pair)
		if err != nil {
			return nil, fmt.Errorf("GetTrades: could not resolve pair name for %s: %w", pair, err)
		}
		params.Set("pair", krakenPair)
	}

	tradesMap, _, err := a.client.QueryTradesAPI(ctx, params)
	if err != nil {
		return nil, err
	}

	var trades []broker.Trade
	for tradeID, trade := range tradesMap {
		commonPair, commonPairErr := a.client.GetCommonPairName(ctx, trade.Pair)
		if commonPairErr != nil {
			a.logger.LogWarn("GetTrades: could not get common pair name for %s, skipping trade. Error: %v", trade.Pair, commonPairErr)
			continue
		}

		if pair != "" && commonPair != pair {
			continue
		}

		price, _ := strconv.ParseFloat(trade.Price, 64)
		volume, _ := strconv.ParseFloat(trade.Vol, 64)
		fee, _ := strconv.ParseFloat(trade.Fee, 64)
		cost, _ := strconv.ParseFloat(trade.Cost, 64)

		trades = append(trades, broker.Trade{
			ID:          tradeID,
			OrderID:     trade.Ordtxid,
			Pair:        commonPair,
			Side:        trade.Type,
			Price:       price,
			Volume:      volume,
			Cost:        cost,
			Fee:         fee,
			FeeCurrency: strings.Split(commonPair, "/")[1],
			Timestamp:   time.Unix(int64(trade.Time), 0),
		})
	}

	sort.Slice(trades, func(i, j int) bool {
		return trades[i].Timestamp.Before(trades[j].Timestamp)
	})

	return trades, nil
}

func (a *Adapter) GetAccountValue(ctx context.Context, quoteCurrency string) (float64, error) {
	a.logger.LogInfo("Fetching account balances from Kraken to calculate total portfolio value in %s...", quoteCurrency)
	balances, err := a.client.GetBalancesAPI(ctx)
	if err != nil {
		return 0, fmt.Errorf("GetAccountValue: failed to get balances: %w", err)
	}

	totalValue := 0.0
	quoteCurrencyUpper := strings.ToUpper(quoteCurrency)

	for originalKey, balanceStr := range balances {
		balance, err := strconv.ParseFloat(balanceStr, 64)
		if err != nil || balance == 0 {
			continue
		}

		krakenAssetName := strings.TrimSuffix(originalKey, ".F")
		commonName, err := a.client.GetCommonAssetName(ctx, krakenAssetName)
		if err != nil {
			a.logger.LogWarn("GetAccountValue: could not get common name for %s, skipping.", krakenAssetName)
			continue
		}

		if strings.EqualFold(commonName, quoteCurrencyUpper) {
			totalValue += balance
			continue
		}

		priceInQuote, err := a.getAssetPriceInQuote(ctx, commonName, quoteCurrencyUpper)
		if err != nil {
			a.logger.LogWarn("GetAccountValue: %v. Skipping value calculation for %s.", err, commonName)
			continue
		}

		assetValueInQuote := balance * priceInQuote
		totalValue += assetValueInQuote
		a.logger.LogDebug("GetAccountValue: Valued %f of %s at %.2f %s/COIN", balance, commonName, priceInQuote, quoteCurrencyUpper)
	}

	a.logger.LogInfo("Calculated total account value: %.2f %s", totalValue, quoteCurrencyUpper)
	return totalValue, nil
}

func (a *Adapter) getAssetPriceInQuote(ctx context.Context, commonBase, commonQuote string) (float64, error) {
	directPair := fmt.Sprintf("%s/%s", commonBase, commonQuote)
	krakenPair, err := a.client.GetPrimaryKrakenPairName(ctx, directPair)
	if err == nil {
		ticker, tickerErr := a.client.GetTickerAPI(ctx, krakenPair)
		if tickerErr == nil {
			price, pErr := strconv.ParseFloat(ticker.Bid[0], 64)
			if pErr == nil && price > 0 {
				return price, nil
			}
		}
	}

	a.logger.LogDebug("getAssetPriceInQuote: No direct ticker for %s, attempting triangulation via BTC.", directPair)
	pivotPair := "BTC/USD"
	if commonQuote != "USD" {
		pivotPair = fmt.Sprintf("BTC/%s", commonQuote)
	}

	krakenPivotPair, err := a.client.GetPrimaryKrakenPairName(ctx, pivotPair)
	if err != nil {
		return 0, fmt.Errorf("could not resolve pivot pair %s for triangulation", pivotPair)
	}
	pivotTicker, err := a.client.GetTickerAPI(ctx, krakenPivotPair)
	if err != nil {
		return 0, fmt.Errorf("failed to get pivot ticker for %s", krakenPivotPair)
	}
	pivotPrice, _ := strconv.ParseFloat(pivotTicker.Bid[0], 64)
	if pivotPrice <= 0 {
		return 0, fmt.Errorf("pivot asset %s has non-positive price", pivotPair)
	}

	if commonBase == "BTC" {
		return pivotPrice, nil
	}

	triangulationPair := fmt.Sprintf("%s/BTC", commonBase)
	krakenTriangulationPair, err := a.client.GetPrimaryKrakenPairName(ctx, triangulationPair)
	if err != nil {
		return 0, fmt.Errorf("could not resolve triangulation pair %s", triangulationPair)
	}
	triangTicker, err := a.client.GetTickerAPI(ctx, krakenTriangulationPair)
	if err != nil {
		return 0, fmt.Errorf("failed to get ticker for triangulation pair %s", krakenTriangulationPair)
	}
	triangPrice, _ := strconv.ParseFloat(triangTicker.Bid[0], 64)
	if triangPrice <= 0 {
		return 0, fmt.Errorf("triangulation asset %s has non-positive price", triangulationPair)
	}

	return triangPrice * pivotPrice, nil
}

func (a *Adapter) GetOrderBook(ctx context.Context, commonPair string, depth int) (broker.OrderBookData, error) {
	krakenPair, err := a.client.GetPrimaryKrakenPairName(ctx, commonPair)
	if err != nil {
		return broker.OrderBookData{}, fmt.Errorf("GetOrderBook: failed to get Kraken pair name for %s: %w", commonPair, err)
	}

	rawOrderBook, err := a.client.GetOrderBookAPI(ctx, krakenPair, depth)
	if err != nil {
		return broker.OrderBookData{}, err
	}

	return a.client.ParseOrderBook(rawOrderBook, commonPair)
}

func (a *Adapter) PlaceOrder(ctx context.Context, assetPair, side, orderType string, volume, price, stopPrice float64, clientOrderID string) (string, error) {
	tradeableKrakenPair, err := a.client.GetTradeableKrakenPairName(ctx, assetPair)
	if err != nil {
		return "", fmt.Errorf("PlaceOrder: could not resolve tradeable pair name for %s: %w", assetPair, err)
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
		return broker.Order{}, fmt.Errorf("order %s not found in query result", orderID)
	}

	commonPair, err := a.client.GetCommonPairName(ctx, krakenOrder.Descr.Pair)
	if err != nil {
		a.logger.LogWarn("GetOrderStatus: could not resolve common pair for %s. Using original.", krakenOrder.Descr.Pair)
		commonPair = krakenOrder.Descr.Pair
	}

	return a.client.ParseOrder(orderID, krakenOrder, commonPair), nil
}

func (a *Adapter) GetTicker(ctx context.Context, pair string) (broker.TickerData, error) {
	krakenPair, err := a.client.GetPrimaryKrakenPairName(ctx, pair)
	if err != nil {
		return broker.TickerData{}, err
	}

	tickerInfo, err := a.client.GetTickerAPI(ctx, krakenPair)
	if err != nil {
		return broker.TickerData{}, err
	}

	return a.client.ParseTicker(tickerInfo, pair), nil
}

// GetTradeFees fetches the user's current maker and taker fee percentages for a given asset pair.
// It queries the TradeVolume endpoint and returns the fee for the current tier as a decimal (e.g., 0.0016 for 0.16%).
func (a *Adapter) GetTradeFees(ctx context.Context, commonPair string) (makerFee float64, takerFee float64, err error) {
	krakenPair, err := a.client.GetPrimaryKrakenPairName(ctx, commonPair)
	if err != nil {
		return 0, 0, fmt.Errorf("GetTradeFees: failed to get Kraken pair name for %s: %w", commonPair, err)
	}

	params := url.Values{
		"pair":     {krakenPair},
		"fee-info": {"true"},
	}

	// This calls the specific client method that queries the TradeVolume endpoint.
	tradeVolumeResult, err := a.client.QueryTradeVolumeAPI(ctx, params)
	if err != nil {
		return 0, 0, fmt.Errorf("GetTradeFees: client API call failed: %w", err)
	}

	// Correctly parse TAKER fees from the "fees" object.
	if feeInfo, ok := tradeVolumeResult.Fees[krakenPair]; ok {
		parsedFee, parseErr := strconv.ParseFloat(feeInfo.Fee, 64)
		if parseErr != nil {
			return 0, 0, fmt.Errorf("GetTradeFees: could not parse taker fee '%s': %w", feeInfo.Fee, parseErr)
		}
		takerFee = parsedFee / 100.0 // Convert from percentage (e.g., 0.26) to decimal (0.0026)
	} else {
		return 0, 0, fmt.Errorf("GetTradeFees: no taker fee info found for pair %s in API response", krakenPair)
	}

	// Correctly parse MAKER fees from the "fees_maker" object.
	if feeInfo, ok := tradeVolumeResult.FeesMaker[krakenPair]; ok {
		parsedFee, parseErr := strconv.ParseFloat(feeInfo.Fee, 64)
		if parseErr != nil {
			return 0, 0, fmt.Errorf("GetTradeFees: could not parse maker fee '%s': %w", feeInfo.Fee, parseErr)
		}
		makerFee = parsedFee / 100.0 // Convert from percentage (e.g., 0.16) to decimal (0.0016)
	} else {
		// Fallback for older accounts that might not have a separate fees_maker tier.
		a.logger.LogWarn("GetTradeFees: no maker-specific fee info for %s, falling back to using taker fee for both.", krakenPair)
		makerFee = takerFee
	}

	return makerFee, takerFee, nil
}
