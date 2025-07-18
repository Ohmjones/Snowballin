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
	// Check cache first
	cachedTrades, fresh, cacheErr := a.cache.GetCachedTrades(pair)
	if cacheErr == nil && fresh && len(cachedTrades) > 0 {
		a.logger.LogDebug("GetTrades [%s]: Cache hit (fresh). Returning %d trades.", pair, len(cachedTrades))
		// Filter by since if needed (cache has all, but we can filter client-side)
		if !since.IsZero() {
			var filtered []broker.Trade
			for _, t := range cachedTrades {
				if t.Timestamp.After(since) {
					filtered = append(filtered, t)
				}
			}
			return filtered, nil
		}
		return cachedTrades, nil
	}

	// Cache miss or stale: Fetch from API
	var allTrades []broker.Trade
	params := url.Values{}
	if !since.IsZero() {
		params.Set("start", strconv.FormatInt(since.UnixNano()/1e9, 10))
	}

	krakenPair := ""
	if pair != "" {
		var err error
		krakenPair, err = a.client.GetPrimaryKrakenPairName(ctx, pair)
		if err != nil {
			return nil, fmt.Errorf("GetTrades: could not resolve pair name for %s: %w", pair, err)
		}
		params.Set("pair", krakenPair) // Let server filter for specific pair
	} // If pair == "", omit 'pair' param to fetch all trades

	ofs := int64(0)
	maxRetries := 3
	backoff := time.Second

	for {
		params.Set("ofs", strconv.FormatInt(ofs, 10))

		var tradesMap map[string]KrakenTradeInfo
		var lastErr error
		for attempt := 0; attempt < maxRetries; attempt++ {
			tradesMap, _, lastErr = a.client.QueryTradesAPI(ctx, params)
			if lastErr == nil {
				break
			}
			if strings.Contains(lastErr.Error(), "EAPI:Rate limit exceeded") {
				a.logger.LogWarn("GetTrades [%s]: Rate limit hit. Waiting %v before retry %d/%d", pair, backoff, attempt+1, maxRetries)
				time.Sleep(backoff)
				backoff *= 2
			} else {
				return nil, lastErr
			}
		}
		if lastErr != nil {
			return nil, fmt.Errorf("GetTrades [%s]: Failed after retries: %w", pair, lastErr)
		}

		var pageTrades []broker.Trade
		for tradeID, trade := range tradesMap {
			if krakenPair != "" && trade.Pair != krakenPair {
				continue
			}
			commonPair := pair
			if commonPair == "" {
				var commonPairErr error
				commonPair, commonPairErr = a.client.GetCommonPairName(ctx, trade.Pair)
				if commonPairErr != nil {
					a.logger.LogWarn("GetTrades: could not get common pair name for %s, skipping trade. Error: %v", trade.Pair, commonPairErr)
					continue
				}
			}

			price, _ := strconv.ParseFloat(trade.Price, 64)
			volume, _ := strconv.ParseFloat(trade.Vol, 64)
			fee, _ := strconv.ParseFloat(trade.Fee, 64)
			cost, _ := strconv.ParseFloat(trade.Cost, 64)

			pageTrades = append(pageTrades, broker.Trade{
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

		allTrades = append(allTrades, pageTrades...)

		if len(tradesMap) < 50 {
			break
		}
		ofs += 50
		time.Sleep(time.Second)
		backoff = time.Second
	}

	sort.Slice(allTrades, func(i, j int) bool {
		return allTrades[i].Timestamp.Before(allTrades[j].Timestamp)
	})

	// Save to cache only if specific pair (skip for all-pairs fetch)
	if pair != "" {
		if saveErr := a.cache.SaveTrades(pair, allTrades); saveErr != nil {
			a.logger.LogWarn("GetTrades [%s]: Failed to cache trades: %v", pair, saveErr)
		} else {
			a.logger.LogDebug("GetTrades [%s]: Cached %d trades.", pair, len(allTrades))
		}
	}

	return allTrades, nil
}

func (a *Adapter) GetAccountValue(ctx context.Context, quoteCurrency string) (float64, error) {
	a.logger.LogInfo("Fetching account balances from Kraken to calculate total portfolio value in %s...", quoteCurrency)
	balances, err := a.client.GetBalancesAPI(ctx)
	if err != nil {
		return 0, fmt.Errorf("GetAccountValue: failed to get balances: %w", err)
	}

	totalValue := 0.0
	quoteCurrencyUpper := strings.ToUpper(quoteCurrency)
	pairsToFetch := make(map[string]string) // Map common pair to kraken pair
	assetBalances := make(map[string]float64)

	// 1. Go through balances, calculate value of quote currency, and build a list of other assets.
	for originalKey, balanceStr := range balances {
		balance, err := strconv.ParseFloat(balanceStr, 64)
		if err != nil || balance <= 1e-8 {
			continue
		}

		commonName, err := a.client.GetCommonAssetName(ctx, originalKey)
		if err != nil {
			a.logger.LogWarn("GetAccountValue: could not get common name for %s, skipping.", originalKey)
			continue
		}

		if strings.EqualFold(commonName, quoteCurrencyUpper) {
			totalValue += balance
		} else {
			assetBalances[commonName] = balance
			commonPair := fmt.Sprintf("%s/%s", commonName, quoteCurrencyUpper)
			if _, exists := pairsToFetch[commonPair]; !exists {
				krakenPair, err := a.client.GetPrimaryKrakenPairName(ctx, commonPair)
				if err == nil {
					pairsToFetch[commonPair] = krakenPair
				}
			}
		}
	}

	if len(pairsToFetch) == 0 {
		return totalValue, nil
	}

	// 2. Make ONE API call to get all tickers for the assets we hold.
	krakenPairList := make([]string, 0, len(pairsToFetch))
	for _, krakenPair := range pairsToFetch {
		krakenPairList = append(krakenPairList, krakenPair)
	}
	a.logger.LogInfo("GetAccountValue: Fetching batch tickers for %d assets...", len(krakenPairList))

	tickerInfoMap, err := a.client.GetTickerAPI(ctx, strings.Join(krakenPairList, ","))
	if err != nil {
		return 0, fmt.Errorf("GetAccountValue: failed to get batch tickers: %w", err)
	}

	krakenToCommonMap := make(map[string]string)
	for common, kraken := range pairsToFetch {
		krakenToCommonMap[kraken] = common
	}

	// 3. Create a simple price map for easy lookup.
	priceMap := make(map[string]float64) // commonPair -> price
	for krakenPair, tickerInfo := range tickerInfoMap {
		if commonPair, ok := krakenToCommonMap[krakenPair]; ok {
			price, _ := strconv.ParseFloat(tickerInfo.LastTradeClosed[0], 64)
			priceMap[commonPair] = price
		}
	}

	// 4. Iterate through our held assets and add their value using the fetched prices.
	for assetName, balance := range assetBalances {
		commonPair := fmt.Sprintf("%s/%s", assetName, quoteCurrencyUpper)
		price, ok := priceMap[commonPair]
		if ok && price > 0 {
			assetValueInQuote := balance * price
			totalValue += assetValueInQuote
		} else {
			a.logger.LogWarn("GetAccountValue: Could not find price for %s in batch ticker response.", assetName)
		}
	}

	a.logger.LogInfo("Calculated total account value: %.2f %s", totalValue, quoteCurrencyUpper)
	return totalValue, nil
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

	// Check cache first
	cachedTicker, fresh, cacheErr := a.cache.GetCachedTicker(krakenPair)
	if cacheErr == nil && fresh {
		a.logger.LogDebug("GetTicker [%s]: Cache hit (fresh).", pair)
		return cachedTicker, nil
	}

	tickerInfoMap, err := a.client.GetTickerAPI(ctx, krakenPair)
	if err != nil {
		return broker.TickerData{}, err
	}

	singleTickerInfo, ok := tickerInfoMap[krakenPair]
	if !ok {
		return broker.TickerData{}, fmt.Errorf("ticker for pair %s (%s) not found in API response", pair, krakenPair)
	}

	parsedTicker := a.client.ParseTicker(singleTickerInfo, pair)

	// Save to cache
	if saveErr := a.cache.SaveTicker(krakenPair, parsedTicker); saveErr != nil {
		a.logger.LogWarn("GetTicker [%s]: Failed to cache ticker: %v", pair, saveErr)
	}

	return parsedTicker, nil
}

// GetTradeFees fetches the user's current maker and taker fee percentages for a given asset pair.
// It queries the TradeVolume endpoint and returns the fee for the current tier as a decimal (e.g., 0.0016 for 0.16%).
func (a *Adapter) GetTradeFees(ctx context.Context, commonPair string) (makerFee float64, takerFee float64, err error) {
	krakenPair, err := a.client.GetPrimaryKrakenPairName(ctx, commonPair)
	if err != nil {
		return 0, 0, fmt.Errorf("GetTradeFees: failed to get Kraken pair name for %s: %w", commonPair, err)
	}

	// Check cache first
	makerFee, takerFee, fresh, cacheErr := a.cache.GetCachedFees(krakenPair)
	if cacheErr == nil && fresh {
		a.logger.LogDebug("GetTradeFees [%s]: Cache hit (fresh). Maker: %.4f, Taker: %.4f", commonPair, makerFee, takerFee)
		return makerFee, takerFee, nil
	}

	params := url.Values{
		"pair":     {krakenPair},
		"fee-info": {"true"},
	}

	tradeVolumeResult, apiErr := a.client.QueryTradeVolumeAPI(ctx, params)
	if apiErr != nil {
		return 0, 0, fmt.Errorf("GetTradeFees: client API call failed: %w", apiErr)
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
	if saveErr := a.cache.SaveFees(krakenPair, makerFee, takerFee); saveErr != nil {
		a.logger.LogWarn("GetTradeFees [%s]: Failed to cache fees: %v", commonPair, saveErr)
	}

	return makerFee, takerFee, nil
}
