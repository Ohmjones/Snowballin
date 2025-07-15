package kraken

import (
	"Snowballin/pkg/broker"
	"Snowballin/utilities"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type PairDetail struct {
	PairDecimals int
	LotDecimals  int
	OrderMin     string
}

type Client struct {
	BaseURL        string
	APIKey         string
	APISecret      string
	HTTPClient     *http.Client
	limiter        *rate.Limiter
	logger         *utilities.Logger
	nonceGenerator *utilities.KrakenNonceGenerator
	cfg            *utilities.KrakenConfig
	dataMu         sync.RWMutex

	assetInfoMap        map[string]AssetInfo
	commonToKrakenAsset map[string]string

	pairInfoMap           map[string]AssetPairAPIInfo
	pairDetailsCache      map[string]PairDetail
	commonToTradeablePair map[string]string
	commonToPrimaryPair   map[string]string
	commonToKrakenPair    map[string]string
	krakenToCommonPair    map[string]string
}

func getCommonAssetName(info AssetInfo) string {
	altname := info.Altname
	if altname == "XBT" {
		return "BTC"
	}
	return strings.Split(altname, ".")[0]
}

func (c *Client) RefreshAssets(ctx context.Context) error {
	c.logger.LogInfo("Kraken Client: Refreshing assets info...")
	var resp struct {
		Error  []string             `json:"error"`
		Result map[string]AssetInfo `json:"result"`
	}
	if err := c.callPublic(ctx, "/0/public/Assets", nil, &resp); err != nil {
		return fmt.Errorf("kraken: RefreshAssets API call failed: %w", err)
	}
	if len(resp.Error) > 0 {
		return fmt.Errorf("kraken: Assets API error: %s", strings.Join(resp.Error, ", "))
	}

	c.dataMu.Lock()
	defer c.dataMu.Unlock()

	c.assetInfoMap = make(map[string]AssetInfo)
	c.commonToKrakenAsset = make(map[string]string)

	for krakenName, info := range resp.Result {
		c.assetInfoMap[krakenName] = info
	}

	for krakenName, info := range resp.Result {
		commonName := getCommonAssetName(info)
		if commonName != "" {
			c.commonToKrakenAsset[strings.ToUpper(commonName)] = krakenName
			if info.Altname != "" && info.Altname != krakenName {
				c.assetInfoMap[info.Altname] = info
			}
		}
	}

	c.logger.LogInfo("Kraken Client: Refreshed and mapped %d assets with all aliases.", len(resp.Result))
	return nil
}

func (c *Client) RefreshAssetPairs(ctx context.Context) error {
	c.logger.LogInfo("Kraken Client: Refreshing asset pairs info...")
	var resp struct {
		Error  []string                    `json:"error"`
		Result map[string]AssetPairAPIInfo `json:"result"`
	}
	if err := c.callPublic(ctx, "/0/public/AssetPairs", nil, &resp); err != nil {
		return err
	}
	if len(resp.Error) > 0 {
		return errors.New(strings.Join(resp.Error, ", "))
	}

	c.dataMu.Lock()
	defer c.dataMu.Unlock()

	if len(c.assetInfoMap) == 0 {
		return errors.New("asset map not initialized, call RefreshAssets first")
	}

	c.pairInfoMap = resp.Result
	c.commonToTradeablePair = make(map[string]string)
	c.commonToPrimaryPair = make(map[string]string)
	c.krakenToCommonPair = make(map[string]string)
	c.pairDetailsCache = make(map[string]PairDetail)
	c.commonToKrakenPair = make(map[string]string)

	for primaryPairName, pairInfo := range resp.Result {
		baseInfo, baseOk := c.assetInfoMap[pairInfo.Base]
		quoteInfo, quoteOk := c.assetInfoMap[pairInfo.Quote]
		if !baseOk || !quoteOk {
			continue
		}

		commonBase := getCommonAssetName(baseInfo)
		commonQuote := getCommonAssetName(quoteInfo)
		if commonBase == "" || commonQuote == "" {
			continue
		}

		commonPairKey := fmt.Sprintf("%s/%s", commonBase, commonQuote)
		tradeablePairName := pairInfo.Altname
		if tradeablePairName == "" {
			tradeablePairName = primaryPairName
		}

		c.commonToPrimaryPair[commonPairKey] = primaryPairName
		c.commonToTradeablePair[commonPairKey] = tradeablePairName
		c.commonToKrakenPair[commonPairKey] = primaryPairName
		c.krakenToCommonPair[primaryPairName] = commonPairKey
		if tradeablePairName != primaryPairName {
			c.krakenToCommonPair[tradeablePairName] = commonPairKey
		}

		c.pairDetailsCache[tradeablePairName] = PairDetail{
			PairDecimals: pairInfo.PairDecimals,
			LotDecimals:  pairInfo.LotDecimals,
			OrderMin:     pairInfo.OrderMin,
		}
	}
	c.logger.LogInfo("Kraken Client: Refreshed %d asset pairs and built comprehensive translation maps.", len(c.pairInfoMap))
	return nil
}

func (c *Client) ParseOHLCV(kBar []interface{}) (utilities.OHLCVBar, error) {
	if len(kBar) < 7 {
		return utilities.OHLCVBar{}, errors.New("invalid OHLCV bar format")
	}
	ts, errTs := utilities.ParseFloatFromInterface(kBar[0])
	open, errO := utilities.ParseFloatFromInterface(kBar[1])
	high, errH := utilities.ParseFloatFromInterface(kBar[2])
	low, errL := utilities.ParseFloatFromInterface(kBar[3])
	closeVal, errC := utilities.ParseFloatFromInterface(kBar[4])
	volume, errV := utilities.ParseFloatFromInterface(kBar[6])
	if errTs != nil || errO != nil || errH != nil || errL != nil || errC != nil || errV != nil {
		return utilities.OHLCVBar{}, fmt.Errorf("error parsing OHLCV values: ts=%v, o=%v, h=%v, l=%v, c=%v, v=%v", errTs, errO, errH, errL, errC, errV)
	}
	return utilities.OHLCVBar{
		Timestamp: int64(ts) * 1000, // Convert to milliseconds
		Open:      open, High: high, Low: low, Close: closeVal, Volume: volume,
	}, nil
}

func (c *Client) ParseOrderBook(rawOrderBook KrakenAPIRawOrderBook, commonPair string) (broker.OrderBookData, error) {
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

func (c *Client) ParseTicker(tickerInfo TickerInfo, pair string) broker.TickerData {
	lastPrice, _ := strconv.ParseFloat(tickerInfo.LastTradeClosed[0], 64)
	highPrice, _ := strconv.ParseFloat(tickerInfo.High[1], 64)
	lowPrice, _ := strconv.ParseFloat(tickerInfo.Low[1], 64)
	volume, _ := strconv.ParseFloat(tickerInfo.Volume[1], 64)
	bidPrice, _ := strconv.ParseFloat(tickerInfo.Bid[0], 64)
	askPrice, _ := strconv.ParseFloat(tickerInfo.Ask[0], 64)

	return broker.TickerData{
		Pair:      pair,
		LastPrice: lastPrice,
		High:      highPrice,
		Low:       lowPrice,
		Volume:    volume,
		Bid:       bidPrice,
		Ask:       askPrice,
		Timestamp: time.Now(),
	}
}

func (c *Client) ParseOrder(orderID string, krakenOrder KrakenOrderInfo, commonPair string) broker.Order {
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
		Pair:          commonPair,
		Side:          krakenOrder.Descr.Type,
		Type:          krakenOrder.Descr.OrderType,
		Status:        krakenOrder.Status,
		Price:         price,
		RequestedVol:  volume,
		ExecutedVol:   filledVolume,
		AvgFillPrice:  avgFillPrice,
		Cost:          cost,
		Fee:           fee,
		TimePlaced:    openTime,
		TimeCompleted: closeTime,
		Reason:        krakenOrder.Reason,
	}
}

func (c *Client) GetAssetInfo(ctx context.Context, krakenAssetName string) (AssetInfo, error) {
	c.dataMu.RLock()
	info, ok := c.assetInfoMap[krakenAssetName]
	c.dataMu.RUnlock()
	if ok {
		return info, nil
	}

	if err := c.RefreshAssets(ctx); err != nil {
		return AssetInfo{}, err
	}

	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	info, ok = c.assetInfoMap[krakenAssetName]
	if !ok {
		return AssetInfo{}, fmt.Errorf("asset info for %s not found after refresh", krakenAssetName)
	}
	return info, nil
}

func (c *Client) GetKrakenAssetName(ctx context.Context, commonAssetName string) (string, error) {
	c.dataMu.RLock()
	krakenName, ok := c.commonToKrakenAsset[strings.ToUpper(commonAssetName)]
	c.dataMu.RUnlock()

	if ok {
		return krakenName, nil
	}

	c.logger.LogWarn("Kraken asset for '%s' not found, refreshing...", commonAssetName)
	if err := c.RefreshAssets(ctx); err != nil {
		return "", err
	}

	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	krakenName, ok = c.commonToKrakenAsset[strings.ToUpper(commonAssetName)]
	if !ok {
		return "", fmt.Errorf("kraken asset name for '%s' not found after refresh", commonAssetName)
	}
	return krakenName, nil
}

func (c *Client) GetPrimaryKrakenPairName(ctx context.Context, commonPair string) (string, error) {
	c.dataMu.RLock()
	krakenPair, ok := c.commonToPrimaryPair[commonPair]
	c.dataMu.RUnlock()

	if ok {
		return krakenPair, nil
	}

	c.logger.LogWarn("Kraken primary pair for '%s' not found, refreshing assets and pairs...", commonPair)
	if err := c.RefreshAssets(ctx); err != nil {
		return "", fmt.Errorf("failed to refresh assets while looking for pair %s: %w", commonPair, err)
	}
	if err := c.RefreshAssetPairs(ctx); err != nil {
		return "", fmt.Errorf("failed to refresh pairs while looking for pair %s: %w", commonPair, err)
	}

	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	krakenPair, ok = c.commonToPrimaryPair[commonPair]
	if !ok {
		return "", fmt.Errorf("primary pair %s not found after refresh", commonPair)
	}
	return krakenPair, nil
}

func (c *Client) GetTradeableKrakenPairName(ctx context.Context, commonPair string) (string, error) {
	c.dataMu.RLock()
	pair, ok := c.commonToTradeablePair[commonPair]
	c.dataMu.RUnlock()
	if ok {
		return pair, nil
	}

	c.logger.LogWarn("Kraken tradeable pair for '%s' not found, refreshing assets and pairs...", commonPair)
	if err := c.RefreshAssets(ctx); err != nil {
		return "", err
	}
	if err := c.RefreshAssetPairs(ctx); err != nil {
		return "", err
	}

	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	pair, ok = c.commonToTradeablePair[commonPair]
	if !ok {
		return "", fmt.Errorf("tradeable pair for %s not found after refresh", commonPair)
	}
	return pair, nil
}

func NewClient(appCfg *utilities.KrakenConfig, HTTPClient *http.Client, logger *utilities.Logger) *Client {
	return &Client{
		BaseURL:               appCfg.BaseURL,
		APIKey:                appCfg.APIKey,
		APISecret:             appCfg.APISecret,
		HTTPClient:            &http.Client{Timeout: time.Duration(appCfg.RequestTimeoutSec) * time.Second},
		limiter:               rate.NewLimiter(rate.Limit(1), 3),
		logger:                logger,
		nonceGenerator:        utilities.NewNonceCounter(),
		cfg:                   appCfg,
		assetInfoMap:          make(map[string]AssetInfo),
		commonToKrakenAsset:   make(map[string]string),
		pairInfoMap:           make(map[string]AssetPairAPIInfo),
		commonToKrakenPair:    make(map[string]string),
		krakenToCommonPair:    make(map[string]string),
		commonToPrimaryPair:   make(map[string]string),
		commonToTradeablePair: make(map[string]string),
		pairDetailsCache:      make(map[string]PairDetail),
	}
}

func (c *Client) GetCommonPairName(ctx context.Context, krakenPair string) (string, error) {
	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	commonPair, ok := c.krakenToCommonPair[krakenPair]
	if !ok {
		return "", fmt.Errorf("common pair name for kraken pair %s not found in map", krakenPair)
	}
	return commonPair, nil
}

func (c *Client) GetCommonAssetName(ctx context.Context, krakenAssetName string) (string, error) {
	sanitizedName := strings.Split(krakenAssetName, ".")[0]

	c.dataMu.RLock()
	assetInfo, ok := c.assetInfoMap[sanitizedName]
	if !ok {
		var found bool
		for _, info := range c.assetInfoMap {
			if info.Altname == sanitizedName {
				assetInfo = info
				found = true
				break
			}
		}
		if !found {
			c.dataMu.RUnlock()
			c.logger.LogWarn("Common name for Kraken asset '%s' (sanitized to '%s') not found, refreshing...", krakenAssetName, sanitizedName)
			if err := c.RefreshAssets(ctx); err != nil {
				return "", fmt.Errorf("failed to refresh assets while getting common name for %s: %w", krakenAssetName, err)
			}
			c.dataMu.RLock()
			var refreshedFound bool
			assetInfo, refreshedFound = c.assetInfoMap[sanitizedName]
			if !refreshedFound {
				for _, info := range c.assetInfoMap {
					if info.Altname == sanitizedName {
						assetInfo = info
						refreshedFound = true
						break
					}
				}
			}
			if !refreshedFound {
				c.dataMu.RUnlock()
				return "", fmt.Errorf("common asset name for Kraken asset %s not found even after refresh", krakenAssetName)
			}
		}
	}
	c.dataMu.RUnlock()

	return getCommonAssetName(assetInfo), nil
}

func (c *Client) GetOrderBookAPI(ctx context.Context, krakenPairName string, depth int) (KrakenAPIRawOrderBook, error) {
	params := url.Values{
		"pair":  {krakenPairName},
		"count": {strconv.Itoa(depth)},
	}
	var resp struct {
		Error  []string                         `json:"error"`
		Result map[string]KrakenAPIRawOrderBook `json:"result"`
	}
	if err := c.callPublic(ctx, "/0/public/Depth", params, &resp); err != nil {
		return KrakenAPIRawOrderBook{}, err
	}
	if len(resp.Error) > 0 {
		return KrakenAPIRawOrderBook{}, errors.New(strings.Join(resp.Error, ", "))
	}
	orderBook, ok := resp.Result[krakenPairName]
	if !ok {
		return KrakenAPIRawOrderBook{}, fmt.Errorf("order book data for pair %s not found in Kraken response", krakenPairName)
	}
	return orderBook, nil
}

func (c *Client) GetOHLCVAPI(ctx context.Context, krakenPairName string, intervalMinutes string, sinceTimestamp int64, countBars int) ([][]interface{}, error) {
	params := url.Values{
		"pair":     {krakenPairName},
		"interval": {intervalMinutes},
	}
	if sinceTimestamp > 0 {
		params.Set("since", strconv.FormatInt(sinceTimestamp, 10))
	}

	var resp struct {
		Error  []string               `json:"error"`
		Result map[string]interface{} `json:"result"`
	}

	err := c.callPublic(ctx, "/0/public/OHLC", params, &resp)
	if err != nil {
		return nil, err
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("kraken OHLC API error: %s", strings.Join(resp.Error, ", "))
	}

	pairData, ok := resp.Result[krakenPairName]
	if !ok {
		return [][]interface{}{}, nil
	}

	ohlcvSlice, ok := pairData.([][]interface{})
	if !ok {
		rawPairData, _ := json.Marshal(pairData)
		if err := json.Unmarshal(rawPairData, &ohlcvSlice); err != nil {
			return nil, fmt.Errorf("unexpected type for ohlcv data for pair %s: %T", krakenPairName, pairData)
		}
	}

	if sinceTimestamp == 0 && countBars > 0 && len(ohlcvSlice) > countBars {
		ohlcvSlice = ohlcvSlice[len(ohlcvSlice)-countBars:]
	}

	return ohlcvSlice, nil
}

func (c *Client) GetPairDetail(ctx context.Context, krakenPair string) (PairDetail, error) {
	c.dataMu.RLock()
	detail, ok := c.pairDetailsCache[krakenPair]
	c.dataMu.RUnlock()

	if ok {
		return detail, nil
	}

	if err := c.RefreshAssetPairs(ctx); err != nil {
		return PairDetail{}, err
	}

	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	detail, ok = c.pairDetailsCache[krakenPair]
	if !ok {
		return PairDetail{}, fmt.Errorf("pair detail %s not found after refresh", krakenPair)
	}
	return detail, nil
}

func (c *Client) AddOrderAPI(ctx context.Context, params url.Values) (string, error) {
	var resp struct {
		Error  []string `json:"error"`
		Result struct {
			Txid []string `json:"txid"`
		} `json:"result"`
	}
	if err := c.callPrivate(ctx, "/0/private/AddOrder", params, &resp); err != nil {
		return "", err
	}
	if len(resp.Error) > 0 {
		return "", errors.New(strings.Join(resp.Error, ", "))
	}
	if len(resp.Result.Txid) == 0 {
		return "", errors.New("Kraken AddOrder returned no transaction ID")
	}
	return resp.Result.Txid[0], nil
}

func (c *Client) CancelOrderAPI(ctx context.Context, orderID string) error {
	params := url.Values{"txid": {orderID}}
	var resp struct {
		Error []string `json:"error"`
	}
	if err := c.callPrivate(ctx, "/0/private/CancelOrder", params, &resp); err != nil {
		return err
	}
	if len(resp.Error) > 0 {
		return errors.New(strings.Join(resp.Error, ", "))
	}
	return nil
}

func (c *Client) QueryOrdersAPI(ctx context.Context, txids string) (map[string]KrakenOrderInfo, error) {
	params := url.Values{"txid": {txids}, "trades": {"true"}}
	var resp struct {
		Error  []string                   `json:"error"`
		Result map[string]KrakenOrderInfo `json:"result"`
	}
	if err := c.callPrivate(ctx, "/0/private/QueryOrders", params, &resp); err != nil {
		return nil, err
	}
	if len(resp.Error) > 0 {
		return nil, errors.New(strings.Join(resp.Error, ", "))
	}
	return resp.Result, nil
}

func (c *Client) GetBalancesAPI(ctx context.Context) (map[string]string, error) {
	var resp struct {
		Error  []string          `json:"error"`
		Result map[string]string `json:"result"`
	}
	if err := c.callPrivate(ctx, "/0/private/Balance", nil, &resp); err != nil {
		return nil, err
	}
	if len(resp.Error) > 0 {
		return nil, errors.New(strings.Join(resp.Error, ", "))
	}
	return resp.Result, nil
}

func (c *Client) GetTickerAPI(ctx context.Context, krakenPairName string) (TickerInfo, error) {
	var resp struct {
		Error  []string              `json:"error"`
		Result map[string]TickerInfo `json:"result"`
	}
	params := url.Values{"pair": {krakenPairName}}
	if err := c.callPublic(ctx, "/0/public/Ticker", params, &resp); err != nil {
		return TickerInfo{}, err
	}
	if len(resp.Error) > 0 {
		return TickerInfo{}, errors.New(strings.Join(resp.Error, ", "))
	}
	ticker, ok := resp.Result[krakenPairName]
	if !ok {
		return TickerInfo{}, errors.New("Kraken GetTicker pair not found")
	}
	return ticker, nil
}

func (c *Client) QueryTradesAPI(ctx context.Context, params url.Values) (map[string]KrakenTradeInfo, int64, error) {
	var resp struct {
		Error  []string `json:"error"`
		Result struct {
			Trades map[string]KrakenTradeInfo `json:"trades"`
			Last   string                     `json:"last"`
		} `json:"result"`
	}
	if err := c.callPrivate(ctx, "/0/private/TradesHistory", params, &resp); err != nil {
		return nil, 0, err
	}
	if len(resp.Error) > 0 {
		return nil, 0, errors.New(strings.Join(resp.Error, ", "))
	}
	last, _ := strconv.ParseInt(resp.Result.Last, 10, 64)
	return resp.Result.Trades, last, nil
}

func (c *Client) callPrivate(ctx context.Context, apiPath string, data url.Values, target interface{}) error {
	if c.APIKey == "" || c.APISecret == "" {
		return errors.New("kraken: API key or secret not configured")
	}
	nonce := c.nonceGenerator.Nonce()
	nonceStr := strconv.FormatUint(nonce, 10)
	if data == nil {
		data = url.Values{}
	}
	data.Set("nonce", nonceStr)

	authHeaders, err := utilities.GenerateKrakenAuthHeaders(c.APIKey, c.APISecret, apiPath, nonceStr, data)
	if err != nil {
		return fmt.Errorf("kraken: generate auth headers for %s: %w", apiPath, err)
	}

	fullURL := c.BaseURL + apiPath
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("kraken: create private request for %s: %w", apiPath, err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "SnowballinBot/1.0")
	for key, val := range authHeaders {
		req.Header.Set(key, val)
	}
	c.logger.LogDebug("Kraken callPrivate: URL=%s, Nonce=%s", fullURL, nonceStr)

	return utilities.DoJSONRequest(c.HTTPClient, req, 2, 2*time.Second, target)
}

func (c *Client) callPublic(ctx context.Context, path string, params url.Values, target interface{}) error {
	endpoint := c.BaseURL + path
	if params != nil && len(params) > 0 {
		endpoint += "?" + params.Encode()
	}
	c.logger.LogDebug("Kraken callPublic: URL=%s", endpoint)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("kraken: create public request for %s: %w", endpoint, err)
	}
	req.Header.Set("User-Agent", "SnowballinBot/1.0")

	return utilities.DoJSONRequest(c.HTTPClient, req, 2, 2*time.Second, target)
}

func (c *Client) QueryTradeVolumeAPI(ctx context.Context, params url.Values) (*TradeVolumeResult, error) {
	var resp TradeVolumeResponse
	if err := c.callPrivate(ctx, "/0/private/TradeVolume", params, &resp); err != nil {
		return nil, fmt.Errorf("QueryTradeVolumeAPI: callPrivate failed: %w", err)
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("QueryTradeVolumeAPI: API error: %s", strings.Join(resp.Error, ", "))
	}
	return &resp.Result, nil
}
