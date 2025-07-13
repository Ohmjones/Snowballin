// File: pkg/broker/kraken/kclient.go
package kraken

import (
	"Snowballin/utilities"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"io"
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
	BaseURL            string
	APIKey             string
	APISecret          string
	HTTPClient         *http.Client
	limiter            *rate.Limiter
	logger             *utilities.Logger
	nonceGenerator     *utilities.KrakenNonceGenerator
	cfg                *utilities.KrakenConfig
	dataMu             sync.RWMutex
	assetInfoMap       map[string]AssetInfo
	pairInfoMap        map[string]AssetPairAPIInfo
	pairDetailsCache   map[string]PairDetail
	commonToKrakenPair map[string]string
	// --- [NEW] Added a reverse map to translate Kraken names back to common names ---
	krakenToCommonPair map[string]string
}

func (c *Client) GetCommonPairName(ctx context.Context, krakenPair string) (string, error) {
	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	normalizedPair := normalizeKrakenPair(krakenPair)
	commonPair, ok := c.krakenToCommonPair[normalizedPair]
	if !ok {
		// As a fallback, try the original name
		commonPair, ok = c.krakenToCommonPair[krakenPair]
		if !ok {
			return "", fmt.Errorf("common pair name for kraken pair %s (normalized: %s) not found in map", krakenPair, normalizedPair)
		}
	}
	return commonPair, nil
}

func (c *Client) GetAssetPairAPIInfo(ctx context.Context, krakenPairName string) (AssetPairAPIInfo, error) {
	c.dataMu.RLock()
	info, ok := c.pairInfoMap[krakenPairName]
	c.dataMu.RUnlock()
	if ok {
		return info, nil
	}
	c.logger.LogInfo("AssetPairAPIInfo for %s not found in cache, attempting refresh...", krakenPairName)
	if err := c.RefreshAssetPairs(ctx); err != nil {
		return AssetPairAPIInfo{}, fmt.Errorf("failed to refresh asset pairs while getting info for %s: %w", krakenPairName, err)
	}
	c.dataMu.RLock()
	info, ok = c.pairInfoMap[krakenPairName]
	c.dataMu.RUnlock()
	if !ok {
		return AssetPairAPIInfo{}, fmt.Errorf("asset pair info for %s not found even after refresh", krakenPairName)
	}
	return info, nil
}

func (c *Client) GetCommonAssetName(ctx context.Context, krakenAssetName string) (string, error) {
	c.dataMu.RLock()
	assetInfo, ok := c.assetInfoMap[krakenAssetName]
	c.dataMu.RUnlock()

	if ok && assetInfo.Altname != "" {
		if assetInfo.Altname == "XBT" {
			return "BTC", nil
		}
		return assetInfo.Altname, nil
	}
	if err := c.RefreshAssets(ctx); err != nil {
		return "", fmt.Errorf("failed to refresh assets while getting common name for %s: %w", krakenAssetName, err)
	}
	c.dataMu.RLock()
	assetInfo, ok = c.assetInfoMap[krakenAssetName]
	c.dataMu.RUnlock()
	if !ok || assetInfo.Altname == "" {
		return "", fmt.Errorf("common asset name for Kraken asset %s not found after refresh", krakenAssetName)
	}
	if assetInfo.Altname == "XBT" {
		return "BTC", nil
	}
	return assetInfo.Altname, nil
}

func (c *Client) GetKrakenAssetName(ctx context.Context, commonAssetName string) (string, error) {
	upperCommonName := strings.ToUpper(commonAssetName)
	if upperCommonName == "BTC" {
		upperCommonName = "XBT"
	}

	c.dataMu.RLock()
	for kName, assetInf := range c.assetInfoMap {
		if assetInf.Altname == upperCommonName {
			c.dataMu.RUnlock()
			return kName, nil
		}
	}
	c.dataMu.RUnlock()

	c.logger.LogInfo("Kraken asset name for common name '%s' not found in cache, attempting refresh...", commonAssetName)
	if err := c.RefreshAssets(ctx); err != nil {
		return "", fmt.Errorf("failed to refresh assets while getting Kraken name for %s: %w", commonAssetName, err)
	}

	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	for kName, assetInf := range c.assetInfoMap {
		if assetInf.Altname == upperCommonName {
			return kName, nil
		}
	}
	return "", fmt.Errorf("kraken internal asset name for common name %s not found after refresh", commonAssetName)
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
		return nil, fmt.Errorf("ohlcv data for pair %s not found in response", krakenPairName)
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

func NewClient(appCfg *utilities.KrakenConfig, HTTPClient *http.Client, logger *utilities.Logger) *Client {
	if appCfg == nil {
		panic("Kraken Client requires non-nil KrakenConfig")
	}

	if logger == nil {
		logger = utilities.NewLogger(utilities.Info)
		logger.LogWarn("Kraken.NewClient: Logger fallback used.")
	}

	if HTTPClient == nil {
		HTTPClient = &http.Client{
			Timeout: time.Duration(appCfg.RequestTimeoutSec) * time.Second,
		}
	}

	return &Client{
		BaseURL:            appCfg.BaseURL,
		APIKey:             appCfg.APIKey,
		APISecret:          appCfg.APISecret,
		HTTPClient:         HTTPClient,
		limiter:            rate.NewLimiter(rate.Limit(appCfg.RateLimitPerSec), appCfg.RateBurst),
		logger:             logger,
		nonceGenerator:     utilities.NewNonceCounter(),
		cfg:                appCfg,
		assetInfoMap:       make(map[string]AssetInfo),
		pairInfoMap:        make(map[string]AssetPairAPIInfo),
		pairDetailsCache:   make(map[string]PairDetail),
		commonToKrakenPair: make(map[string]string),
		// --- [NEW] Initialize the new map ---
		krakenToCommonPair: make(map[string]string),
	}
}

func (c *Client) GetKrakenPairName(ctx context.Context, commonPair string) (string, error) {
	c.dataMu.RLock()
	krakenPair, ok := c.commonToKrakenPair[commonPair]
	c.dataMu.RUnlock()

	if ok {
		return krakenPair, nil
	}

	if err := c.RefreshAssetPairs(ctx); err != nil {
		return "", err
	}

	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	krakenPair, ok = c.commonToKrakenPair[commonPair]
	if !ok {
		return "", fmt.Errorf("pair %s not found after refresh", commonPair)
	}
	return krakenPair, nil
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

func (c *Client) callPublicRaw(ctx context.Context, path string, params url.Values) ([]byte, error) {
	endpoint := c.BaseURL + path
	if params != nil && len(params) > 0 {
		endpoint += "?" + params.Encode()
	}
	c.logger.LogDebug("Kraken callPublicRaw: URL=%s", endpoint)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("kraken: create public raw request for %s: %w", endpoint, err)
	}
	req.Header.Set("User-Agent", "SnowballinBot/1.0")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kraken: execute public raw request for %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("kraken: read raw response body for %s: %w", endpoint, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return bodyBytes, fmt.Errorf("kraken: http status %d for %s: %s", resp.StatusCode, endpoint, string(bodyBytes))
	}
	return bodyBytes, nil
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

// --- [MODIFIED] This function now populates the new reverse map for pair name translation. ---
func (c *Client) RefreshAssetPairs(ctx context.Context) error {
	c.logger.LogInfo("Kraken Client: Refreshing asset pairs info...")
	var resp struct {
		Error  []string                    `json:"error"`
		Result map[string]AssetPairAPIInfo `json:"result"`
	}
	err := c.callPublic(ctx, "/0/public/AssetPairs", nil, &resp)
	if err != nil {
		errMsg := "kraken: RefreshAssetPairs API call failed"
		if len(resp.Error) > 0 {
			errMsg = fmt.Sprintf("kraken: AssetPairs API error: %s", strings.Join(resp.Error, ", "))
		}
		return fmt.Errorf("%s (underlying: %w)", errMsg, err)
	}
	if len(resp.Error) > 0 {
		return fmt.Errorf("kraken: AssetPairs API error on successful call: %s", strings.Join(resp.Error, ", "))
	}

	c.dataMu.Lock()
	defer c.dataMu.Unlock()

	if len(c.assetInfoMap) == 0 {
		c.logger.LogFatal("Kraken Client: Asset map is empty. RefreshAssets() must be called before RefreshAssetPairs().")
		return errors.New("asset map not initialized")
	}

	c.pairInfoMap = resp.Result
	c.pairDetailsCache = make(map[string]PairDetail)
	c.commonToKrakenPair = make(map[string]string)
	c.krakenToCommonPair = make(map[string]string) // Initialize the new map

	for krakenPairName, pairInfo := range resp.Result {
		c.pairDetailsCache[krakenPairName] = PairDetail{
			PairDecimals: pairInfo.PairDecimals,
			LotDecimals:  pairInfo.LotDecimals,
			OrderMin:     pairInfo.OrderMin,
		}

		baseAssetInfo, baseOk := c.assetInfoMap[pairInfo.Base]
		quoteAssetInfo, quoteOk := c.assetInfoMap[pairInfo.Quote]

		if !baseOk || !quoteOk {
			continue
		}

		commonBase := baseAssetInfo.Altname
		commonQuote := quoteAssetInfo.Altname

		if commonBase != "" && commonQuote != "" {
			normalizedPair := normalizeKrakenPair(krakenPairName)
			commonPairKey := fmt.Sprintf("%s/%s", commonBase, commonQuote)
			c.commonToKrakenPair[commonPairKey] = krakenPairName
			c.krakenToCommonPair[normalizedPair] = commonPairKey // Use normalized name

			if commonBase == "XBT" {
				btcPairKey := fmt.Sprintf("BTC/%s", commonQuote)
				c.commonToKrakenPair[btcPairKey] = krakenPairName
			}
		}
	}

	c.logger.LogInfo("Kraken Client: Refreshed %d asset pairs. Mapped %d human-readable pairs to Kraken API pairs.", len(c.pairInfoMap), len(c.commonToKrakenPair))
	return nil
}
func normalizeKrakenPair(pair string) string {
	// Normalizes variations like XXBTZUSD, XBTUSD, SOLUSD -> BTCUSD, BTCUSD, SOLUSD
	// This is a simplified example; a production version might need more rules.
	p := strings.ToUpper(pair)
	p = strings.Replace(p, "XBT", "BTC", 1) // Handle the most common BTC variant
	p = strings.TrimPrefix(p, "X")
	p = strings.Replace(p, "ZUSD", "USD", 1) // Handle USD variation
	return p
}

func (c *Client) RefreshAssets(ctx context.Context) error {
	c.logger.LogInfo("Kraken Client: Refreshing assets info...")
	var resp struct {
		Error  []string             `json:"error"`
		Result map[string]AssetInfo `json:"result"`
	}
	err := c.callPublic(ctx, "/0/public/Assets", nil, &resp)
	if err != nil {
		if len(resp.Error) > 0 {
			return fmt.Errorf("kraken: Assets API error: %s (underlying: %w)", strings.Join(resp.Error, ", "), err)
		}
		return fmt.Errorf("kraken: RefreshAssets API call failed: %w", err)
	}
	if len(resp.Error) > 0 {
		return fmt.Errorf("kraken: Assets API error: %s", strings.Join(resp.Error, ", "))
	}

	c.dataMu.Lock()
	defer c.dataMu.Unlock()
	c.assetInfoMap = make(map[string]AssetInfo)
	for krakenName, info := range resp.Result {
		c.assetInfoMap[krakenName] = info
	}
	c.logger.LogInfo("Kraken Client: Refreshed %d assets.", len(c.assetInfoMap))
	return nil
}
