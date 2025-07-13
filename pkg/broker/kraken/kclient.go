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

// Client struct now holds more sophisticated maps for translation
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

	// Maps for asset name translation
	assetInfoMap        map[string]AssetInfo
	commonToKrakenAsset map[string]string

	// Maps for pair name translation
	pairInfoMap           map[string]AssetPairAPIInfo
	pairDetailsCache      map[string]PairDetail
	commonToTradeablePair map[string]string
	commonToPrimaryPair   map[string]string
	commonToKrakenPair    map[string]string
	krakenToCommonPair    map[string]string
}

// getCommonAssetName is a helper to derive a standardized asset name from Kraken's AssetInfo.
func getCommonAssetName(info AssetInfo) string {
	// The altname is usually the common ticker (e.g., ETH for XETH).
	altname := info.Altname
	// Handle Bitcoin's specific altname.
	if altname == "XBT" {
		return "BTC"
	}
	// For assets like "ETH2.S", we just want "ETH2".
	return strings.Split(altname, ".")[0]
}

// RefreshAssets fetches the latest asset data from Kraken and builds the necessary translation maps.
// This function has been modified to correctly populate the common-to-Kraken asset map.
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

	// --- FIX: Overwrite old maps and build them with the correct logic ---
	c.assetInfoMap = make(map[string]AssetInfo)
	c.commonToKrakenAsset = make(map[string]string)

	// First, populate the assetInfoMap with the primary Kraken names (e.g., "XETH").
	// This map is useful for getting detailed info from a primary name.
	for krakenName, info := range resp.Result {
		c.assetInfoMap[krakenName] = info
	}

	// Second, build the crucial translation map from common names to Kraken names.
	// This is the key fix for the asset lookup errors.
	for krakenName, info := range resp.Result {
		commonName := getCommonAssetName(info)
		if commonName != "" {
			// Map the common name (e.g., "BTC") to the primary Kraken name (e.g., "XXBT").
			// Use uppercase for the key for consistent lookups.
			c.commonToKrakenAsset[strings.ToUpper(commonName)] = krakenName

			// Also, add the altname to the assetInfoMap so we can look up info by altname if needed.
			if info.Altname != "" && info.Altname != krakenName {
				c.assetInfoMap[info.Altname] = info
			}
		}
	}

	c.logger.LogInfo("Kraken Client: Refreshed and mapped %d assets with all aliases.", len(resp.Result))
	return nil
}

// RefreshAssetPairs fetches the latest pair data and builds the pair translation maps.
// This function has been modified to populate all necessary maps correctly.
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

	// Initialize all pair-related maps
	c.pairInfoMap = resp.Result
	c.commonToTradeablePair = make(map[string]string)
	c.commonToPrimaryPair = make(map[string]string)
	c.krakenToCommonPair = make(map[string]string)
	c.pairDetailsCache = make(map[string]PairDetail)
	// --- FIX: Initialize the map that was being missed ---
	c.commonToKrakenPair = make(map[string]string)

	for primaryPairName, pairInfo := range resp.Result {
		baseInfo, baseOk := c.assetInfoMap[pairInfo.Base]
		quoteInfo, quoteOk := c.assetInfoMap[pairInfo.Quote]
		if !baseOk || !quoteOk {
			c.logger.LogDebug("Skipping pair %s: base asset '%s' or quote asset '%s' not found in asset map.", primaryPairName, pairInfo.Base, pairInfo.Quote)
			continue
		}

		commonBase := getCommonAssetName(baseInfo)
		commonQuote := getCommonAssetName(quoteInfo)
		if commonBase == "" || commonQuote == "" {
			continue
		}

		commonPairKey := fmt.Sprintf("%s/%s", commonBase, commonQuote)
		tradeablePairName := pairInfo.Altname

		// --- FIX: Populate all maps with the correct names ---
		c.commonToPrimaryPair[commonPairKey] = primaryPairName
		c.commonToTradeablePair[commonPairKey] = tradeablePairName

		// This is the missing piece. Populate the map used by GetKrakenPairName.
		// We point it to the primary name (e.g. XETHZUSD), as that's needed for data fetching.
		c.commonToKrakenPair[commonPairKey] = primaryPairName

		// This map translates from any Kraken pair name back to our common format.
		c.krakenToCommonPair[primaryPairName] = commonPairKey
		if tradeablePairName != "" && tradeablePairName != primaryPairName {
			c.krakenToCommonPair[tradeablePairName] = commonPairKey
		}

		// Cache details by the tradeable name, which is used for placing orders.
		c.pairDetailsCache[tradeablePairName] = PairDetail{
			PairDecimals: pairInfo.PairDecimals,
			LotDecimals:  pairInfo.LotDecimals,
			OrderMin:     pairInfo.OrderMin,
		}
	}
	c.logger.LogInfo("Kraken Client: Refreshed %d asset pairs and built comprehensive translation maps.", len(c.pairInfoMap))
	return nil
}

// GetKrakenAssetName translates a common asset name (e.g., "BTC") to its Kraken equivalent (e.g., "XXBT").
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

// GetKrakenPairName translates a common pair (e.g., "ETH/USD") to its primary Kraken equivalent (e.g., "XETHZUSD").
func (c *Client) GetKrakenPairName(ctx context.Context, commonPair string) (string, error) {
	c.dataMu.RLock()
	krakenPair, ok := c.commonToKrakenPair[commonPair]
	c.dataMu.RUnlock()

	if ok {
		return krakenPair, nil
	}

	// Pair not found, let's refresh both assets and pairs to be safe
	c.logger.LogWarn("Kraken pair for '%s' not found, refreshing assets and pairs...", commonPair)
	if err := c.RefreshAssets(ctx); err != nil {
		return "", fmt.Errorf("failed to refresh assets while looking for pair %s: %w", commonPair, err)
	}
	if err := c.RefreshAssetPairs(ctx); err != nil {
		return "", fmt.Errorf("failed to refresh pairs while looking for pair %s: %w", commonPair, err)
	}

	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	krakenPair, ok = c.commonToKrakenPair[commonPair]
	if !ok {
		return "", fmt.Errorf("pair %s not found after refresh", commonPair)
	}
	return krakenPair, nil
}

// --- The rest of the file remains unchanged ---

func NewClient(appCfg *utilities.KrakenConfig, HTTPClient *http.Client, logger *utilities.Logger) *Client {
	return &Client{
		BaseURL:               appCfg.BaseURL,
		APIKey:                appCfg.APIKey,
		APISecret:             appCfg.APISecret,
		HTTPClient:            &http.Client{Timeout: time.Duration(appCfg.RequestTimeoutSec) * time.Second},
		limiter:               rate.NewLimiter(rate.Limit(1), 3), // Example rate limit
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

	if !ok {
		c.logger.LogWarn("Common name for Kraken asset '%s' not found, refreshing...", krakenAssetName)
		if err := c.RefreshAssets(ctx); err != nil {
			return "", fmt.Errorf("failed to refresh assets while getting common name for %s: %w", krakenAssetName, err)
		}
		c.dataMu.RLock()
		assetInfo, ok = c.assetInfoMap[krakenAssetName]
		c.dataMu.RUnlock()
		if !ok {
			return "", fmt.Errorf("common asset name for Kraken asset %s not found even after refresh", krakenAssetName)
		}
	}

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

func (c *Client) GetPrimaryKrakenPairName(ctx context.Context, commonPair string) (string, error) {
	c.dataMu.RLock()
	pair, ok := c.commonToPrimaryPair[commonPair]
	c.dataMu.RUnlock()
	if ok {
		return pair, nil
	}

	if err := c.RefreshAssetPairs(ctx); err != nil {
		return "", err
	}

	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	pair, ok = c.commonToPrimaryPair[commonPair]
	if !ok {
		return "", fmt.Errorf("primary pair for %s not found after refresh", commonPair)
	}
	return pair, nil
}

func (c *Client) GetTradeableKrakenPairName(ctx context.Context, commonPair string) (string, error) {
	c.dataMu.RLock()
	pair, ok := c.commonToTradeablePair[commonPair]
	c.dataMu.RUnlock()
	if ok {
		return pair, nil
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
