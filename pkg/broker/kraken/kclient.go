package kraken

import (
	"Snowballin/pkg/broker"
	"Snowballin/utilities"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Client manages all low-level communication with the Kraken API.
// It handles request signing, rate limiting, and mapping between Kraken's asset names
// and the application's common asset names (e.g., "XBT" vs "BTC").
type Client struct {
	BaseURL        string
	APIKey         string
	APISecret      string
	HTTPClient     *http.Client
	limiter        *rate.Limiter
	logger         *utilities.Logger
	nonceGenerator *utilities.KrakenNonceGenerator
	cfg            *utilities.KrakenConfig
	dataMu         sync.RWMutex // Protects all map caches below

	// Simplified maps for configured pairs only (pruned for efficiency)
	// commonToTradeablePair maps a common pair name (e.g., "BTC/USD") to Kraken's altname (e.g., "XBTUSD").
	commonToTradeablePair map[string]string
	// commonToPrimaryPair maps a common pair name (e.g., "BTC/USD") to Kraken's primary pair (e.g., "XXBTZUSD").
	commonToPrimaryPair map[string]string
	// AssetPairInfo stores curated AssetPairInfo structs for quick access to formatting rules.
	pairDetailsCache map[string]AssetPairInfo
	// commonToKrakenAsset maps common asset (e.g., "BTC") to Kraken altname (e.g., "XBT").
	commonToKrakenAsset map[string]string
}

// OHLCResponse defines the structure for Kraken's OHLC API response.
// The Result field is a map where the key is the asset pair name, and the value
// is either the list of candles or a 'last' timestamp for pagination.
type OHLCResponse struct {
	Error  []string               `json:"error"`
	Result map[string]interface{} `json:"result"`
}

// NewClient initializes and returns a new Kraken API client.
// It requires configuration, an HTTP client, and a logger to operate.
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
		commonToTradeablePair: make(map[string]string),
		commonToPrimaryPair:   make(map[string]string),
		pairDetailsCache:      make(map[string]AssetPairInfo),
		commonToKrakenAsset:   make(map[string]string),
	}
}

// getAltNameForCommon queries Kraken Assets API for a single common asset symbol and returns its altname.
func (c *Client) getAltNameForCommon(ctx context.Context, common string) (string, error) {
	var resp struct {
		Error  []string             `json:"error"`
		Result map[string]AssetInfo `json:"result"`
	}
	params := url.Values{"asset": {common}}
	if err := c.callPublic(ctx, "/0/public/Assets", params, &resp); err != nil {
		return "", fmt.Errorf("getAltNameForCommon %s: API call failed: %w", common, err)
	}
	if len(resp.Error) > 0 {
		// Specifically ignore "Unknown asset" for fiat, as they might not be in the Assets list
		if strings.Contains(strings.Join(resp.Error, ", "), "EQuery:Unknown asset") && (strings.EqualFold(common, "USD") || strings.EqualFold(common, "EUR")) {
			c.logger.LogDebug("Known fiat '%s' not found in Assets endpoint, assuming altname is the same.", common)
			return common, nil
		}
		return "", fmt.Errorf("getAltNameForCommon %s: API error: %s", common, strings.Join(resp.Error, ", "))
	}
	if len(resp.Result) == 0 {
		return "", fmt.Errorf("getAltNameForCommon %s: no result returned", common)
	}
	// Take the first (only) entry's altname
	for _, info := range resp.Result {
		return info.Altname, nil
	}
	return "", errors.New("getAltNameForCommon: no altname found in result")
}

// RefreshAssetPairs fetches all necessary asset and pair data from Kraken for the configured pairs.
// It uses an efficient batching method to minimize API calls:
// 1. Normalizes configured pair strings (e.g., "BTC" -> "BTC/USD").
// 2. Gathers all unique base and quote assets from the pairs.
// 3. Makes an individual API call to the "Assets" endpoint for each unique asset to reliably get its altname.
// 4. Constructs the Kraken pair names (e.g., "XBT" + "USD" -> "XBTUSD").
// 5. Makes a single API call to the "AssetPairs" endpoint to get details for all constructed pairs.
// 6. Caches all mappings (common-to-tradeable, common-to-primary, asset-altnames, and pair details).
func (c *Client) RefreshAssetPairs(ctx context.Context, configuredPairs []string) error {
	if len(configuredPairs) == 0 {
		return errors.New("RefreshAssetPairs requires at least one configured pair")
	}
	c.logger.LogInfo("Kraken Client: Starting refresh of asset pairs for %v...", configuredPairs)

	c.dataMu.Lock()
	defer c.dataMu.Unlock()

	// Clear existing maps to ensure fresh data
	c.commonToTradeablePair = make(map[string]string)
	c.commonToPrimaryPair = make(map[string]string)
	c.pairDetailsCache = make(map[string]AssetPairInfo)
	c.commonToKrakenAsset = make(map[string]string)

	// Step 1: Normalize pairs and collect unique assets
	normalizedPairs := make(map[string]string) // map[common format]original format
	uniqueAssets := make(map[string]bool)
	for _, pair := range configuredPairs {
		p := strings.ToUpper(strings.TrimSpace(pair))
		if !strings.Contains(p, "/") {
			p = p + "/USD" // Assume USD quote if missing
		}
		parts := strings.Split(p, "/")
		if len(parts) != 2 {
			c.logger.LogWarn("Invalid pair format '%s', skipping", pair)
			continue
		}
		base, quote := parts[0], parts[1]
		commonPair := fmt.Sprintf("%s/%s", base, quote)
		normalizedPairs[commonPair] = pair
		uniqueAssets[base] = true
		uniqueAssets[quote] = true
	}

	if len(uniqueAssets) == 0 {
		return errors.New("no valid assets extracted from configured pairs")
	}

	// Step 2 & 3: Get altnames for all unique assets by calling the API for each one.
	// This is more robust than a single bulk call, as the response keys can be unpredictable.
	for asset := range uniqueAssets {
		alt, err := c.getAltNameForCommon(ctx, asset)
		if err != nil {
			// A special case for USD, which may not be in the Assets list but is a valid quote.
			if strings.EqualFold(asset, "USD") {
				c.logger.LogWarn("Could not resolve 'USD' from API, falling back to default 'USD'. This is usually safe.")
				c.commonToKrakenAsset[asset] = "USD"
				continue
			}
			return fmt.Errorf("failed to get asset altname for %s: %w", asset, err)
		}
		c.logger.LogDebug("Resolved asset %s -> %s", asset, alt)
		c.commonToKrakenAsset[asset] = alt
	}

	// Step 4: Construct Kraken pair altnames
	pairAltsToCommon := make(map[string]string) // map[XBTUSD]BTC/USD
	var pairAltsList []string
	for commonPair := range normalizedPairs {
		parts := strings.Split(commonPair, "/")
		base, quote := parts[0], parts[1]

		baseAlt, ok1 := c.commonToKrakenAsset[base]
		quoteAlt, ok2 := c.commonToKrakenAsset[quote]

		if !ok1 || !ok2 {
			c.logger.LogWarn("Could not find required altname for pair '%s' (Base: %s, Quote: %s), skipping", commonPair, base, quote)
			continue
		}

		krakenPairAlt := baseAlt + quoteAlt
		pairAltsList = append(pairAltsList, krakenPairAlt)
		pairAltsToCommon[krakenPairAlt] = commonPair
	}

	if len(pairAltsList) == 0 {
		return errors.New("no valid kraken pair altnames could be constructed")
	}

	// Step 5: Get info for all constructed pairs in a single API call
	pairsStr := strings.Join(pairAltsList, ",")
	c.logger.LogDebug("Querying pair details for: %s", pairsStr)
	pairInfos, err := c.getAssetPairsForAlts(ctx, pairsStr)
	if err != nil {
		return fmt.Errorf("failed to get asset pair info: %w", err)
	}

	// Step 6: Build the final maps from the API response
	for krakenPrimary, info := range pairInfos {
		commonPair, ok := pairAltsToCommon[info.Altname]
		if !ok {
			c.logger.LogWarn("Received pair info for '%s' (%s) which was not in the original request, skipping", info.Altname, krakenPrimary)
			continue
		}

		c.commonToTradeablePair[commonPair] = info.Altname
		c.commonToPrimaryPair[commonPair] = krakenPrimary

		var makerFee, takerFee float64
		if len(info.FeesMaker) > 0 && len(info.FeesMaker[0]) > 1 {
			if f, err := info.FeesMaker[0][1].Float64(); err == nil {
				makerFee = f / 100.0
			}
		}
		if len(info.Fees) > 0 && len(info.Fees[0]) > 1 {
			if f, err := info.Fees[0][1].Float64(); err == nil {
				takerFee = f / 100.0
			}
		}

		c.pairDetailsCache[commonPair] = AssetPairInfo{
			PairDecimals: info.PairDecimals,
			LotDecimals:  info.LotDecimals,
			OrderMin:     info.OrderMin,
			MakerFee:     makerFee,
			TakerFee:     takerFee,
		}
		c.logger.LogDebug("Mapped %s -> tradeable: %s, primary: %s, fees(M/T): %.4f/%.4f", commonPair, info.Altname, krakenPrimary, makerFee, takerFee)
	}

	if len(c.commonToTradeablePair) == 0 {
		return fmt.Errorf("failed to map any of the configured pairs: %v", configuredPairs)
	}

	c.logger.LogInfo("Kraken Client: Successfully refreshed mappings for %d pairs.", len(c.commonToTradeablePair))
	return nil
}

// getAssetPairsForAlts queries the AssetPairs endpoint for a comma-separated list of pair altnames.
// It returns the raw result from the API, where keys are the primary pair names (e.g., XXBTZUSD).
func (c *Client) getAssetPairsForAlts(ctx context.Context, pairsStr string) (map[string]AssetPairInfo, error) {
	var resp AssetPairsResponse
	params := url.Values{"pair": {pairsStr}}
	if err := c.callPublic(ctx, "/0/public/AssetPairs", params, &resp); err != nil {
		return nil, fmt.Errorf("API call to AssetPairs failed: %w", err)
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("API error from AssetPairs: %s", strings.Join(resp.Error, ", "))
	}
	return resp.Result, nil
}

// GetAssetPairsAPI queries AssetPairs for a specific pair string and returns the map.
func (c *Client) GetAssetPairsAPI(ctx context.Context, pairStr string) (map[string]AssetPairInfo, error) {
	var resp AssetPairsResponse
	params := url.Values{"pair": {pairStr}}
	if err := c.callPublic(ctx, "/0/public/AssetPairs", params, &resp); err != nil {
		return nil, err
	}
	if len(resp.Error) > 0 {
		return nil, errors.New(strings.Join(resp.Error, ", "))
	}
	return resp.Result, nil
}

// GetTradeableKrakenPairName returns the Kraken altname for a common pair (e.g., "XBTUSD" for "BTC/USD").
func (c *Client) GetTradeableKrakenPairName(ctx context.Context, commonPair string) (string, error) {
	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	if tradeable, ok := c.commonToTradeablePair[commonPair]; ok {
		return tradeable, nil
	}
	return "", fmt.Errorf("no tradeable pair mapping for %s; run RefreshAssetPairs first", commonPair)
}

// GetPrimaryKrakenPairName returns the Kraken primary pair for a common pair (e.g., "XXBTZUSD").
func (c *Client) GetPrimaryKrakenPairName(ctx context.Context, commonPair string) (string, error) {
	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	if primary, ok := c.commonToPrimaryPair[commonPair]; ok {
		return primary, nil
	}
	return "", fmt.Errorf("no primary pair mapping for %s; run RefreshAssetPairs first", commonPair)
}

// GetCommonPairName reverse-maps a Kraken pair (alt or primary) to common format.
func (c *Client) GetCommonPairName(ctx context.Context, krakenPair string) (string, error) {
	c.dataMu.RLock()
	defer c.dataMu.RUnlock()
	for common, tradeable := range c.commonToTradeablePair {
		if tradeable == krakenPair {
			return common, nil
		}
	}
	for common, primary := range c.commonToPrimaryPair {
		if primary == krakenPair {
			return common, nil
		}
	}
	return "", fmt.Errorf("no common pair for Kraken pair %s", krakenPair)
}

// GetCommonAssetName reverse-maps a Kraken asset (e.g., "XXBT", "ZUSD") to its common symbol ("BTC", "USD").
// The fallback logic has been improved to correctly handle prefixed assets by checking the map's values
// instead of its keys, resolving the 'XXBT' unmapped asset error.
func (c *Client) GetCommonAssetName(ctx context.Context, krakenAsset string) (string, error) {
	c.dataMu.RLock()
	defer c.dataMu.RUnlock()

	// Perform a reverse lookup on the map to find the common name.
	for common, kraken := range c.commonToKrakenAsset {
		if kraken == krakenAsset {
			return common, nil
		}
	}

	// Fallback for prefixed assets (e.g., XXBT, ZUSD)
	if strings.HasPrefix(krakenAsset, "Z") || strings.HasPrefix(krakenAsset, "X") {
		trimmed := krakenAsset[1:] // e.g., "XXBT" -> "XBT"
		// Now, perform the reverse lookup on the trimmed version.
		for common, kraken := range c.commonToKrakenAsset {
			if kraken == trimmed {
				return common, nil
			}
		}
	}

	return "", fmt.Errorf("no common asset found for Kraken asset %s", krakenAsset)
}

// GetKrakenAssetAltName returns altname for common asset (internal helper).
func (c *Client) GetKrakenAssetAltName(ctx context.Context, common string) (string, error) {
	c.dataMu.RLock()
	if alt, ok := c.commonToKrakenAsset[strings.ToUpper(common)]; ok {
		c.dataMu.RUnlock()
		return alt, nil
	}
	c.dataMu.RUnlock()

	// On miss, query
	alt, err := c.getAltNameForCommon(ctx, strings.ToUpper(common))
	if err != nil {
		return "", err
	}
	c.dataMu.Lock()
	c.commonToKrakenAsset[strings.ToUpper(common)] = alt
	c.dataMu.Unlock()
	return alt, nil
}

// GetPairDetail retrieves cached formatting and limit details for a given common pair.
func (c *Client) GetPairDetail(ctx context.Context, commonPair string) (AssetPairInfo, error) {
	c.dataMu.RLock()
	detail, ok := c.pairDetailsCache[commonPair]
	c.dataMu.RUnlock()

	if ok {
		return detail, nil
	}

	return AssetPairInfo{}, fmt.Errorf("pair detail for %s not found; ensure RefreshAssetPairs run", commonPair)
}

// GetOHLCV fetches OHLCV data from Kraken. It has been made more robust
// to handle cases where the API response key for the pair data does not exactly
// match the pair name used in the request. It now iterates through the response
// map to find the candle data, making it resilient to API inconsistencies.
func (c *Client) GetOHLCV(ctx context.Context, pair, interval string, sinceMinutes int64) ([]utilities.OHLCVBar, error) {
	params := url.Values{
		"pair":     {pair},
		"interval": {interval},
	}
	if sinceMinutes > 0 {
		sinceTime := time.Now().Add(-time.Duration(sinceMinutes) * time.Minute).Unix()
		params.Set("since", strconv.FormatInt(sinceTime, 10))
	}

	var resp OHLCResponse
	if err := c.callPublic(ctx, "/0/public/OHLC", params, &resp); err != nil {
		return nil, fmt.Errorf("GetOHLCV: API call failed: %w", err)
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("GetOHLCV: API error: %s", strings.Join(resp.Error, ", "))
	}

	var rawBarsData [][]interface{}
	// Iterate over the result map to find the candle data. The key is the primary
	// pair name (e.g., XXBTZUSD) and may not match the requested 'pair' string.
	for key, value := range resp.Result {
		if key == "last" {
			continue
		}

		// The JSON decoder unmarshals an array of arrays into a slice of interfaces,
		// where each element is another interface holding a slice.
		// We must first assert the top-level slice.
		topLevelSlice, ok := value.([]interface{})
		if !ok {
			continue // This key does not contain an array, so skip it.
		}

		// Now, build the [][]interface{} structure that the parsing logic expects.
		reconstructedBars := make([][]interface{}, 0, len(topLevelSlice))
		isDataValid := true
		for _, item := range topLevelSlice {
			// Assert that each item in the top-level slice is also a slice.
			if innerSlice, ok := item.([]interface{}); ok {
				reconstructedBars = append(reconstructedBars, innerSlice)
			} else {
				// If any item is not a slice, this is not the candle data we are looking for.
				isDataValid = false
				break
			}
		}

		if isDataValid {
			rawBarsData = reconstructedBars
			c.logger.LogDebug("GetOHLCV [%s]: Successfully parsed candle data found under API response key '%s'", pair, key)
			break // Data found and parsed, exit the loop.
		}
	}

	if rawBarsData == nil {
		return nil, fmt.Errorf("GetOHLCV: could not find and parse OHLCV data for pair %s in API response", pair)
	}

	bars := make([]utilities.OHLCVBar, 0, len(rawBarsData))
	for _, row := range rawBarsData {
		if len(row) < 7 {
			continue
		}
		ts, err1 := utilities.ParseFloatFromInterface(row[0])
		open, err2 := utilities.ParseFloatFromInterface(row[1])
		high, err3 := utilities.ParseFloatFromInterface(row[2])
		low, err4 := utilities.ParseFloatFromInterface(row[3])
		closePrice, err5 := utilities.ParseFloatFromInterface(row[4])
		volume, err6 := utilities.ParseFloatFromInterface(row[6])

		if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil || err6 != nil {
			c.logger.LogWarn("GetOHLCV: failed to parse one or more values in OHLC row for pair %s.", pair)
			continue
		}

		bars = append(bars, utilities.OHLCVBar{
			Timestamp: int64(ts) * 1000,
			Open:      open,
			High:      high,
			Low:       low,
			Close:     closePrice,
			Volume:    volume,
		})
	}

	return bars, nil
}

// AddOrderAPI submits a new order to Kraken's private API.
func (c *Client) AddOrderAPI(ctx context.Context, params url.Values) (string, error) {
	var resp struct {
		Error  []string `json:"error"`
		Result struct {
			Descr struct {
				Order string `json:"order"`
			} `json:"descr"`
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

// CancelOrderAPI cancels an existing order on Kraken's private API.
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

// QueryOrdersAPI retrieves details for one or more orders from Kraken's private API.
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

// GetBalancesAPI retrieves all account balances from Kraken's private API.
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

// GetTradesAPI fetches recent public trade data for a given pair.
func (c *Client) GetTradesAPI(ctx context.Context, pair string, since string) (interface{}, error) {
	params := url.Values{"pair": {pair}}
	if since != "" {
		params.Set("since", since)
	}

	var resp struct {
		Error  []string    `json:"error"`
		Result interface{} `json:"result"`
	}

	if err := c.callPublic(ctx, "/0/public/Trades", params, &resp); err != nil {
		return nil, fmt.Errorf("GetTradesAPI: API call failed: %w", err)
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("GetTradesAPI: API error: %s", strings.Join(resp.Error, ", "))
	}
	return resp.Result, nil
}

// GetOrderBookAPI fetches the order book for a given pair.
func (c *Client) GetOrderBookAPI(ctx context.Context, pair string, depth int) (map[string]interface{}, error) {
	params := url.Values{
		"pair":  {pair},
		"count": {strconv.Itoa(depth)},
	}

	var resp struct {
		Error  []string               `json:"error"`
		Result map[string]interface{} `json:"result"`
	}

	if err := c.callPublic(ctx, "/0/public/Depth", params, &resp); err != nil {
		return nil, fmt.Errorf("GetOrderBookAPI: API call failed: %w", err)
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("GetOrderBookAPI: API error: %s", strings.Join(resp.Error, ", "))
	}

	// The result is nested under the pair name key, so we return the inner map.
	for _, orderBook := range resp.Result {
		if ob, ok := orderBook.(map[string]interface{}); ok {
			return ob, nil
		}
	}

	return nil, fmt.Errorf("order book for pair %s not found in API response", pair)
}

// GetTickerAPI fetches ticker information for one or more pairs from Kraken's public API.
func (c *Client) GetTickerAPI(ctx context.Context, pair string) (map[string]TickerInfo, error) {
	params := url.Values{}
	if pair != "" {
		params.Set("pair", pair)
	}
	var resp TickerResponse
	if err := c.callPublic(ctx, "/0/public/Ticker", params, &resp); err != nil {
		return nil, fmt.Errorf("GetTickerAPI: callPublic failed: %w", err)
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("GetTickerAPI: API error: %s", strings.Join(resp.Error, ", "))
	}
	return resp.Result, nil
}

// ParseTicker converts raw TickerInfo to broker.TickerData.
func (c *Client) ParseTicker(info TickerInfo, pair string) (broker.TickerData, error) {
	last, _ := strconv.ParseFloat(info.LastTradeClosed[0], 64)

	return broker.TickerData{
		Pair:      pair,
		LastPrice: last,
	}, nil
}

// callPrivate handles making authenticated POST requests to private Kraken API endpoints.
// It includes logic for nonce generation, request signing, and retries with exponential backoff on rate limit errors.
func (c *Client) callPrivate(ctx context.Context, apiPath string, data url.Values, target interface{}) error {
	if c.APIKey == "" || c.APISecret == "" {
		return errors.New("kraken: API key or secret not configured")
	}

	var lastErr error
	maxRetries := c.cfg.MaxRetries
	backoff := time.Duration(c.cfg.RetryDelaySec) * time.Second

	for i := 0; i < maxRetries; i++ {
		nonce := c.nonceGenerator.Nonce()
		nonceStr := strconv.FormatUint(nonce, 10)
		if data == nil {
			data = url.Values{}
		}
		data.Set("nonce", nonceStr)

		authHeaders, authErr := utilities.GenerateKrakenAuthHeaders(c.APIKey, c.APISecret, apiPath, nonceStr, data)
		if authErr != nil {
			return fmt.Errorf("kraken: generate auth headers for %s: %w", apiPath, authErr)
		}

		fullURL := c.BaseURL + apiPath
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, strings.NewReader(data.Encode()))
		if reqErr != nil {
			return fmt.Errorf("kraken: create private request for %s: %w", apiPath, reqErr)
		}

		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("User-Agent", "SnowballinBot/1.0")
		for key, val := range authHeaders {
			req.Header.Set(key, val)
		}
		c.logger.LogDebug("Kraken callPrivate: URL=%s, Nonce=%s, Attempt=%d", fullURL, nonceStr, i+1)

		lastErr = utilities.DoJSONRequest(c.HTTPClient, req, 0, 0, target)

		if lastErr != nil {
			if strings.Contains(lastErr.Error(), "EAPI:Rate limit exceeded") {
				c.logger.LogWarn("Rate limit exceeded on %s. Waiting %v before retrying... (Attempt %d/%d)", apiPath, backoff, i+1, maxRetries)
				select {
				case <-time.After(backoff):
					backoff *= 2 // Exponential backoff
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return fmt.Errorf("kraken private call to %s failed: %w", apiPath, lastErr)
		}

		return nil
	}

	return fmt.Errorf("API call to %s failed after %d retries: %w", apiPath, maxRetries, lastErr)
}

// callPublic handles making GET requests to public Kraken API endpoints.
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

	return utilities.DoJSONRequest(c.HTTPClient, req, c.cfg.MaxRetries, time.Duration(c.cfg.RetryDelaySec)*time.Second, target)
}
