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

// PairDetail holds essential formatting and limit information for a specific trading pair.
// This data is cached within the client to avoid repeated API calls for static information.
type PairDetail struct {
	PairDecimals int
	LotDecimals  int
	OrderMin     string
}

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

	// assetInfoMap caches raw asset details from the /Assets endpoint, keyed by Kraken's asset name (e.g., "XXBT").
	assetInfoMap map[string]AssetInfo
	// commonToKrakenAsset maps a common uppercase asset name (e.g., "BTC") to Kraken's primary asset name (e.g., "XXBT").
	commonToKrakenAsset map[string]string

	// pairInfoMap caches raw pair details from the /AssetPairs endpoint, keyed by Kraken's primary pair name (e.g., "XBTUSD").
	pairInfoMap map[string]AssetPairInfo
	// pairDetailsCache stores curated PairDetail structs for quick access to formatting rules.
	pairDetailsCache map[string]PairDetail
	// commonToTradeablePair maps a common pair name (e.g., "BTC/USD") to Kraken's altname or primary name used for trading.
	commonToTradeablePair map[string]string
	// commonToPrimaryPair maps a common pair name (e.g., "BTC/USD") to Kraken's unique, non-altname pair identifier.
	commonToPrimaryPair map[string]string
	// commonToKrakenPair maps a common pair name (e.g., "BTC/USD") to the Kraken pair name.
	commonToKrakenPair map[string]string
	// krakenToCommonPair maps a Kraken pair name (primary or altname) back to the common "BASE/QUOTE" format.
	krakenToCommonPair map[string]string
}

// buildPairMaps builds the internal maps for pair names, details, and aliases.
func (c *Client) buildPairMaps(pairs map[string]AssetPairInfo) {
	for pairName, pair := range pairs {
		commonBase, err := c.GetCommonAssetName(context.Background(), pair.Base)
		if err != nil {
			continue
		}
		commonQuote, err := c.GetCommonAssetName(context.Background(), pair.Quote)
		if err != nil {
			continue
		}
		commonPair := fmt.Sprintf("%s/%s", strings.ToUpper(commonBase), strings.ToUpper(commonQuote))

		c.pairInfoMap[pairName] = pair
		c.commonToKrakenPair[commonPair] = pairName
		c.krakenToCommonPair[pairName] = commonPair
		c.commonToPrimaryPair[commonPair] = pairName
		if pair.Altname != "" {
			c.commonToTradeablePair[commonPair] = pair.Altname
			c.krakenToCommonPair[pair.Altname] = commonPair
		}
		ordermin, _ := strconv.ParseFloat(pair.OrderMin, 64)
		c.pairDetailsCache[commonPair] = PairDetail{
			PairDecimals: pair.PairDecimals,
			LotDecimals:  pair.LotDecimals,
			OrderMin:     fmt.Sprintf("%f", ordermin),
		}
	}
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
		assetInfoMap:          make(map[string]AssetInfo),
		commonToKrakenAsset:   make(map[string]string),
		pairInfoMap:           make(map[string]AssetPairInfo),
		commonToKrakenPair:    make(map[string]string),
		krakenToCommonPair:    make(map[string]string),
		commonToPrimaryPair:   make(map[string]string),
		commonToTradeablePair: make(map[string]string),
		pairDetailsCache:      make(map[string]PairDetail),
	}
}

// getCommonAssetName is a helper to derive a common symbol from Kraken's AssetInfo.
// It primarily parses the altname, removing suffixes like ".S".
func getCommonAssetName(info AssetInfo) string {
	// Handles assets like "ETH2.S" -> "ETH2" or "SOL" -> "SOL"
	return strings.Split(info.Altname, ".")[0]
}

// RefreshAssets fetches the latest asset data from Kraken and rebuilds the internal asset maps.
// This is crucial for translating between common names and Kraken's specific names.
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

	for krakenName, info := range resp.Result {
		c.assetInfoMap[krakenName] = info
		if info.Altname != "" && info.Altname != krakenName {
			c.assetInfoMap[info.Altname] = info // Also map by altname for easier lookup
		}
		commonName := getCommonAssetName(info)
		c.commonToKrakenAsset[commonName] = krakenName
	}

	c.logger.LogInfo("Kraken Client: Refreshed and cached %d assets.", len(resp.Result))
	return nil
}

// RefreshAssetPairs fetches asset pair data from Kraken and builds the pair translation maps.
// It uses a two-stage fetch: first, it gets all pairs to learn the canonical primary names,
// then it resolves the user's configured common pairs against that canonical data.
// This allows it to programmatically learn all mappings without any hardcoding.
func (c *Client) RefreshAssetPairs(ctx context.Context, configuredPairs []string) error {
	if len(configuredPairs) == 0 {
		return errors.New("RefreshAssetPairs requires at least one configured pair to build mappings")
	}
	c.logger.LogInfo("Kraken Client: Refreshing asset pairs using %v...", configuredPairs)

	// STAGE 1: Discovery - Fetch ALL pairs to learn canonical primary names.
	var allPairsResp AssetPairsResponse
	if err := c.callPublic(ctx, "/0/public/AssetPairs", nil, &allPairsResp); err != nil {
		return fmt.Errorf("failed to call AssetPairs for discovery: %w", err)
	}
	if len(allPairsResp.Error) > 0 {
		return fmt.Errorf("AssetPairs discovery API error: %s", strings.Join(allPairsResp.Error, ", "))
	}

	// Build a lookup map from "BaseQuote" -> "PrimaryName" (e.g., "XXBTZUSD" -> "XBTUSD")
	baseQuoteToPrimaryName := make(map[string]string)
	for primaryName, pairInfo := range allPairsResp.Result {
		lookupKey := pairInfo.Base + pairInfo.Quote
		baseQuoteToPrimaryName[lookupKey] = primaryName
	}

	// STAGE 2: Resolution - Fetch specific data for configured pairs to get links.
	var resolvedPairsResp AssetPairsResponse
	params := url.Values{"pair": {strings.Join(configuredPairs, ",")}}
	if err := c.callPublic(ctx, "/0/public/AssetPairs", params, &resolvedPairsResp); err != nil {
		return fmt.Errorf("failed to call AssetPairs for resolution: %w", err)
	}
	if len(resolvedPairsResp.Error) > 0 {
		return fmt.Errorf("AssetPairs resolution API error: %s", strings.Join(resolvedPairsResp.Error, ", "))
	}

	c.dataMu.Lock()
	defer c.dataMu.Unlock()

	if len(c.assetInfoMap) == 0 {
		return errors.New("asset map not initialized, call RefreshAssets first")
	}

	// Reset all maps before rebuilding
	c.pairInfoMap = allPairsResp.Result // Store the full map
	c.commonToTradeablePair = make(map[string]string)
	c.commonToPrimaryPair = make(map[string]string)
	c.krakenToCommonPair = make(map[string]string)
	c.pairDetailsCache = make(map[string]PairDetail)
	c.commonToKrakenPair = make(map[string]string)
	c.commonToKrakenAsset = make(map[string]string)

	// STAGE 3: Build Maps using resolved data and the canonical lookup map
	for commonPairKey, resolvedPairInfo := range resolvedPairsResp.Result {
		parts := strings.Split(commonPairKey, "/")
		if len(parts) != 2 {
			c.logger.LogWarn("Invalid common pair format in API response, skipping: %s", commonPairKey)
			continue
		}
		commonBase, commonQuote := parts[0], parts[1]

		// Map common asset names to Kraken's internal base names (e.g., "BTC" -> "XXBT")
		c.commonToKrakenAsset[commonBase] = resolvedPairInfo.Base
		c.commonToKrakenAsset[commonQuote] = resolvedPairInfo.Quote

		// Find the true primary name using the lookup map created in Stage 1
		lookupKey := resolvedPairInfo.Base + resolvedPairInfo.Quote
		primaryPairName, ok := baseQuoteToPrimaryName[lookupKey]
		if !ok {
			c.logger.LogWarn("Could not find primary name for pair %s, falling back to altname", commonPairKey)
			primaryPairName = resolvedPairInfo.Altname // Fallback, should be rare
		}

		// Populate all translation maps with the resolved and canonical data
		c.commonToPrimaryPair[commonPairKey] = primaryPairName
		c.commonToTradeablePair[commonPairKey] = resolvedPairInfo.Altname
		c.commonToKrakenPair[commonPairKey] = primaryPairName
		c.krakenToCommonPair[primaryPairName] = commonPairKey
		if resolvedPairInfo.Altname != primaryPairName {
			c.krakenToCommonPair[resolvedPairInfo.Altname] = commonPairKey
		}

		c.pairDetailsCache[primaryPairName] = PairDetail{
			PairDecimals: resolvedPairInfo.PairDecimals,
			LotDecimals:  resolvedPairInfo.LotDecimals,
			OrderMin:     resolvedPairInfo.OrderMin,
		}
		if resolvedPairInfo.Altname != primaryPairName {
			c.pairDetailsCache[resolvedPairInfo.Altname] = c.pairDetailsCache[primaryPairName]
		}
	}
	c.logger.LogInfo("Kraken Client: Refreshed and built canonical maps for %d configured pairs.", len(c.commonToKrakenPair))
	return nil
}

// GetAssetPairsAPI fetches asset pair information for one or all pairs.
func (c *Client) GetAssetPairsAPI(ctx context.Context, pair string) (map[string]AssetPairInfo, error) {
	params := url.Values{}
	if pair != "" {
		params.Set("pair", pair)
	}
	var resp AssetPairsResponse
	if err := c.callPublic(ctx, "/0/public/AssetPairs", params, &resp); err != nil {
		return nil, fmt.Errorf("GetAssetPairsAPI: callPublic failed: %w", err)
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("GetAssetPairsAPI: API error: %s", strings.Join(resp.Error, ", "))
	}
	return resp.Result, nil
}

// ParseOHLCV converts a raw Kraken OHLCV array into a standardized utilities.OHLCVBar struct.
// Kraken's format is typically [time, open, high, low, close, vwap, volume, count].
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
		Timestamp: int64(ts) * 1000, // Convert Unix seconds to milliseconds
		Open:      open, High: high, Low: low, Close: closeVal, Volume: volume,
	}, nil
}

// ParseOrderBook converts a raw Kraken order book response into the standardized broker.OrderBookData struct.
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

// ParseTicker converts a raw Kraken TickerInfo struct into the standardized broker.TickerData struct.
func (c *Client) ParseTicker(info TickerInfo, commonPair string) (broker.TickerData, error) {
	askPrice, err := strconv.ParseFloat(info.Ask[0], 64)
	if err != nil {
		return broker.TickerData{}, fmt.Errorf("ParseTicker: failed to parse ask price: %w", err)
	}
	bidPrice, err := strconv.ParseFloat(info.Bid[0], 64)
	if err != nil {
		return broker.TickerData{}, fmt.Errorf("ParseTicker: failed to parse bid price: %w", err)
	}
	lastPrice, err := strconv.ParseFloat(info.LastTradeClosed[0], 64)
	if err != nil {
		return broker.TickerData{}, fmt.Errorf("ParseTicker: failed to parse last price: %w", err)
	}
	volume24h, err := strconv.ParseFloat(info.Volume[1], 64)
	if err != nil {
		return broker.TickerData{}, fmt.Errorf("ParseTicker: failed to parse 24h volume: %w", err)
	}
	high24h, err := strconv.ParseFloat(info.High[1], 64)
	if err != nil {
		return broker.TickerData{}, fmt.Errorf("ParseTicker: failed to parse 24h high: %w", err)
	}
	low24h, err := strconv.ParseFloat(info.Low[1], 64)
	if err != nil {
		return broker.TickerData{}, fmt.Errorf("ParseTicker: failed to parse 24h low: %w", err)
	}
	open24h, err := strconv.ParseFloat(info.OpenPrice, 64)
	if err != nil {
		return broker.TickerData{}, fmt.Errorf("ParseTicker: failed to parse 24h open: %w", err)
	}

	return broker.TickerData{
		Pair:      commonPair,
		Ask:       askPrice,
		Bid:       bidPrice,
		LastPrice: lastPrice,
		Volume:    volume24h,
		High:      high24h,
		Low:       low24h,
		Open:      open24h,
		Timestamp: time.Now(),
	}, nil
}

// GetAssetInfo retrieves cached asset information for a given Kraken asset name.
// It will trigger a refresh from the API if the asset is not found in the cache.
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

// GetKrakenAssetName translates a common asset name (e.g., "BTC") to its Kraken equivalent (e.g., "XXBT").
// Triggers a cache refresh if the name is not found.
func (c *Client) GetKrakenAssetName(ctx context.Context, commonAssetName string) (string, error) {
	c.dataMu.RLock()
	krakenName, ok := c.commonToKrakenAsset[strings.ToUpper(commonAssetName)]
	c.dataMu.RUnlock()

	if ok {
		return krakenName, nil
	}

	// The map is now built on startup based on configured pairs.
	// A miss here indicates a logic error or a request for an unconfigured asset.
	return "", fmt.Errorf("kraken asset name for '%s' not found in map; ensure it is part of the initial configured pairs", commonAssetName)
}

// GetKrakenAltname retrieves the Kraken altname for a given common symbol by querying the Assets API.
func (c *Client) GetKrakenAltname(ctx context.Context, symbol string) (string, error) {
	params := url.Values{}
	params.Set("asset", strings.ToUpper(symbol))
	var resp struct {
		Error  []string             `json:"error"`
		Result map[string]AssetInfo `json:"result"`
	}
	if err := c.callPublic(ctx, "/0/public/Assets", params, &resp); err != nil {
		return "", err
	}
	if len(resp.Error) > 0 {
		return "", errors.New(strings.Join(resp.Error, ", "))
	}
	if len(resp.Result) != 1 {
		return "", fmt.Errorf("expected 1 asset, got %d for %s", len(resp.Result), symbol)
	}
	for _, info := range resp.Result {
		return info.Altname, nil
	}
	return "", fmt.Errorf("no altname found for %s", symbol)
}

// GetPrimaryKrakenPairName translates a common pair name ("BTC/USD") to Kraken's primary, non-aliased pair name ("XBTUSD").
// This is useful for API endpoints that require the primary pair name.
// GetPrimaryKrakenPairName resolves the canonical (primary) Kraken pair name for a common pair like "BTC/USD".
func (c *Client) GetPrimaryKrakenPairName(ctx context.Context, commonPair string) (string, error) {
	upperPair := strings.ToUpper(commonPair)
	c.dataMu.RLock()
	primary, ok := c.commonToPrimaryPair[upperPair]
	c.dataMu.RUnlock()
	if ok {
		return primary, nil
	}

	// Dynamic fetch for non-configured pairs
	parts := strings.Split(upperPair, "/")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid common pair format: %s", commonPair)
	}
	base, quote := parts[0], parts[1]

	baseAlt, err := c.GetKrakenAltname(ctx, base)
	if err != nil {
		return "", fmt.Errorf("no altname found for base %s: %w", base, err)
	}
	quoteAlt, err := c.GetKrakenAltname(ctx, quote)
	if err != nil {
		return "", fmt.Errorf("no altname found for quote %s: %w", quote, err)
	}

	krakenPairQuery := baseAlt + quoteAlt
	pairs, err := c.GetAssetPairsAPI(ctx, krakenPairQuery)
	if err != nil {
		return "", fmt.Errorf("failed to fetch pair %s: %w", krakenPairQuery, err)
	}
	if len(pairs) == 0 {
		return "", fmt.Errorf("no pair found for %s (%s)", commonPair, krakenPairQuery)
	}

	// Add to maps dynamically
	c.dataMu.Lock()
	c.buildPairMaps(pairs)
	normalized := strings.ToUpper(baseAlt + "/" + quoteAlt)
	primary = c.commonToPrimaryPair[normalized]
	if primary == "" {
		c.dataMu.Unlock()
		return "", fmt.Errorf("failed to resolve primary after build for %s", commonPair)
	}
	if normalized != upperPair {
		c.commonToPrimaryPair[upperPair] = primary
		c.commonToKrakenPair[upperPair] = c.commonToKrakenPair[normalized]
		c.commonToTradeablePair[upperPair] = c.commonToTradeablePair[normalized]
		c.pairDetailsCache[upperPair] = c.pairDetailsCache[normalized]
	}
	c.dataMu.Unlock()

	return primary, nil
}

// GetTradeableKrakenPairName returns the Kraken pair name (e.g., SOLUSD) for a common pair like "SOL/USD".
// This name can be either the `altname` or the primary name, suitable for placing orders.
func (c *Client) GetTradeableKrakenPairName(ctx context.Context, commonPair string) (string, error) {
	c.dataMu.RLock()
	pairID, ok := c.commonToTradeablePair[commonPair]
	c.dataMu.RUnlock()
	if ok {
		return pairID, nil
	}

	return "", fmt.Errorf("tradeable kraken pair for %s not found in map; ensure it is part of the initial configured pairs", commonPair)
}

// GetCommonPairName translates a Kraken pair name (e.g., "XBTUSD") back to its common "BASE/QUOTE" format ("BTC/USD").
func (c *Client) GetCommonPairName(ctx context.Context, krakenPair string) (string, error) {
	c.dataMu.RLock()
	commonPair, ok := c.krakenToCommonPair[krakenPair]
	c.dataMu.RUnlock()
	if !ok {
		return "", fmt.Errorf("common pair name for kraken pair %s not found in map", krakenPair)
	}
	return commonPair, nil
}

// GetCommonAssetName translates a Kraken asset name (e.g., "XXBT") to its common equivalent ("BTC").
// It handles various Kraken naming conventions and triggers a cache refresh if needed.
func (c *Client) GetCommonAssetName(ctx context.Context, krakenAssetName string) (string, error) {
	sanitizedName := strings.Split(krakenAssetName, ".")[0]

	c.dataMu.RLock()
	// Reverse lookup: find which common name maps to this kraken name
	for common, kraken := range c.commonToKrakenAsset {
		if kraken == sanitizedName {
			c.dataMu.RUnlock()
			return common, nil
		}
	}
	c.dataMu.RUnlock()

	// Fallback to iterating assets if needed, though the primary map should be sufficient
	c.dataMu.RLock()
	assetInfo, ok := c.assetInfoMap[sanitizedName]
	c.dataMu.RUnlock()
	if ok {
		return getCommonAssetName(assetInfo), nil
	}

	return "", fmt.Errorf("could not resolve common asset name for Kraken asset %s", krakenAssetName)
}

// GetOrderBookAPI fetches the raw order book data from Kraken's public API.
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
	// The key in the result might be the primary name, even if we query by an alias
	for _, orderBook := range resp.Result {
		return orderBook, nil // Return the first (and only) order book in the response
	}
	return KrakenAPIRawOrderBook{}, fmt.Errorf("order book data for pair %s not found in Kraken response", krakenPairName)
}

// GetOHLCVAPI fetches raw OHLCV data from Kraken's public API.
func (c *Client) GetOHLCVAPI(ctx context.Context, krakenPairName string, intervalMinutes string, sinceTimestamp int64) ([][]interface{}, error) {
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

	for key, data := range resp.Result {
		if key == "last" {
			continue
		}

		// First, assert that the data is a slice of interfaces.
		iSlice, ok := data.([]interface{})
		if !ok {
			c.logger.LogWarn("GetOHLCVAPI: data for key '%s' is not a slice, it's a %T. Skipping.", key, data)
			continue
		}

		// If the slice is empty, it's valid and means no new data. Return an empty result.
		if len(iSlice) == 0 {
			return [][]interface{}{}, nil
		}

		// Now, validate the structure by checking if the *first element* is also a slice.
		// This confirms the expected slice-of-slices structure (e.g., [[...], [...]]).
		_, isSliceOfSlices := iSlice[0].([]interface{})
		if !isSliceOfSlices {
			c.logger.LogWarn("GetOHLCVAPI: data for key '%s' is a slice, but not a slice of slices. First element is %T. Skipping.", key, iSlice[0])
			continue
		}

		// If all checks pass, manually and safely construct the [][]interface{} to return.
		ohlcvSlice := make([][]interface{}, len(iSlice))
		for i, v := range iSlice {
			if innerSlice, ok := v.([]interface{}); ok {
				ohlcvSlice[i] = innerSlice
			} else {
				// This safeguard handles malformed data where not all elements are slices.
				return nil, fmt.Errorf("inconsistent data in OHLCV response for key %s; element %d is not a slice", key, i)
			}
		}
		return ohlcvSlice, nil
	}

	return nil, fmt.Errorf("no valid OHLCV data found for pair %s in API response", krakenPairName)
}

// RefreshPairs fetches the latest asset pair data for the configured pairs and rebuilds the internal pair maps.
func (c *Client) RefreshPairs(ctx context.Context, configuredPairs []string) error {
	c.logger.LogInfo("Kraken Client: Refreshing asset pairs using %v...", configuredPairs)
	pairs, err := c.GetAssetPairsAPI(ctx, "")
	if err != nil {
		return fmt.Errorf("failed to refresh all asset pairs: %w", err)
	}

	c.dataMu.Lock()
	c.buildPairMaps(pairs)
	c.dataMu.Unlock()

	// Fetch specific configured pairs if needed, but since we have all, perhaps filter.
	c.logger.LogInfo("Kraken Client: Refreshed and built canonical maps for %d configured pairs.", len(configuredPairs))
	return nil
}

// GetPairDetail retrieves cached formatting and limit details for a given Kraken pair.
// Triggers a refresh if the details are not found in the cache.
func (c *Client) GetPairDetail(ctx context.Context, krakenPair string) (PairDetail, error) {
	c.dataMu.RLock()
	detail, ok := c.pairDetailsCache[krakenPair]
	c.dataMu.RUnlock()

	if ok {
		return detail, nil
	}

	return PairDetail{}, fmt.Errorf("pair detail for %s not found in map; ensure it is part of the initial configured pairs", krakenPair)
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

// QueryTradesAPI retrieves historical trade data from Kraken's private API.
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

// QueryTradeVolumeAPI retrieves user's trade volume and fee tier information from Kraken.
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
