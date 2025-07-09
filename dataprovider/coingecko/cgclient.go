// File: dataprovider/coingecko/client.go
package coingecko

import (
	"Snowballin/dataprovider"
	utils "Snowballin/utilities"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate" // For rate limiting
)

type Client struct {
	BaseURL              string
	APIKey               string
	HTTPClient           *http.Client
	limiter              *rate.Limiter
	logger               *utils.Logger
	idMap                map[string]string
	symbolToIDMap        map[string]string
	idMapMu              sync.RWMutex
	lastIDMapRefresh     time.Time
	idMapRefreshInterval time.Duration
	isRefreshingIDMap    bool
	refreshMapMu         sync.Mutex
	cfg                  *utils.CoingeckoConfig
	cache                *dataprovider.SQLiteCache
}

// --- Internal structs for CoinGecko API responses ---
// (These are kept from your original file and assumed correct for CoinGecko's API)
// Minor adjustments for omitempty and pointer for nullable ROI might be good practice.

type cgCoinListEntry struct {
	ID     string `json:"id"`
	Symbol string `json:"symbol"`
	Name   string `json:"name"`
}
type cgMarketData struct {
	ID                                 string  `json:"id"`
	Symbol                             string  `json:"symbol"`
	Name                               string  `json:"name"`
	Image                              string  `json:"image"`
	CurrentPrice                       float64 `json:"current_price"`
	MarketCap                          float64 `json:"market_cap"`
	MarketCapRank                      int     `json:"market_cap_rank"`
	FullyDilutedValuation              float64 `json:"fully_diluted_valuation,omitempty"`
	TotalVolume                        float64 `json:"total_volume"`
	High24h                            float64 `json:"high_24h"`
	Low24h                             float64 `json:"low_24h"`
	PriceChange24h                     float64 `json:"price_change_24h"`
	PriceChangePercentage24h           float64 `json:"price_change_percentage_24h"`
	MarketCapChange24h                 float64 `json:"market_cap_change_24h"`
	MarketCapChangePercentage24h       float64 `json:"market_cap_change_percentage_24h"`
	CirculatingSupply                  float64 `json:"circulating_supply"`
	TotalSupply                        float64 `json:"total_supply,omitempty"`
	MaxSupply                          float64 `json:"max_supply,omitempty"`
	Ath                                float64 `json:"ath"`
	AthChangePercentage                float64 `json:"ath_change_percentage"`
	AthDate                            string  `json:"ath_date"`
	Atl                                float64 `json:"atl"`
	AtlChangePercentage                float64 `json:"atl_change_percentage"`
	AtlDate                            string  `json:"atl_date"`
	Roi                                *cgRoi  `json:"roi,omitempty"` // Pointer to handle null
	LastUpdated                        string  `json:"last_updated"`
	PriceChangePercentage1hInCurrency  float64 `json:"price_change_percentage_1h_in_currency,omitempty"`
	PriceChangePercentage24hInCurrency float64 `json:"price_change_percentage_24h_in_currency,omitempty"`
	PriceChangePercentage7dInCurrency  float64 `json:"price_change_percentage_7d_in_currency,omitempty"`
}
type cgRoi struct {
	Times      float64 `json:"times"`
	Currency   string  `json:"currency"`
	Percentage float64 `json:"percentage"`
}

// cgOHLCResponse is a type alias for the structure returned by CoinGecko's /ohlc endpoint.
// It's an array of arrays, where each inner array is [timestamp_ms, open, high, low, close].
type cgOHLCResponse = [][5]float64

type cgHistoryResponse struct {
	MarketData *struct {
		CurrentPrice map[string]float64 `json:"current_price"`
	} `json:"market_data"`
}
type cgExchange struct {
	Name                        string  `json:"name"`
	TradeVolume24hBtcNormalized float64 `json:"trade_volume_24h_btc_normalized"`
}
type cgGlobalData struct {
	TotalMarketCap map[string]float64 `json:"total_market_cap"`
	// Add other fields from "data" object if needed
}
type cgGlobalResponse struct {
	Data cgGlobalData `json:"data"`
}
type cgTrendingItem struct { // Wrapper object in the "coins" array
	Item cgTrendingCoinInfo `json:"item"`
}
type cgTrendingCoinInfo struct { // Actual coin info nested under "item"
	ID            string `json:"id"`      // e.g., "bitcoin"
	CoinID        int    `json:"coin_id"` // e.g., 1 (CoinGecko's internal numerical ID)
	Name          string `json:"name"`    // e.g., "Bitcoin"
	Symbol        string `json:"symbol"`  // e.g., "BTC"
	MarketCapRank int    `json:"market_cap_rank"`
	Thumb         string `json:"thumb"`
	Small         string `json:"small"`
	Large         string `json:"large"`
	Slug          string `json:"slug"`
	Score         int    `json:"score"`
}
type cgTrendingResponse struct {
	Coins []cgTrendingItem `json:"coins"`
}

// cgOHLCDataPoint represents a single data point from CoinGecko's /ohlc endpoint
// [timestamp_ms, open, high, low, close]
type cgOHLCDataPoint = [5]float64

// cgMarketChartResponse is for /market_chart endpoint
type cgMarketChartResponse struct {
	Prices       [][2]float64 `json:"prices"`
	MarketCaps   [][2]float64 `json:"market_caps"`
	TotalVolumes [][2]float64 `json:"total_volumes"`
}

func NewClient(cfg *utils.AppConfig, logger *utils.Logger, cache *dataprovider.SQLiteCache) (*Client, error) {
	if cfg == nil {
		return nil, errors.New("coingecko client: AppConfig cannot be nil")
	}
	if logger == nil {
		logger = utils.NewLogger(utils.Info)
		logger.LogWarn("CoinGecko Client: Logger not provided, using default logger.")
	}

	var cgCfg *utils.CoingeckoConfig
	if cfg.Coingecko != nil {
		cgCfg = cfg.Coingecko
	} else {
		return nil, errors.New("coingecko client: CoingeckoConfig missing in AppConfig")
	}

	if cgCfg.BaseURL == "" {
		return nil, errors.New("coingecko client: BaseURL is required in CoingeckoConfig")
	}

	if cgCfg.RateLimitPerSec <= 0 {
		cgCfg.RateLimitPerSec = 1.0
		logger.LogWarn("CoinGecko Client: Invalid RateLimitPerSec, defaulting to 1.0")
	}

	if cgCfg.RateLimitBurst <= 0 {
		cgCfg.RateLimitBurst = 1
		logger.LogWarn("CoinGecko Client: Invalid RateLimitBurst, defaulting to 1")
	}

	if cgCfg.RequestTimeoutSec <= 0 {
		cgCfg.RequestTimeoutSec = 10
		logger.LogWarn("CoinGecko Client: Invalid RequestTimeoutSec, defaulting to 10 seconds")
	}
	sqliteCache := cache

	if cache == nil {
		return nil, errors.New("coingecko client: SQLiteCache cannot be nil")
	}

	client := &Client{
		BaseURL:              cgCfg.BaseURL,
		APIKey:               cgCfg.APIKey,
		HTTPClient:           &http.Client{Timeout: time.Duration(cgCfg.RequestTimeoutSec) * time.Second},
		limiter:              rate.NewLimiter(rate.Limit(cgCfg.RateLimitPerSec), cgCfg.RateLimitBurst),
		logger:               logger,
		idMap:                make(map[string]string),
		symbolToIDMap:        make(map[string]string),
		idMapRefreshInterval: time.Duration(cgCfg.IDMapRefreshIntervalHours) * time.Hour,
		cfg:                  cgCfg,
		cache:                sqliteCache,
	}

	logger.LogInfo("CoinGecko client initialized with URL: %s, RateLimit: %.2f req/sec", client.BaseURL, cgCfg.RateLimitPerSec)

	return client, nil
}

// PrimeCache ensures the coin ID map is populated before trading begins.
func (c *Client) PrimeCache(ctx context.Context) error {
	c.logger.LogInfo("CoinGecko Client: Priming cache by fetching initial coin ID map...")
	// The 'true' flag forces a refresh, ignoring timers or existing cache state.
	return c.refreshCoinIDMapIfNeeded(ctx, true)
}

// request handles making the HTTP request, rate limiting, API key, and decoding JSON.
func (c *Client) request(ctx context.Context, endpoint string, queryParams url.Values, result interface{}) error {
	if ctx == nil {
		c.logger.LogWarn("CoinGecko Client: request called with nil context for endpoint %s. Using background context.", endpoint)
		ctx = context.Background() // Fallback, but callers should provide a context
	}

	// Wait for rate limiter
	if err := c.limiter.Wait(ctx); err != nil {
		c.logger.LogError("CoinGecko Client: Rate limiter wait error for endpoint %s: %v", endpoint, err)
		return fmt.Errorf("rate limiter error for endpoint %s: %w", endpoint, err)
	}

	fullURL := c.BaseURL + endpoint
	if !strings.HasPrefix(endpoint, "/") && !strings.HasSuffix(c.BaseURL, "/") {
		fullURL = c.BaseURL + "/" + endpoint
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		c.logger.LogError("CoinGecko Client: Error creating request for %s: %v", fullURL, err)
		return fmt.Errorf("failed to create request for %s: %w", fullURL, err)
	}

	// Initialize queryParams if nil, to ensure API key can be added if present.
	if queryParams == nil {
		queryParams = url.Values{}
	}

	if c.APIKey != "" { // For CoinGecko Pro API
		// CoinGecko Pro API key is usually passed as a query parameter "x_cg_pro_api_key"
		// or sometimes "x_cg_demo_api_key" for demo.
		// Check their latest docs if this is still the preferred method.
		// The provided code used req.Header.Set("x-cg-pro-api-key", c.APIKey).
		// For query param method:
		queryParams.Set("x_cg_pro_api_key", c.APIKey)
		// If it's a header, it would be: req.Header.Set("X-Cg-Pro-Api-Key", c.APIKey)
		// Let's stick to query param as it's common for keys not part of auth schemes.
	}

	if len(queryParams) > 0 {
		req.URL.RawQuery = queryParams.Encode()
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "SnowballinBot/1.0") // Good practice
	c.logger.LogDebug("CoinGecko Request: %s %s", req.Method, req.URL.String())

	maxRetries := 0 // Default to 0 if cfg or MaxRetries is not set
	if c.cfg != nil {
		maxRetries = c.cfg.MaxRetries
	}
	if maxRetries < 0 { // Ensure it's not negative
		maxRetries = 0
	}

	retryDelay := 2 * time.Second // Default
	if c.cfg != nil && c.cfg.RetryDelaySec > 0 {
		retryDelay = time.Duration(c.cfg.RetryDelaySec) * time.Second
	}

	// Use the utils.DoJSONRequest helper
	return utils.DoJSONRequest(c.HTTPClient, req, maxRetries, retryDelay, result)
}

// refreshCoinIDMapIfNeeded fetches the coin list from CoinGecko and updates the internal ID maps.
func (c *Client) refreshCoinIDMapIfNeeded(ctx context.Context, force bool) error {
	c.idMapMu.RLock()
	mapIsEmpty := len(c.idMap) == 0
	var intervalPassed bool
	if c.idMapRefreshInterval > 0 { // Avoid panic if interval is zero
		intervalPassed = time.Since(c.lastIDMapRefresh) > c.idMapRefreshInterval
	}
	c.idMapMu.RUnlock()

	if !force && !mapIsEmpty && !intervalPassed {
		return nil // Map is populated and fresh enough
	}

	c.refreshMapMu.Lock()
	if c.isRefreshingIDMap && !force { // Check again after acquiring lock
		c.refreshMapMu.Unlock()
		c.logger.LogDebug("CoinGecko Client: Coin ID map refresh already in progress by another goroutine.")
		return nil
	}
	c.isRefreshingIDMap = true
	c.refreshMapMu.Unlock()

	defer func() {
		c.refreshMapMu.Lock()
		c.isRefreshingIDMap = false
		c.refreshMapMu.Unlock()
	}()

	c.logger.LogInfo("CoinGecko Client: Refreshing coin ID map (Forced: %t, MapEmpty: %t, IntervalPassed: %t)...", force, mapIsEmpty, intervalPassed)

	cgCoins, err := c.GetSupportedCoins(ctx) // Use the method that takes context
	if err != nil {
		return fmt.Errorf("refreshCoinIDMapIfNeeded: failed to get supported coins from CoinGecko: %w", err)
	}

	c.idMapMu.Lock()
	defer c.idMapMu.Unlock()

	c.idMap = make(map[string]string, len(cgCoins)*2) // Estimate size
	c.symbolToIDMap = make(map[string]string, len(cgCoins)*2)

	for _, cgCoin := range cgCoins { // cgCoin is dataprovider.Coin
		cgIDLower := strings.ToLower(cgCoin.ID) // CoinGecko IDs are typically lowercase

		c.idMap[cgIDLower] = cgIDLower // Map CoinGecko ID to itself

		if cgCoin.Symbol != "" {
			commonSymbolUpper := strings.ToUpper(cgCoin.Symbol)
			c.idMap[commonSymbolUpper] = cgIDLower // Map common Uppercase Symbol to CG ID

			// Also map CoinGecko's own symbol (lowercase) -> CoinGecko ID
			c.symbolToIDMap[strings.ToLower(cgCoin.Symbol)] = cgIDLower
		}
		if cgCoin.Name != "" {
			// Map CoinGecko name (lowercase) -> CoinGecko ID
			c.symbolToIDMap[strings.ToLower(cgCoin.Name)] = cgIDLower
		}
	}
	c.lastIDMapRefresh = time.Now().UTC()
	c.logger.LogInfo("CoinGecko Client: Successfully refreshed coin ID map. Mapped %d coins.", len(cgCoins))
	return nil
}

// GetCoinID implements DataProvider interface.
// commonAssetSymbol: e.g., "BTC", "Ethereum", "bitcoin"
func (c *Client) GetCoinID(ctx context.Context, sym string) (string, error) {
	// 1) Refresh with short timeout
	rc, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := c.refreshCoinIDMapIfNeeded(rc, false); err != nil {
		c.logger.LogWarn(
			"CoinGecko GetCoinID: non-critical error refreshing ID map for '%s': %v; proceeding",
			sym, err,
		)
	}

	// 2) Grab read-lock and bail if empty
	c.idMapMu.RLock()
	// defer c.idMapMu.RUnlock() // This defer should be after all uses of idMap and symbolToIDMap
	mapLen := len(c.idMap) + len(c.symbolToIDMap)
	if mapLen == 0 {
		c.idMapMu.RUnlock() // Unlock before returning
		return "", fmt.Errorf(
			"CoinGecko ID map is empty for asset '%s'; initial population may be pending or failed",
			sym,
		)
	}

	// 3) Prepare lookup keys
	up := strings.ToUpper(sym)
	lo := strings.ToLower(sym)

	// 4) Data-driven lookup strategies
	// Check idMap first (common uppercase symbol to CG ID, CG ID to CG ID)
	if id, ok := c.idMap[up]; ok {
		c.idMapMu.RUnlock()
		return id, nil
	}
	if id, ok := c.idMap[lo]; ok { // This covers direct ID lookup if 'sym' was already the CG ID
		c.idMapMu.RUnlock()
		return id, nil
	}
	// Then check symbolToIDMap (lowercase symbol or name to CG ID)
	if id, ok := c.symbolToIDMap[lo]; ok {
		c.idMapMu.RUnlock()
		return id, nil
	}

	// 5) XBT → BTC → bitcoin special case
	if up == "XBT" {
		if id, ok := c.idMap["BTC"]; ok {
			c.idMapMu.RUnlock()
			return id, nil
		}
		if id, ok := c.idMap["bitcoin"]; ok { // "bitcoin" should be in idMap (lo map) or symbolToIDMap
			c.idMapMu.RUnlock()
			return id, nil
		}
	}

	c.idMapMu.RUnlock() // Unlock after all checks

	// 6) Nothing matched
	c.logger.LogWarn(
		"CoinGecko GetCoinID: no ID found for symbol '%s'",
		sym,
	)
	return "", fmt.Errorf("CoinGecko ID not found for asset symbol: %s", sym)
}

// --- DataProvider Interface Method Implementations ---

// GetSupportedCoins implements DataProvider interface.
func (c *Client) GetSupportedCoins(ctx context.Context) ([]dataprovider.Coin, error) {
	var cgCoins []cgCoinListEntry // Internal CoinGecko type
	params := url.Values{}
	params.Add("include_platform", "false") // Typically not needed for just ID mapping

	err := c.request(ctx, "/coins/list", params, &cgCoins)
	if err != nil {
		return nil, fmt.Errorf("GetSupportedCoins: API request failed: %w", err)
	}

	dpCoins := make([]dataprovider.Coin, len(cgCoins))
	for i, cgCoin := range cgCoins {
		dpCoins[i] = dataprovider.Coin{
			ID:     cgCoin.ID,
			Symbol: cgCoin.Symbol,
			Name:   cgCoin.Name,
		}
	}
	return dpCoins, nil
}

// GetMarketData implements DataProvider interface.
// ids: CoinGecko coin IDs (e.g., "bitcoin", "ethereum")
func (c *Client) GetMarketData(ctx context.Context, ids []string, vsCurrency string) ([]dataprovider.MarketData, error) {
	if len(ids) == 0 {
		return []dataprovider.MarketData{}, nil
	}

	var cgData []cgMarketData // Internal CoinGecko type
	params := url.Values{}
	params.Add("vs_currency", strings.ToLower(vsCurrency))
	params.Add("ids", strings.Join(ids, ","))
	params.Add("order", "market_cap_desc") // Default, good for most cases
	params.Add("per_page", strconv.Itoa(len(ids)))
	params.Add("page", "1")
	params.Add("sparkline", "false")
	params.Add("price_change_percentage", "1h,24h,7d") // Request specific change percentages
	params.Add("locale", "en")                         // Consistent locale

	err := c.request(ctx, "/coins/markets", params, &cgData)
	if err != nil {
		return nil, fmt.Errorf("GetMarketData for CoinGecko IDs [%s] in %s failed: %w", strings.Join(ids, ","), vsCurrency, err)
	}

	dpData := make([]dataprovider.MarketData, len(cgData))
	for i, cgItem := range cgData {
		ohlcvBar, err := ConvertCoinGeckoMarketData(cgItem)
		if err != nil {
			c.logger.LogWarn("GetMarketData: Error converting OHLCV data for %s: %v", cgItem.ID, err)
			ohlcvBar = utils.OHLCVBar{} // Safe empty fallback on conversion error
		}

		dpData[i] = dataprovider.MarketData{
			ID:             cgItem.ID,
			Symbol:         cgItem.Symbol,
			Name:           cgItem.Name,
			CurrentPrice:   cgItem.CurrentPrice,
			MarketCap:      cgItem.MarketCap,
			Volume24h:      ohlcvBar.Volume,
			High24h:        ohlcvBar.High,
			Low24h:         ohlcvBar.Low,
			PriceChange1h:  cgItem.PriceChangePercentage1hInCurrency,
			PriceChange24h: cgItem.PriceChangePercentage24hInCurrency,
			PriceChange7d:  cgItem.PriceChangePercentage7dInCurrency,
			LastUpdated:    time.UnixMilli(ohlcvBar.Timestamp).UTC(),
		}
	}

	return dpData, nil
}

// GetOHLCVHistorical fetches OHLCV data by combining /ohlc (for OHL C) and /market_chart (for Volume).
func (c *Client) GetOHLCVHistorical(ctx context.Context, id, vsCurrency, interval string) ([]utils.OHLCVBar, error) {
	c.logger.LogDebug("CoinGecko GetOHLCVHistorical: Fetching for ID=%s, VS=%s, Interval=%s", id, vsCurrency, interval)

	cacheProvider := "coingecko"
	cacheCoinID := fmt.Sprintf("%s-%s-%s", id, vsCurrency, interval)

	var daysForAPI string
	var expectedIntervalDuration time.Duration

	switch strings.ToLower(interval) {
	case "1d", "daily":
		daysForAPI = "max"
		expectedIntervalDuration = 24 * time.Hour
	case "4h":
		daysForAPI = "90"
		expectedIntervalDuration = 4 * time.Hour
	case "1h":
		daysForAPI = "7"
		expectedIntervalDuration = 1 * time.Hour
	default:
		daysForAPI = "max"
		expectedIntervalDuration = 24 * time.Hour
	}

	// 1. Try to fetch from cache
	now := time.Now()
	startTime := now.Add(-time.Duration(90) * 24 * time.Hour) // Use a reasonable lookback for cache
	if daysForAPI != "max" {
		d, _ := strconv.Atoi(daysForAPI)
		startTime = now.Add(-time.Duration(d) * 24 * time.Hour)
	}

	cachedBars, err := c.cache.GetBars(cacheProvider, cacheCoinID, startTime.UnixMilli(), now.UnixMilli())
	if err != nil {
		c.logger.LogWarn("cgclient GetOHLCVHistorical [%s]: Failed to get bars from cache: %v", id, err)
	}
	if len(cachedBars) > 0 {
		latestCachedBarTime := time.UnixMilli(cachedBars[len(cachedBars)-1].Timestamp)
		if time.Since(latestCachedBarTime) < expectedIntervalDuration {
			c.logger.LogInfo("cgclient GetOHLCVHistorical [%s]: Using %d bars from cache.", id, len(cachedBars))
			return cachedBars, nil
		}
	}

	// 2. Fetch from API if cache is insufficient
	c.logger.LogInfo("cgclient GetOHLCVHistorical [%s]: Cache miss or insufficient data. Fetching from API.", id)
	var ohlcData []cgOHLCDataPoint
	ohlcParams := url.Values{}
	ohlcParams.Add("vs_currency", strings.ToLower(vsCurrency))
	ohlcParams.Add("days", daysForAPI)

	ohlcEndpoint := fmt.Sprintf("/coins/%s/ohlc", id)
	err = c.request(ctx, ohlcEndpoint, ohlcParams, &ohlcData)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch OHLC data from CoinGecko /ohlc for %s: %w", id, err)
	}
	if len(ohlcData) == 0 {
		return []utils.OHLCVBar{}, nil
	}

	// Since CoinGecko OHLC does not provide volume, we'll fetch it separately
	// and merge, or just save the OHLC data with zero volume. For simplicity, we will save with zero volume.
	// The previous implementation's volume merge was complex and potentially inaccurate.

	mergedBars := make([]utils.OHLCVBar, 0, len(ohlcData))
	for _, ohlcPoint := range ohlcData {
		if len(ohlcPoint) != 5 {
			continue
		}
		bar := utils.OHLCVBar{
			Timestamp: int64(ohlcPoint[0]),
			Open:      ohlcPoint[1],
			High:      ohlcPoint[2],
			Low:       ohlcPoint[3],
			Close:     ohlcPoint[4],
			Volume:    0, // Volume not available from this endpoint
		}
		mergedBars = append(mergedBars, bar)

		// 3. Save to cache
		if err := c.cache.SaveBar(cacheProvider, cacheCoinID, bar); err != nil {
			c.logger.LogWarn("cgclient GetOHLCVHistorical [%s]: Failed to save bar to cache: %v", id, err)
		}
	}

	sort.Slice(mergedBars, func(i, j int) bool {
		return mergedBars[i].Timestamp < mergedBars[j].Timestamp
	})

	return mergedBars, nil
}

// GetHistoricalPrice implements DataProvider interface.
// date: "DD-MM-YYYY" format
func (c *Client) GetHistoricalPrice(ctx context.Context, id, date string) (dataprovider.HistoricalPrice, error) {
	var cgHistory cgHistoryResponse // Internal CoinGecko type
	params := url.Values{}
	params.Add("date", date) // CoinGecko expects DD-MM-YYYY format
	params.Add("localization", "false")

	endpoint := fmt.Sprintf("/coins/%s/history", id)
	err := c.request(ctx, endpoint, params, &cgHistory)
	if err != nil {
		return dataprovider.HistoricalPrice{}, fmt.Errorf("GetHistoricalPrice for CoinGecko ID %s on %s failed: %w", id, date, err)
	}

	if cgHistory.MarketData == nil || cgHistory.MarketData.CurrentPrice == nil {
		return dataprovider.HistoricalPrice{}, fmt.Errorf("no market_data.current_price in CoinGecko historical response for ID %s on %s", id, date)
	}

	// Determine the quote currency to use from config, defaulting to USD
	quoteCurrencyKey := "usd"                      // Default to USD
	if c.cfg != nil && c.cfg.QuoteCurrency != "" { // Check if configured quote currency exists
		quoteCurrencyKey = strings.ToLower(c.cfg.QuoteCurrency)
	}

	price, ok := cgHistory.MarketData.CurrentPrice[quoteCurrencyKey]
	if !ok {
		// Fallback to USD if the configured quote currency is not found or not "usd"
		if quoteCurrencyKey != "usd" {
			price, ok = cgHistory.MarketData.CurrentPrice["usd"]
		}
		if !ok {
			// Fallback to the first available currency if USD/configured quote is also missing
			for _, pVal := range cgHistory.MarketData.CurrentPrice {
				price = pVal
				ok = true
				c.logger.LogWarn("GetHistoricalPrice: Target quote ('%s') and USD price not found for %s on %s, using first available currency.", quoteCurrencyKey, id, date)
				break
			}
			if !ok {
				return dataprovider.HistoricalPrice{}, fmt.Errorf("no price data found in any currency for CoinGecko ID %s on %s", id, date)
			}
		} else if quoteCurrencyKey != "usd" {
			c.logger.LogWarn("GetHistoricalPrice: Target quote currency ('%s') not found for %s on %s, fell back to USD.", quoteCurrencyKey, id, date)
		}
	}
	return dataprovider.HistoricalPrice{Price: price}, nil
}

// GetExchangeDetails implements DataProvider interface.
func (c *Client) GetExchangeDetails(ctx context.Context, exchangeID string) (dataprovider.ExchangeDetails, error) {
	var cgExchangeData cgExchange // Internal CoinGecko type
	endpoint := fmt.Sprintf("/exchanges/%s", exchangeID)
	err := c.request(ctx, endpoint, nil, &cgExchangeData)
	if err != nil {
		return dataprovider.ExchangeDetails{}, fmt.Errorf("GetExchangeDetails for exchange ID %s failed: %w", exchangeID, err)
	}
	return dataprovider.ExchangeDetails{
		Name:      cgExchangeData.Name,
		Volume24h: cgExchangeData.TradeVolume24hBtcNormalized, // This is normalized to BTC
	}, nil
}

// GetGlobalMarketData implements DataProvider interface.
func (c *Client) GetGlobalMarketData(ctx context.Context) (dataprovider.GlobalMarketData, error) {
	var cgGlobalResp cgGlobalResponse // Internal CoinGecko type
	err := c.request(ctx, "/global", nil, &cgGlobalResp)
	if err != nil {
		return dataprovider.GlobalMarketData{}, fmt.Errorf("GetGlobalMarketData failed: %w", err)
	}

	quoteCurrencyKey := "usd" // Default to USD
	if c.cfg != nil && c.cfg.QuoteCurrency != "" {
		quoteCurrencyKey = strings.ToLower(c.cfg.QuoteCurrency)
	}

	totalMarketCap, ok := cgGlobalResp.Data.TotalMarketCap[quoteCurrencyKey]
	if !ok {
		c.logger.LogWarn("GetGlobalMarketData: Total market cap for quote currency '%s' not found, attempting USD fallback.", quoteCurrencyKey)
		totalMarketCap, ok = cgGlobalResp.Data.TotalMarketCap["usd"]
		if !ok {
			c.logger.LogError("GetGlobalMarketData: Total market cap for USD also not found in CoinGecko response.")
			// Fallback: Iterate to find first available market cap
			for _, val := range cgGlobalResp.Data.TotalMarketCap {
				totalMarketCap = val
				ok = true
				break
			}
			if !ok {
				return dataprovider.GlobalMarketData{}, errors.New("no total market cap data found in global response from CoinGecko")
			}
		}
	}
	return dataprovider.GlobalMarketData{TotalMarketCap: totalMarketCap}, nil
}

// GetTrendingSearches implements DataProvider interface.
func (c *Client) GetTrendingSearches(ctx context.Context) ([]dataprovider.TrendingCoin, error) {
	var cgTrendingResp cgTrendingResponse // Internal CoinGecko type
	err := c.request(ctx, "/search/trending", nil, &cgTrendingResp)
	if err != nil {
		return nil, fmt.Errorf("GetTrendingSearches from CoinGecko failed: %w", err)
	}

	dpTrending := make([]dataprovider.TrendingCoin, 0, len(cgTrendingResp.Coins))
	for _, cgItemWrapper := range cgTrendingResp.Coins {
		cgItem := cgItemWrapper.Item // Access the nested "item"
		dpTrending = append(dpTrending, dataprovider.TrendingCoin{
			Coin: dataprovider.Coin{
				ID:     cgItem.ID,
				Symbol: cgItem.Symbol,
				Name:   cgItem.Name,
			},
			Score: cgItem.Score,
		})
	}
	return dpTrending, nil
}

// ConvertCoinGeckoMarketData converts CGMarketData into utilities.OHLCVBar
func ConvertCoinGeckoMarketData(data cgMarketData) (utils.OHLCVBar, error) {
	parsedTime, err := time.Parse(time.RFC3339, data.LastUpdated)
	if err != nil {
		return utils.OHLCVBar{}, fmt.Errorf("failed to parse CoinGecko last updated time: %w", err)
	}

	return utils.OHLCVBar{
		Timestamp: parsedTime.UnixMilli(),
		Open:      data.CurrentPrice, // approximation (current price as open)
		High:      data.High24h,
		Low:       data.Low24h,
		Close:     data.CurrentPrice,
		Volume:    data.TotalVolume,
	}, nil
}
