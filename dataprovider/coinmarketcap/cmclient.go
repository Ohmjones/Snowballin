// File: dataprovider/coinmarketcap/cmclient.go
package coinmarketcap

import (
	"Snowballin/dataprovider"
	utils "Snowballin/utilities"
	"context"
	"encoding/json"
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

// --- Constants ---
const (
	// Supported intervals for historical OHLCV data
	Interval1h = "1h"
	Interval4h = "4h"
	Interval1d = "1d"
	Interval7d = "7d"

	providerName = "CoinMarketCap" // For SQLite cache provider key
)

// --- Type Declarations ---
type Client struct {
	BaseURL                string
	APIKeyHeaderName       string
	APIKey                 string
	HTTPClient             *http.Client
	limiter                *rate.Limiter
	logger                 *utils.Logger
	cache                  *dataprovider.SQLiteCache
	cfg                    *utils.CoinmarketcapConfig
	idMapMu                sync.RWMutex
	symbolToNumericalIDMap map[string]string
	nameToNumericalIDMap   map[string]string
	numericalIDToSymbolMap map[string]string
	lastIDMapRefresh       time.Time
	idMapRefreshInterval   time.Duration
	isRefreshingIDMap      bool
	refreshMapMu           sync.Mutex
	sourceBarsCache        sync.Map
}

// --- CMC API Response Structs ---
type cmcStatus struct {
	Timestamp    string `json:"timestamp"`
	ErrorCode    int    `json:"error_code"`
	ErrorMessage string `json:"error_message,omitempty"`
}
type cmcMapEntry struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Symbol   string `json:"symbol"`
	IsActive int    `json:"is_active"`
}
type cmcMapResponse struct {
	Data   []cmcMapEntry `json:"data"`
	Status cmcStatus     `json:"status"`
}
type cmcOHLCVHistoricalResponse struct {
	Status cmcStatus       `json:"status"`
	Data   json.RawMessage `json:"data"` // Use RawMessage to handle flexible response types
}

type cmcOHLCData struct {
	ID     interface{}     `json:"id"`
	Name   string          `json:"name"`
	Symbol string          `json:"symbol"`
	Quotes []cmcOHLCVQuote `json:"quotes"`
}
type cmcOHLCVQuote struct {
	TimeOpen string                 `json:"time_open"`
	QuoteMap map[string]cmcOHLCVSet `json:"quote"`
}
type cmcOHLCVSet struct {
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
	Timestamp string  `json:"timestamp"`
}
type cmcQuotesLatestResponse struct {
	Data   map[string]cmcQuoteData `json:"data"`
	Status cmcStatus               `json:"status"`
}
type cmcQuoteData struct {
	ID          int                      `json:"id"`
	Name        string                   `json:"name"`
	Symbol      string                   `json:"symbol"`
	LastUpdated string                   `json:"last_updated"`
	Quote       map[string]cmcPriceQuote `json:"quote"`
}
type cmcPriceQuote struct {
	Price            float64 `json:"price"`
	Volume24h        float64 `json:"volume_24h"`
	PercentChange1h  float64 `json:"percent_change_1h"`
	PercentChange24h float64 `json:"percent_change_24h"`
	PercentChange7d  float64 `json:"percent_change_7d"`
	MarketCap        float64 `json:"market_cap"`
	LastUpdated      string  `json:"last_updated"`
}
type cmcGlobalMetricsResponse struct {
	Data   cmcGlobalMetricsData `json:"data"`
	Status cmcStatus            `json:"status"`
}
type cmcGlobalMetricsData struct {
	BtcDominance float64                   `json:"btc_dominance"`
	Quote        map[string]cmcGlobalQuote `json:"quote"`
}
type cmcGlobalQuote struct {
	TotalMarketCap float64 `json:"total_market_cap"`
}

// --- Constructor ---
func NewClient(appCfg *utils.AppConfig, logger *utils.Logger, sqliteCache *dataprovider.SQLiteCache) (*Client, error) {
	if appCfg == nil || appCfg.Coinmarketcap == nil {
		return nil, errors.New("coinmarketcap client: AppConfig or CoinmarketcapConfig cannot be nil")
	}
	cfg := appCfg.Coinmarketcap

	if logger == nil {
		return nil, errors.New("coinmarketcap client: logger cannot be nil")
	}
	if cfg.BaseURL == "" {
		return nil, errors.New("coinmarketcap client: BaseURL is required")
	}
	if sqliteCache == nil {
		return nil, errors.New("coinmarketcap client: SQLiteCache cannot be nil")
	}

	if cfg.APIKey == "" {
		logger.LogWarn("CoinMarketCap Client: API key is empty. Functionality will be limited.")
	}

	// Default settings
	rateLimit := rate.Limit(1)
	if cfg.RateLimitPerSec > 0 {
		rateLimit = rate.Limit(cfg.RateLimitPerSec)
	}
	burst := 1
	if cfg.RateLimitBurst > 0 {
		burst = cfg.RateLimitBurst
	}
	timeout := 15 * time.Second
	if cfg.RequestTimeoutSec > 0 {
		timeout = time.Duration(cfg.RequestTimeoutSec) * time.Second
	}
	refreshInterval := 24 * time.Hour
	if cfg.IDMapRefreshIntervalHours > 0 {
		refreshInterval = time.Duration(cfg.IDMapRefreshIntervalHours) * time.Hour
	}

	client := &Client{
		BaseURL:                cfg.BaseURL,
		APIKeyHeaderName:       "X-CMC_PRO_API_KEY",
		APIKey:                 cfg.APIKey,
		HTTPClient:             &http.Client{Timeout: timeout},
		limiter:                rate.NewLimiter(rateLimit, burst),
		logger:                 logger,
		cache:                  sqliteCache,
		cfg:                    cfg,
		symbolToNumericalIDMap: make(map[string]string),
		nameToNumericalIDMap:   make(map[string]string),
		numericalIDToSymbolMap: make(map[string]string),
		idMapRefreshInterval:   refreshInterval,
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := client.refreshCoinIDMapIfNeeded(ctx, true); err != nil {
			client.logger.LogError("CoinMarketCap Client: Initial coin ID map refresh failed: %v", err)
		}
	}()

	// --- DEBUGGING LINE ADDED HERE ---
	logger.LogInfo("CoinMarketCap client initialized with symbol overrides: %v", cfg.SymbolOverrides)

	logger.LogInfo("CoinMarketCap client initialized.")
	return client, nil
}

// --- API Call Helper ---
func (c *Client) makeAPICall(ctx context.Context, endpoint string, params url.Values, result interface{}) error {
	if err := c.limiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter wait error for %s: %w", endpoint, err)
	}

	fullURLStr := c.BaseURL + endpoint
	parsedURL, err := url.Parse(fullURLStr)
	if err != nil {
		return fmt.Errorf("cmc: bad url %s: %w", fullURLStr, err)
	}
	parsedURL.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsedURL.String(), nil)
	if err != nil {
		return fmt.Errorf("cmc: create request for %s: %w", parsedURL.String(), err)
	}

	req.Header.Set("Accept", "application/json")
	if c.APIKey != "" {
		req.Header.Set(c.APIKeyHeaderName, c.APIKey)
	}
	req.Header.Set("User-Agent", "SnowballinBot/1.0")

	c.logger.LogDebug("CMC Request: %s %s", req.Method, req.URL.String())

	maxRetries := c.cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}
	retryDelay := time.Duration(c.cfg.RetryDelaySec) * time.Second
	if retryDelay <= 0 {
		retryDelay = 5 * time.Second
	}

	return utils.DoJSONRequest(c.HTTPClient, req, maxRetries, retryDelay, result)
}

// --- ID Mapping ---
func (c *Client) refreshCoinIDMapIfNeeded(ctx context.Context, force bool) error {
	c.idMapMu.RLock()
	mapIsEmpty := len(c.symbolToNumericalIDMap) == 0
	intervalPassed := time.Since(c.lastIDMapRefresh) > c.idMapRefreshInterval
	c.idMapMu.RUnlock()

	if !force && !mapIsEmpty && !intervalPassed {
		return nil
	}

	c.refreshMapMu.Lock()
	if c.isRefreshingIDMap && !force {
		c.refreshMapMu.Unlock()
		return nil
	}
	c.isRefreshingIDMap = true
	c.refreshMapMu.Unlock()

	defer func() {
		c.refreshMapMu.Lock()
		c.isRefreshingIDMap = false
		c.refreshMapMu.Unlock()
	}()

	c.logger.LogInfo("CoinMarketCap Client: Refreshing coin ID map...")
	var response cmcMapResponse
	params := url.Values{"listing_status": {"active"}}

	err := c.makeAPICall(ctx, "/v1/cryptocurrency/map", params, &response)
	if err != nil {
		return fmt.Errorf("refreshCoinIDMapIfNeeded: API call failed: %w", err)
	}
	if response.Status.ErrorCode != 0 {
		return fmt.Errorf("refreshCoinIDMapIfNeeded: API error: %s", response.Status.ErrorMessage)
	}

	c.idMapMu.Lock()
	defer c.idMapMu.Unlock()

	c.symbolToNumericalIDMap = make(map[string]string)
	c.nameToNumericalIDMap = make(map[string]string)
	c.numericalIDToSymbolMap = make(map[string]string)

	for _, entry := range response.Data {
		if entry.IsActive == 1 {
			idStr := strconv.Itoa(entry.ID)
			c.symbolToNumericalIDMap[strings.ToLower(entry.Symbol)] = idStr
			c.nameToNumericalIDMap[strings.ToLower(entry.Name)] = idStr
			c.numericalIDToSymbolMap[idStr] = entry.Symbol
		}
	}
	c.lastIDMapRefresh = time.Now().UTC()
	c.logger.LogInfo("CoinMarketCap Client: Coin ID map refreshed with %d active coins.", len(c.symbolToNumericalIDMap))
	return nil
}

func (c *Client) GetCoinID(ctx context.Context, commonAssetSymbol string) (string, error) {
	// --- START OF FIX ---
	// Step 1: Check for a manual override first.
	if c.cfg.SymbolOverrides != nil {
		if id, ok := c.cfg.SymbolOverrides[strings.ToLower(commonAssetSymbol)]; ok {
			c.logger.LogDebug("GetCoinID: Using manual override for %s -> %s", commonAssetSymbol, id)
			return id, nil
		}
	}
	// --- END OF FIX ---

	// Step 2: If no override, proceed with the normal lookup.
	if err := c.refreshCoinIDMapIfNeeded(ctx, false); err != nil {
		c.logger.LogWarn("CMC GetCoinID: Non-critical error refreshing ID map: %v", err)
	}

	c.idMapMu.RLock()
	defer c.idMapMu.RUnlock()

	lookupKey := strings.ToLower(commonAssetSymbol)
	if id, ok := c.symbolToNumericalIDMap[lookupKey]; ok {
		return id, nil
	}
	if id, ok := c.nameToNumericalIDMap[lookupKey]; ok {
		return id, nil
	}

	return "", fmt.Errorf("CMC ID not found for asset: %s", commonAssetSymbol)
}

// --- DataProvider Interface Implementations ---

func (c *Client) PrimeCache(ctx context.Context) error {
	c.logger.LogInfo("CoinMarketCap Client: Priming cache by fetching initial coin ID map...")
	return c.refreshCoinIDMapIfNeeded(ctx, true)
}

func (c *Client) GetSupportedCoins(ctx context.Context) ([]dataprovider.Coin, error) {
	if err := c.refreshCoinIDMapIfNeeded(ctx, false); err != nil {
		return nil, fmt.Errorf("failed to refresh ID map for GetSupportedCoins: %w", err)
	}

	c.idMapMu.RLock()
	defer c.idMapMu.RUnlock()

	coins := make([]dataprovider.Coin, 0, len(c.symbolToNumericalIDMap))
	for symbol, id := range c.symbolToNumericalIDMap {
		// This is simplified; a full implementation would need to find the name as well.
		coins = append(coins, dataprovider.Coin{ID: id, Symbol: strings.ToUpper(symbol), Name: symbol})
	}
	return coins, nil
}

func (c *Client) GetMarketData(ctx context.Context, numericalIDs []string, vsCurrency string) ([]dataprovider.MarketData, error) {
	if len(numericalIDs) == 0 {
		return []dataprovider.MarketData{}, nil
	}
	vsCurrency = strings.ToUpper(vsCurrency)
	params := url.Values{"id": {strings.Join(numericalIDs, ",")}, "convert": {vsCurrency}}

	var response cmcQuotesLatestResponse
	if err := c.makeAPICall(ctx, "/v1/cryptocurrency/quotes/latest", params, &response); err != nil {
		return nil, err
	}
	if response.Status.ErrorCode != 0 {
		return nil, fmt.Errorf("CMC API error: %s", response.Status.ErrorMessage)
	}

	marketData := make([]dataprovider.MarketData, 0, len(response.Data))
	for _, data := range response.Data {
		quote, ok := data.Quote[vsCurrency]
		if !ok {
			continue
		}
		lastUpdated, _ := time.Parse(time.RFC3339, quote.LastUpdated)
		marketData = append(marketData, dataprovider.MarketData{
			ID:             strconv.Itoa(data.ID),
			Symbol:         data.Symbol,
			Name:           data.Name,
			CurrentPrice:   quote.Price,
			MarketCap:      quote.MarketCap,
			Volume24h:      quote.Volume24h,
			PriceChange1h:  quote.PercentChange1h,
			PriceChange24h: quote.PercentChange24h,
			PriceChange7d:  quote.PercentChange7d,
			LastUpdated:    lastUpdated,
		})
	}
	return marketData, nil
}

func (c *Client) GetOHLCVHistorical(ctx context.Context, id, vsCurrency, userInterval string) ([]utils.OHLCVBar, error) {
	c.logger.LogDebug("CMC GetOHLCVHistorical: Fetching for ID=%s, VS=%s, Interval=%s", id, vsCurrency, userInterval)

	// Determine if the requested interval is supported. If not, find the best substitute.
	cmcInterval, isSupported := mapToCMCInterval(userInterval)
	sourceInterval := cmcInterval
	if !isSupported {
		sourceInterval = getBestSourceInterval(userInterval)
		c.logger.LogWarn("[ADAPTIVE FETCH] CMC: Unsupported interval '%s'. Will fetch '%s' and resample.", userInterval, sourceInterval)
	}

	// For unsupported, use shared source fetch to avoid multiple API calls
	var sourceBars []utils.OHLCVBar
	var fetchErr error
	if !isSupported {
		cacheKey := fmt.Sprintf("%s-%s", id, vsCurrency)
		if cached, ok := c.sourceBarsCache.Load(cacheKey); ok {
			sourceBars = cached.([]utils.OHLCVBar)
			c.logger.LogInfo("CMC GetOHLCVHistorical: Reusing shared '1h' source bars from session cache for %s.", cacheKey)
		} else {
			sourceBars, fetchErr = c.fetchBarsFromAPI(ctx, id, vsCurrency, sourceInterval)
			if fetchErr != nil {
				return nil, fmt.Errorf("failed to fetch shared source bars for ID %s (%s): %w", id, sourceInterval, fetchErr)
			}
			c.sourceBarsCache.Store(cacheKey, sourceBars)
		}
	} else {
		// For supported intervals, fetch normally
		sourceBars, fetchErr = c.fetchBarsFromAPI(ctx, id, vsCurrency, sourceInterval)
		if fetchErr != nil {
			return nil, fmt.Errorf("failed to fetch bars for ID %s (%s): %w", id, sourceInterval, fetchErr)
		}
	}

	// If the requested interval was not supported, resample the fetched data.
	if !isSupported {
		resampledBars, resampleErr := resampleBars(sourceBars, userInterval, c.logger)
		if resampleErr != nil {
			return nil, fmt.Errorf("failed to resample bars for ID %s from %s to %s: %w", id, sourceInterval, userInterval, resampleErr)
		}
		return resampledBars, nil
	}

	return sourceBars, nil
}

// fetchBarsFromAPI handles the actual API call and caching logic.
func (c *Client) fetchBarsFromAPI(ctx context.Context, numericalCoinID, vsCurrency, interval string) ([]utils.OHLCVBar, error) {
	c.idMapMu.RLock()
	assetSymbol, symbolFound := c.numericalIDToSymbolMap[numericalCoinID]
	c.idMapMu.RUnlock()
	if !symbolFound {
		// Fallback for logging purposes if the map isn't populated yet.
		assetSymbol = "unknown"
	}

	// --- START OF FIX ---
	// Create a unique cache key that includes the interval, just like the coingecko client.
	cacheKey := fmt.Sprintf("%s-%s-%s", assetSymbol, vsCurrency, interval)
	// --- END OF FIX ---

	var lookbackPeriodDays int
	var countForAPI string
	var expectedIntervalDuration time.Duration

	switch interval {
	case Interval1d:
		lookbackPeriodDays = 90
		countForAPI = strconv.Itoa(lookbackPeriodDays)
		expectedIntervalDuration = 24 * time.Hour
	case Interval4h:
		lookbackPeriodDays = 30
		countForAPI = strconv.Itoa(lookbackPeriodDays * 6)
		expectedIntervalDuration = 4 * time.Hour
	case Interval1h:
		lookbackPeriodDays = 7
		countForAPI = strconv.Itoa(lookbackPeriodDays * 24)
		expectedIntervalDuration = 1 * time.Hour
	default:
		return nil, fmt.Errorf("internal error: fetchBarsFromAPI called with unhandled interval %s", interval)
	}

	now := time.Now().UTC()
	cacheStartTime := now.AddDate(0, 0, -lookbackPeriodDays)

	if c.cache != nil {
		// --- MODIFIED LINE: Use the new cacheKey ---
		cachedBars, errCache := c.cache.GetBars(providerName, cacheKey, cacheStartTime.UnixMilli(), now.UnixMilli())
		if errCache == nil && len(cachedBars) > 0 {
			latestCachedBarTime := time.UnixMilli(cachedBars[len(cachedBars)-1].Timestamp).UTC()
			if now.Sub(latestCachedBarTime) < 2*expectedIntervalDuration {
				// --- MODIFIED LINE: Use the new cacheKey in the log message for clarity ---
				c.logger.LogInfo("CMC fetchBarsFromAPI: Using %d bars from cache for %s.", len(cachedBars), cacheKey)
				utils.SortBarsByTimestamp(cachedBars)
				return cachedBars, nil
			}
		}
	}

	params := url.Values{
		"id":       {numericalCoinID},
		"convert":  {strings.ToUpper(vsCurrency)},
		"interval": {interval},
		"count":    {countForAPI},
	}

	var response cmcOHLCVHistoricalResponse
	if apiErr := c.makeAPICall(ctx, "/v2/cryptocurrency/ohlcv/historical", params, &response); apiErr != nil {
		return nil, apiErr
	}
	if response.Status.ErrorCode != 0 {
		return nil, fmt.Errorf("CMC API error: %s", response.Status.ErrorMessage)
	}

	var apiData cmcOHLCData
	if err := json.Unmarshal(response.Data, &apiData); err != nil {
		return nil, fmt.Errorf("failed to decode OHLCV data object: %w", err)
	}

	if len(apiData.Quotes) == 0 {
		return nil, fmt.Errorf("no OHLCV data for ID %s in response", numericalCoinID)
	}

	fetchedBars := make([]utils.OHLCVBar, 0, len(apiData.Quotes))
	for _, qData := range apiData.Quotes {
		quoteSet, ok := qData.QuoteMap[strings.ToUpper(vsCurrency)]
		if !ok {
			continue
		}
		ts, parseErr := time.Parse(time.RFC3339Nano, qData.TimeOpen)
		if parseErr != nil {
			continue
		}

		bar := utils.OHLCVBar{Timestamp: ts.UnixMilli(), Open: quoteSet.Open, High: quoteSet.High, Low: quoteSet.Low, Close: quoteSet.Close, Volume: quoteSet.Volume}
		fetchedBars = append(fetchedBars, bar)
		if c.cache != nil {
			// --- MODIFIED LINE: Use the new cacheKey when saving ---
			_ = c.cache.SaveBar(providerName, cacheKey, bar)
		}
	}
	utils.SortBarsByTimestamp(fetchedBars)
	return fetchedBars, nil
}

func (c *Client) GetHistoricalPrice(ctx context.Context, id, date string) (dataprovider.HistoricalPrice, error) {
	// Implementation for GetHistoricalPrice - can be complex with CMC's API
	return dataprovider.HistoricalPrice{}, errors.New("GetHistoricalPrice not fully implemented for CoinMarketCap")
}

func (c *Client) GetExchangeDetails(ctx context.Context, exchangeID string) (dataprovider.ExchangeDetails, error) {
	// Implementation for GetExchangeDetails
	return dataprovider.ExchangeDetails{}, errors.New("GetExchangeDetails not implemented for CoinMarketCap")
}

func (c *Client) GetGlobalMarketData(ctx context.Context) (dataprovider.GlobalMarketData, error) {
	params := url.Values{"convert": {c.cfg.QuoteCurrency}}
	var response cmcGlobalMetricsResponse
	if err := c.makeAPICall(ctx, "/v1/global-metrics/quotes/latest", params, &response); err != nil {
		return dataprovider.GlobalMarketData{}, err
	}
	if response.Status.ErrorCode != 0 {
		return dataprovider.GlobalMarketData{}, fmt.Errorf("CMC API error: %s", response.Status.ErrorMessage)
	}
	quote, ok := response.Data.Quote[strings.ToUpper(c.cfg.QuoteCurrency)]
	if !ok {
		return dataprovider.GlobalMarketData{}, fmt.Errorf("quote currency %s not found in global metrics response", c.cfg.QuoteCurrency)
	}
	return dataprovider.GlobalMarketData{
		TotalMarketCap: quote.TotalMarketCap,
		BTCDominance:   response.Data.BtcDominance,
	}, nil
}

func (c *Client) PrimeHistoricalData(ctx context.Context, id, vsCurrency, userInterval string, days int) error {
	cmcInterval, isSupported := mapToCMCInterval(userInterval)
	if !isSupported {
		c.logger.LogError("PRIMING CMC: Skipping unsupported interval '%s'. Priming is only supported for 1h, 4h, 1d, 7d.", userInterval)
		return nil
	}

	c.logger.LogInfo("PRIMING CMC: Fetching %d days for ID=%s, Interval=%s", days, id, cmcInterval)
	c.idMapMu.RLock()
	assetSymbol, symbolFound := c.numericalIDToSymbolMap[id]
	c.idMapMu.RUnlock()
	if !symbolFound {
		return fmt.Errorf("symbol for numerical ID %s not found", id)
	}

	var countForAPI string
	switch cmcInterval {
	case Interval1d:
		countForAPI = strconv.Itoa(days)
	case Interval1h:
		countForAPI = strconv.Itoa(days * 24)
	case Interval4h:
		countForAPI = strconv.Itoa(days * 6)
	default:
		countForAPI = "5000" // Max count for CMC
	}

	params := url.Values{
		"id":       {id},
		"convert":  {strings.ToUpper(vsCurrency)},
		"interval": {cmcInterval},
		"count":    {countForAPI},
	}

	var response cmcOHLCVHistoricalResponse
	if err := c.makeAPICall(ctx, "/v2/cryptocurrency/ohlcv/historical", params, &response); err != nil {
		return err
	}
	if response.Status.ErrorCode != 0 {
		return fmt.Errorf("CMC API error: %s", response.Status.ErrorMessage)
	}

	// --- FINAL FIX ---
	// Decode the 'Data' field directly into the object, matching the API response.
	var apiData cmcOHLCData
	if err := json.Unmarshal(response.Data, &apiData); err != nil {
		return fmt.Errorf("failed to decode historical data for priming: %w", err)
	}

	for _, qData := range apiData.Quotes {
		quoteSet, ok := qData.QuoteMap[strings.ToUpper(vsCurrency)]
		if !ok {
			continue
		}
		ts, err := time.Parse(time.RFC3339Nano, qData.TimeOpen)
		if err != nil {
			continue
		}
		bar := utils.OHLCVBar{Timestamp: ts.UnixMilli(), Open: quoteSet.Open, High: quoteSet.High, Low: quoteSet.Low, Close: quoteSet.Close, Volume: quoteSet.Volume}
		if c.cache != nil {
			_ = c.cache.SaveBar(providerName, assetSymbol, bar)
		}
	}
	c.logger.LogInfo("PRIMING CMC: Successfully processed and cached %d data points for %s.", len(apiData.Quotes), assetSymbol)
	return nil
}

// --- Helper Functions for Adaptive Fetch ---

// mapToCMCInterval checks if an interval is directly supported by the OHLCV endpoint.
func mapToCMCInterval(userInterval string) (string, bool) {
	lowInterval := strings.ToLower(userInterval)
	switch lowInterval {
	case "1h", "hourly", "60", "60m":
		return Interval1h, true
	case "4h", "4hour", "240", "240m":
		return Interval4h, true
	case "1d", "daily", "1440", "1440m":
		return Interval1d, true
	case "7d", "weekly":
		return Interval7d, true
	default:
		return "", false
	}
}

// getBestSourceInterval determines the best supported interval to fetch for resampling.
func getBestSourceInterval(userInterval string) string {
	// For any interval less than 4 hours, the best source we have is 1 hour.
	return Interval1h
}

// resampleBars converts bars from a source interval to a target interval.
func resampleBars(sourceBars []utils.OHLCVBar, targetIntervalStr string, logger *utils.Logger) ([]utils.OHLCVBar, error) {
	// --- FIX: Manually handle the "w" for week suffix ---
	// The time.ParseDuration function doesn't understand "w", so we convert it to hours.
	if strings.HasSuffix(targetIntervalStr, "w") {
		weeksStr := strings.TrimSuffix(targetIntervalStr, "w")
		weeks, err := strconv.Atoi(weeksStr)
		if err != nil {
			// This case is unlikely but good to have for safety.
			return nil, fmt.Errorf("invalid week format in interval: %s", targetIntervalStr)
		}
		hours := weeks * 7 * 24
		targetIntervalStr = fmt.Sprintf("%dh", hours) // e.g., "1w" becomes "168h"
	}
	// --- END OF FIX ---

	targetDuration, err := time.ParseDuration(targetIntervalStr)
	if err != nil {
		// This error should no longer occur for "1w".
		return nil, fmt.Errorf("invalid target interval format: %s", targetIntervalStr)
	}

	if len(sourceBars) == 0 {
		return []utils.OHLCVBar{}, nil
	}

	// For CoinMarketCap, we assume the smallest reliable source interval is 1 hour.
	sourceDuration := time.Hour
	sourceInterval := "1h"
	// If the user wants a smaller interval than our source can provide,
	// return no data to prevent using artificial prices.
	if targetDuration < sourceDuration {
		// Basic: Duplicate source bar for finer intervals (not ideal, but better than empty)
		resampled := []utils.OHLCVBar{}
		ratio := int(sourceDuration / targetDuration)
		for _, bar := range sourceBars {
			for i := 0; i < ratio; i++ {
				newBar := bar
				newBar.Timestamp -= int64(i) * int64(targetDuration.Milliseconds()) // Approximate timestamps
				resampled = append(resampled, newBar)
			}
		}
		logger.LogWarn("Resampled finer interval %s from %s by duplication (approx).", targetIntervalStr, sourceInterval)
		return resampled, nil
	}

	// If the target is larger or equal, we can just return the source bars.
	// A more complex implementation could aggregate 1h bars into 4h bars, etc.
	return sourceBars, nil
}
