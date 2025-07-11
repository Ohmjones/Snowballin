// File: dataprovider/coinmarketcap/cmclient.go
package coinmarketcap

import (
	"Snowballin/dataprovider"
	utils "Snowballin/utilities" // Renamed to utils to avoid conflict
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

// --- Constants ---
const (
	Interval5m   = "5m"
	Interval15m  = "15m"
	Interval30m  = "30m"
	Interval1h   = "1h"
	Interval2h   = "2h"
	Interval4h   = "4h"
	Interval6h   = "6h"
	Interval12h  = "12h"
	Interval1d   = "1d"
	Interval2d   = "2d"
	Interval3d   = "3d"
	Interval7d   = "7d"
	Interval14d  = "14d"
	Interval30d  = "30d"
	Interval90d  = "90d"
	Interval365d = "365d"

	providerName = "CoinMarketCap" // For SQLite cache provider key
)

// --- Type Declarations ---
type Client struct {
	BaseURL          string
	APIKeyHeaderName string
	APIKey           string
	HTTPClient       *http.Client
	limiter          *rate.Limiter
	logger           *utils.Logger
	cache            *dataprovider.SQLiteCache // SQLite cache
	cfg              *utils.CoinmarketcapConfig

	// ID Mapping and Refresh Control
	idMapMu                sync.RWMutex
	symbolToNumericalIDMap map[string]string // "btc" -> "1"
	nameToNumericalIDMap   map[string]string // "bitcoin" -> "1"
	numericalIDToSymbolMap map[string]string // "1" -> "BTC" (stores the symbol in the case CMC expects, e.g., uppercase)
	lastIDMapRefresh       time.Time
	idMapRefreshInterval   time.Duration
	isRefreshingIDMap      bool
	refreshMapMu           sync.Mutex
}

// --- CMC API Response Structs ---
type cmcStatus struct {
	Timestamp    string `json:"timestamp"`
	ErrorCode    int    `json:"error_code"`
	ErrorMessage string `json:"error_message,omitempty"`
	Elapsed      int    `json:"elapsed"`
	CreditCount  int    `json:"credit_count"`
	Notice       string `json:"notice,omitempty"`
}

// For /v1/cryptocurrency/map
type cmcMapEntry struct {
	ID                  int          `json:"id"`
	Name                string       `json:"name"`
	Symbol              string       `json:"symbol"`
	Slug                string       `json:"slug"`
	IsActive            int          `json:"is_active"` // 1 for active, 0 for inactive
	FirstHistoricalData string       `json:"first_historical_data,omitempty"`
	LastHistoricalData  string       `json:"last_historical_data,omitempty"`
	Platform            *cmcPlatform `json:"platform,omitempty"` // Used for tokens
}

type cmcPlatform struct {
	ID           int    `json:"id"`
	Name         string `json:"name"`
	Symbol       string `json:"symbol"`
	Slug         string `json:"slug"`
	TokenAddress string `json:"token_address"`
}

type cmcMapResponse struct {
	Data   []cmcMapEntry `json:"data"`
	Status cmcStatus     `json:"status"`
}

// For /v2/cryptocurrency/ohlcv/historical
type cmcOHLCVHistoricalResponse struct {
	Status cmcStatus              `json:"status"`
	Data   map[string]cmcOHLCData `json:"data"` // Keyed by numerical ID (string)
}

type cmcOHLCData struct {
	ID     interface{}     `json:"id"` // Can be int or string (numerical ID)
	Name   string          `json:"name"`
	Symbol string          `json:"symbol"`
	Quotes []cmcOHLCVQuote `json:"quotes"`
}

type cmcOHLCVQuote struct {
	TimeOpen  string                 `json:"time_open"`
	TimeClose string                 `json:"time_close"`
	QuoteMap  map[string]cmcOHLCVSet `json:"quote"` // Keyed by convert currency e.g. "USD"
}

type cmcOHLCVSet struct {
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
	MarketCap float64 `json:"market_cap"`
	Timestamp string  `json:"timestamp"` // This is the closing time of the candle
}

// For /v1/cryptocurrency/quotes/latest
type cmcQuotesLatestResponse struct {
	Data   map[string]cmcQuoteData `json:"data"` // Keyed by numerical ID (string)
	Status cmcStatus               `json:"status"`
}

type cmcQuoteData struct {
	ID                int                      `json:"id"`
	Name              string                   `json:"name"`
	Symbol            string                   `json:"symbol"`
	Slug              string                   `json:"slug"`
	NumMarketPairs    int                      `json:"num_market_pairs"`
	DateAdded         string                   `json:"date_added"`
	Tags              []string                 `json:"tags"`
	MaxSupply         *float64                 `json:"max_supply"` // Use pointer for nullable
	CirculatingSupply float64                  `json:"circulating_supply"`
	TotalSupply       float64                  `json:"total_supply"`
	Platform          *cmcPlatform             `json:"platform"`
	IsActive          int                      `json:"is_active"`
	CmcRank           int                      `json:"cmc_rank"`
	IsFiat            int                      `json:"is_fiat"`
	LastUpdated       string                   `json:"last_updated"`
	Quote             map[string]cmcPriceQuote `json:"quote"` // Keyed by convert currency
}

type cmcPriceQuote struct {
	Price                 float64 `json:"price"`
	Volume24h             float64 `json:"volume_24h"`
	VolumeChange24h       float64 `json:"volume_change_24h"`
	PercentChange1h       float64 `json:"percent_change_1h"`
	PercentChange24h      float64 `json:"percent_change_24h"`
	PercentChange7d       float64 `json:"percent_change_7d"`
	PercentChange30d      float64 `json:"percent_change_30d"`
	PercentChange60d      float64 `json:"percent_change_60d"`
	PercentChange90d      float64 `json:"percent_change_90d"`
	MarketCap             float64 `json:"market_cap"`
	MarketCapDominance    float64 `json:"market_cap_dominance"`
	FullyDilutedMarketCap float64 `json:"fully_diluted_market_cap"`
	LastUpdated           string  `json:"last_updated"`
}

// For /v1/exchange/info
type cmcExchangeInfoResponse struct {
	Data   map[string]cmcExchangeInfo `json:"data"` // Keyed by exchange ID or slug
	Status cmcStatus                  `json:"status"`
}

type cmcExchangeInfo struct {
	ID                    int     `json:"id"`
	Name                  string  `json:"name"`
	Slug                  string  `json:"slug"`
	Logo                  string  `json:"logo"`
	Description           string  `json:"description"`
	DateLaunched          string  `json:"date_launched"`
	Notice                string  `json:"notice"`
	SpotVolumeUsd         float64 `json:"spot_volume_usd"`
	SpotVolumeLastUpdated string  `json:"spot_volume_last_updated"`
	// other fields as needed
}

// For /v1/global-metrics/quotes/latest
type cmcGlobalMetricsResponse struct {
	Data   cmcGlobalMetricsData `json:"data"`
	Status cmcStatus            `json:"status"`
}

type cmcGlobalMetricsData struct {
	ActiveCryptocurrencies          int                       `json:"active_cryptocurrencies"`
	TotalCryptocurrencies           int                       `json:"total_cryptocurrencies"`
	ActiveMarketPairs               int                       `json:"active_market_pairs"`
	ActiveExchanges                 int                       `json:"active_exchanges"`
	TotalExchanges                  int                       `json:"total_exchanges"`
	EthDominance                    float64                   `json:"eth_dominance"`
	BtcDominance                    float64                   `json:"btc_dominance"`
	EthDominanceYesterday           float64                   `json:"eth_dominance_yesterday"`
	BtcDominanceYesterday           float64                   `json:"btc_dominance_yesterday"`
	EthDominance24hPercentageChange float64                   `json:"eth_dominance_24h_percentage_change"`
	BtcDominance24hPercentageChange float64                   `json:"btc_dominance_24h_percentage_change"`
	DefiVolume24h                   float64                   `json:"defi_volume_24h"`
	DefiVolume24hReported           float64                   `json:"defi_volume_24h_reported"`
	DefiMarketCap                   float64                   `json:"defi_market_cap"`
	Defi24hPercentageChange         float64                   `json:"defi_24h_percentage_change"`
	StablecoinVolume24h             float64                   `json:"stablecoin_volume_24h"`
	StablecoinVolume24hReported     float64                   `json:"stablecoin_volume_24h_reported"`
	StablecoinMarketCap             float64                   `json:"stablecoin_market_cap"`
	Stablecoin24hPercentageChange   float64                   `json:"stablecoin_24h_percentage_change"`
	DerivativesVolume24h            float64                   `json:"derivatives_volume_24h"`
	DerivativesVolume24hReported    float64                   `json:"derivatives_volume_24h_reported"`
	Derivatives24hPercentageChange  float64                   `json:"derivatives_24h_percentage_change"`
	LastUpdated                     string                    `json:"last_updated"`
	Quote                           map[string]cmcGlobalQuote `json:"quote"` // Keyed by convert currency
}

type cmcGlobalQuote struct {
	TotalMarketCap                          float64 `json:"total_market_cap"`
	TotalVolume24h                          float64 `json:"total_volume_24h"`
	TotalMarketCapYesterday                 float64 `json:"total_market_cap_yesterday"`
	TotalVolume24hYesterday                 float64 `json:"total_volume_24h_yesterday"`
	TotalMarketCapYesterdayPercentageChange float64 `json:"total_market_cap_yesterday_percentage_change"`
	TotalVolume24hYesterdayPercentageChange float64 `json:"total_volume_24h_yesterday_percentage_change"`
	LastUpdated                             string  `json:"last_updated"`
}

// --- Constructor ---
func NewClient(appCfg *utils.AppConfig, logger *utils.Logger, sqliteCache *dataprovider.SQLiteCache) (*Client, error) {
	if appCfg == nil || appCfg.Coinmarketcap == nil {
		return nil, errors.New("coinmarketcap client: AppConfig or CoinmarketcapConfig cannot be nil")
	}
	cfg := appCfg.Coinmarketcap

	if logger == nil {
		logger = utils.NewLogger(utils.Info) // Default logger
		logger.LogWarn("CoinMarketCap Client: Logger not provided, using default logger.")
	}
	if cfg.BaseURL == "" {
		return nil, errors.New("coinmarketcap client: BaseURL is required")
	}
	if cfg.APIKey == "" {
		// Allow empty API key for basic/test usage if CMC supports it, but log a warning.
		logger.LogWarn("CoinMarketCap Client: API key is empty. Functionality may be limited.")
	}
	if sqliteCache == nil {
		return nil, errors.New("coinmarketcap client: SQLiteCache cannot be nil")
	}

	// Default settings
	if cfg.RateLimitPerSec <= 0 {
		cfg.RateLimitPerSec = 1 // Default to 1 req/sec if not set or invalid
	}
	if cfg.RateLimitBurst <= 0 {
		cfg.RateLimitBurst = 1 // Default burst of 1
	}
	if cfg.RequestTimeoutSec <= 0 {
		cfg.RequestTimeoutSec = 15
	}
	if cfg.IDMapRefreshIntervalHours <= 0 {
		cfg.IDMapRefreshIntervalHours = 24 // Default to 24 hours
	}

	client := &Client{
		BaseURL:                cfg.BaseURL,
		APIKeyHeaderName:       "X-CMC_PRO_API_KEY",
		APIKey:                 cfg.APIKey,
		HTTPClient:             &http.Client{Timeout: time.Duration(cfg.RequestTimeoutSec) * time.Second},
		limiter:                rate.NewLimiter(rate.Limit(cfg.RateLimitPerSec), cfg.RateLimitBurst),
		logger:                 logger,
		cache:                  sqliteCache,
		cfg:                    cfg,
		symbolToNumericalIDMap: make(map[string]string),
		nameToNumericalIDMap:   make(map[string]string),
		numericalIDToSymbolMap: make(map[string]string),
		idMapRefreshInterval:   time.Duration(cfg.IDMapRefreshIntervalHours) * time.Hour,
	}

	// Non-blocking initial refresh of the ID map
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // Short timeout for initial refresh
		defer cancel()
		if err := client.refreshCoinIDMapIfNeeded(ctx, true); err != nil {
			client.logger.LogError("CoinMarketCap Client: Initial coin ID map refresh failed: %v", err)
		}
	}()

	logger.LogInfo("CoinMarketCap client initialized. BaseURL: %s, RateLimit: %.2f req/sec, Burst: %d", cfg.BaseURL, cfg.RateLimitPerSec, cfg.RateLimitBurst)
	return client, nil
}

// PrimeCache ensures the coin ID map is populated before trading begins.
func (c *Client) PrimeCache(ctx context.Context) error {
	c.logger.LogInfo("CoinMarketCap Client: Priming cache by fetching initial coin ID map...")
	// The 'true' flag forces a refresh.
	return c.refreshCoinIDMapIfNeeded(ctx, true)
}

// --- API Call Helper ---
func (c *Client) makeAPICall(ctx context.Context, endpoint string, params url.Values, result interface{}) error {
	if c.limiter == nil { // Should not happen if NewClient is used
		return errors.New("coinmarketcap client: rate limiter not initialized")
	}
	if err := ctx.Err(); err != nil { // Check context cancellation early
		return err
	}
	if err := c.limiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter wait error for %s: %w", endpoint, err)
	}

	fullURLStr := c.BaseURL + endpoint
	parsedURL, err := url.Parse(fullURLStr)
	if err != nil {
		return fmt.Errorf("cmc: bad url %s: %w", fullURLStr, err)
	}
	if params == nil {
		params = url.Values{} // Ensure params is not nil for encoding
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

	maxRetries := 3
	if c.cfg != nil && c.cfg.MaxRetries > 0 {
		maxRetries = c.cfg.MaxRetries
	}
	retryDelay := 5 * time.Second
	if c.cfg != nil && c.cfg.RetryDelaySec > 0 {
		retryDelay = time.Duration(c.cfg.RetryDelaySec) * time.Second
	}

	return utils.DoJSONRequest(c.HTTPClient, req, maxRetries, retryDelay, result)
}

// --- ID Mapping ---
func (c *Client) refreshCoinIDMapIfNeeded(ctx context.Context, force bool) error {
	c.idMapMu.RLock()
	mapIsEmpty := len(c.symbolToNumericalIDMap) == 0 && len(c.nameToNumericalIDMap) == 0
	var intervalPassed bool
	if c.idMapRefreshInterval > 0 {
		intervalPassed = time.Since(c.lastIDMapRefresh) > c.idMapRefreshInterval
	}
	c.idMapMu.RUnlock()

	if !force && !mapIsEmpty && !intervalPassed {
		return nil // Map is populated and fresh enough
	}

	c.refreshMapMu.Lock() // Single-flight lock for refresh operation
	if c.isRefreshingIDMap && !force {
		c.refreshMapMu.Unlock()
		c.logger.LogDebug("CoinMarketCap Client: Coin ID map refresh already in progress.")
		return nil // Another goroutine is already refreshing
	}
	c.isRefreshingIDMap = true
	c.refreshMapMu.Unlock()

	defer func() {
		c.refreshMapMu.Lock()
		c.isRefreshingIDMap = false
		c.refreshMapMu.Unlock()
	}()

	c.logger.LogInfo("CoinMarketCap Client: Refreshing coin ID map (Forced: %t, MapEmpty: %t, IntervalPassed: %t)...", force, mapIsEmpty, intervalPassed)

	var response cmcMapResponse
	// The /v1/cryptocurrency/map endpoint can be large. Consider if "aux" params are needed or if listing_status=active is enough.
	// For now, fetch all active.
	params := url.Values{"listing_status": {"active"}} // Fetch only active cryptocurrencies

	err := c.makeAPICall(ctx, "/v1/cryptocurrency/map", params, &response)
	if err != nil {
		return fmt.Errorf("refreshCoinIDMapIfNeeded: API call to /map failed: %w", err)
	}
	if response.Status.ErrorCode != 0 {
		return fmt.Errorf("refreshCoinIDMapIfNeeded: /map API error: %s (Code: %d)", response.Status.ErrorMessage, response.Status.ErrorCode)
	}

	c.idMapMu.Lock()
	defer c.idMapMu.Unlock()

	// Reset maps before repopulating
	c.symbolToNumericalIDMap = make(map[string]string)
	c.nameToNumericalIDMap = make(map[string]string)
	c.numericalIDToSymbolMap = make(map[string]string)

	for _, entry := range response.Data {
		if entry.IsActive == 0 { // Skip inactive coins if any slip through filter
			continue
		}
		numericalIDStr := strconv.Itoa(entry.ID)

		// Store symbol -> numerical ID (lowercase key for consistent lookup)
		if entry.Symbol != "" {
			c.symbolToNumericalIDMap[strings.ToLower(entry.Symbol)] = numericalIDStr
			c.numericalIDToSymbolMap[numericalIDStr] = entry.Symbol // Store the original casing of the symbol
		}
		// Store name -> numerical ID (lowercase key)
		if entry.Name != "" {
			c.nameToNumericalIDMap[strings.ToLower(entry.Name)] = numericalIDStr
		}
	}
	c.lastIDMapRefresh = time.Now().UTC()
	c.logger.LogInfo("CoinMarketCap Client: Coin ID map refreshed. Mapped %d active coins.", len(response.Data))
	return nil
}

func (c *Client) GetCoinID(ctx context.Context, commonAssetSymbol string) (string, error) {
	if commonAssetSymbol == "" {
		c.logger.LogError("CoinMarketCap GetCoinID: called with empty commonAssetSymbol")
		return "", errors.New("common asset symbol cannot be empty")
	}

	// Attempt to refresh map if needed, with a short timeout for this specific call context
	refreshCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := c.refreshCoinIDMapIfNeeded(refreshCtx, false); err != nil {
		c.logger.LogWarn("CoinMarketCap GetCoinID: Non-critical error refreshing ID map for '%s': %v. Proceeding with potentially stale map.", commonAssetSymbol, err)
	}

	c.idMapMu.RLock()
	defer c.idMapMu.RUnlock()

	if len(c.symbolToNumericalIDMap) == 0 && len(c.nameToNumericalIDMap) == 0 { // Check if maps are populated
		// If force refreshing didn't populate, something is wrong with the /map endpoint or our handling of it.
		// Try one more forceful refresh, blocking, if the maps are truly empty.
		// This case should be rare if the initial non-blocking refresh in NewClient works.
		c.idMapMu.RUnlock() // Release read lock before acquiring write lock in refresh
		c.logger.LogWarn("CoinMarketCap GetCoinID: ID maps are empty. Attempting a blocking refresh for '%s'.", commonAssetSymbol)
		// Use a slightly longer timeout for this blocking refresh attempt.
		blockingRefreshCtx, blockingCancel := context.WithTimeout(ctx, 25*time.Second)
		defer blockingCancel()
		if refreshErr := c.refreshCoinIDMapIfNeeded(blockingRefreshCtx, true); refreshErr != nil {
			c.logger.LogError("CoinMarketCap GetCoinID: Blocking refresh also failed for '%s': %v.", commonAssetSymbol, refreshErr)
			return "", fmt.Errorf("CoinMarketCap ID map is empty and refresh failed for asset '%s': %w", commonAssetSymbol, refreshErr)
		}
		c.idMapMu.RLock() // Re-acquire read lock
		if len(c.symbolToNumericalIDMap) == 0 && len(c.nameToNumericalIDMap) == 0 {
			return "", fmt.Errorf("CoinMarketCap ID map is still empty after blocking refresh for asset '%s'", commonAssetSymbol)
		}
	}

	lookupKey := strings.ToLower(commonAssetSymbol)

	// Try symbol map first
	if id, ok := c.symbolToNumericalIDMap[lookupKey]; ok {
		return id, nil
	}
	// Try name map if symbol not found
	if id, ok := c.nameToNumericalIDMap[lookupKey]; ok {
		return id, nil
	}

	c.logger.LogWarn("CoinMarketCap GetCoinID: No numerical ID found for symbol/name '%s' after checking maps.", commonAssetSymbol)
	return "", fmt.Errorf("CoinMarketCap numerical ID not found for asset symbol/name: %s", commonAssetSymbol)
}

// --- DataProvider Interface Implementations ---

// GetSupportedCoins retrieves a list of all coins supported by the provider.
func (c *Client) GetSupportedCoins(ctx context.Context) ([]dataprovider.Coin, error) {
	// This method directly calls the /map endpoint to get full coin details
	// as required by the dataprovider.Coin struct (ID, Symbol, Name).
	// The internal ID maps (symbolToNumericalIDMap, etc.) are primarily for the GetCoinID method.

	c.logger.LogDebug("CoinMarketCap GetSupportedCoins: Fetching directly from /v1/cryptocurrency/map")

	var response cmcMapResponse
	// Fetch only active cryptocurrencies. Adjust if other statuses are needed.
	params := url.Values{"listing_status": {"active"}}

	err := c.makeAPICall(ctx, "/v1/cryptocurrency/map", params, &response)
	if err != nil {
		return nil, fmt.Errorf("GetSupportedCoins: API call to /map failed: %w", err)
	}
	if response.Status.ErrorCode != 0 {
		return nil, fmt.Errorf("GetSupportedCoins: /map API error: %s (Code: %d)", response.Status.ErrorMessage, response.Status.ErrorCode)
	}

	dpCoins := make([]dataprovider.Coin, 0, len(response.Data))
	for _, entry := range response.Data {
		if entry.IsActive != 0 { // Ensure we only add active coins
			dpCoins = append(dpCoins, dataprovider.Coin{
				ID:     strconv.Itoa(entry.ID), // Numerical ID as string
				Symbol: entry.Symbol,
				Name:   entry.Name,
			})
		}
	}

	if len(dpCoins) == 0 && len(response.Data) > 0 {
		c.logger.LogWarn("CoinMarketCap GetSupportedCoins: API returned coin data, but none were processed as active. Check 'is_active' field or API response.")
	} else if len(dpCoins) == 0 {
		c.logger.LogWarn("CoinMarketCap GetSupportedCoins: No coins returned from /map endpoint or none were active.")
	} else {
		c.logger.LogInfo("CoinMarketCap GetSupportedCoins: Successfully fetched %d active coins.", len(dpCoins))
	}

	return dpCoins, nil
}

// GetMarketData retrieves current market data for a list of *numerical* coin IDs.
func (c *Client) GetMarketData(ctx context.Context, numericalIDs []string, vsCurrency string) ([]dataprovider.MarketData, error) {
	if len(numericalIDs) == 0 {
		return []dataprovider.MarketData{}, nil
	}
	if vsCurrency == "" {
		vsCurrency = "USD" // Default vsCurrency
	}

	joinedIDs := strings.Join(numericalIDs, ",")
	params := url.Values{
		"id":      {joinedIDs},
		"convert": {strings.ToUpper(vsCurrency)},
		// "aux": {"num_market_pairs,cmc_rank,date_added,tags,platform,max_supply,circulating_supply,total_supply,is_active,is_fiat"}, // Request more data if needed
	}

	var response cmcQuotesLatestResponse
	err := c.makeAPICall(ctx, "/v1/cryptocurrency/quotes/latest", params, &response)
	if err != nil {
		return nil, fmt.Errorf("CMC GetMarketData for IDs [%s] failed: %w", joinedIDs, err)
	}
	if response.Status.ErrorCode != 0 {
		return nil, fmt.Errorf("CMC GetMarketData API error for IDs [%s]: %s (Code: %d)", joinedIDs, response.Status.ErrorMessage, response.Status.ErrorCode)
	}

	marketDataSlice := make([]dataprovider.MarketData, 0, len(numericalIDs))
	for _, numericalID := range numericalIDs {
		data, ok := response.Data[numericalID]
		if !ok {
			c.logger.LogWarn("CMC GetMarketData: No data found for numerical ID %s in response.", numericalID)
			continue
		}
		quote, quoteOk := data.Quote[strings.ToUpper(vsCurrency)]
		if !quoteOk {
			c.logger.LogWarn("CMC GetMarketData: No quote for currency %s found for ID %s (Symbol: %s).", vsCurrency, numericalID, data.Symbol)
			continue
		}

		lastUpdatedTime, parseErr := time.Parse(time.RFC3339Nano, quote.LastUpdated)
		if parseErr != nil {
			c.logger.LogWarn("CMC GetMarketData: Failed to parse LastUpdated timestamp '%s' for %s: %v. Using current time.", quote.LastUpdated, data.Symbol, parseErr)
			lastUpdatedTime = time.Now().UTC()
		}

		// Save current price as a bar to SQLite cache for potential gap filling or simple price tracking
		// This is a single point, not true OHLCV, but better than nothing for the cache.
		barToCache := utils.OHLCVBar{
			Timestamp: lastUpdatedTime.UnixMilli(), // Use the quote's last_updated time
			Open:      quote.Price,                 // Use price for O, H, L, C as it's a snapshot
			High:      quote.Price,
			Low:       quote.Price,
			Close:     quote.Price,
			Volume:    quote.Volume24h, // Use 24h volume
		}
		if c.cache != nil {
			cacheErr := c.cache.SaveBar(providerName, data.Symbol, barToCache) // Use Symbol for coin_id in cache for consistency with GetOHLCV
			if cacheErr != nil {
				c.logger.LogWarn("CMC GetMarketData: Failed to save market data point to SQLite cache for %s: %v", data.Symbol, cacheErr)
			}
		}

		marketDataSlice = append(marketDataSlice, dataprovider.MarketData{
			ID:             strconv.Itoa(data.ID), // Ensure ID is string
			Symbol:         data.Symbol,
			Name:           data.Name,
			CurrentPrice:   quote.Price,
			MarketCap:      quote.MarketCap,
			Volume24h:      quote.Volume24h,
			High24h:        quote.Price, // CMC quotes/latest doesn't provide discrete 24h high/low for the *pair*
			Low24h:         quote.Price, // It's part of ticker info for exchanges, not direct coin quote.
			PriceChange1h:  quote.PercentChange1h,
			PriceChange24h: quote.PercentChange24h,
			PriceChange7d:  quote.PercentChange7d,
			LastUpdated:    lastUpdatedTime,
		})
	}
	return marketDataSlice, nil
}

// GetOHLCVHistorical retrieves historical OHLCV data for regular, smaller fetches.
func (c *Client) GetOHLCVHistorical(ctx context.Context, id, vsCurrency, interval string) ([]utils.OHLCVBar, error) {
	numericalCoinID := id
	userInterval := interval
	c.logger.LogDebug("CoinMarketCap GetOHLCVHistorical: Fetching for NumID=%s, VS=%s, Interval=%s", numericalCoinID, vsCurrency, userInterval)

	c.idMapMu.RLock()
	assetSymbol, symbolFound := c.numericalIDToSymbolMap[numericalCoinID]
	c.idMapMu.RUnlock()
	if !symbolFound {
		return nil, fmt.Errorf("CoinMarketCap GetOHLCVHistorical: Symbol for numerical ID %s not found", numericalCoinID)
	}

	cmcInterval, err := mapToCMCInterval(userInterval)
	if err != nil {
		cmcInterval = Interval1d // Default
	}

	var lookbackPeriodDays int
	var countForAPI string
	var expectedIntervalDuration time.Duration

	// This logic determines the amount of data to get for regular indicator calculations.
	switch cmcInterval {
	case Interval1d:
		lookbackPeriodDays = c.cfg.OHLCVDaysDefault
		if lookbackPeriodDays <= 0 {
			lookbackPeriodDays = 90
		}
		countForAPI = strconv.Itoa(lookbackPeriodDays)
		expectedIntervalDuration = 24 * time.Hour
	case Interval1h:
		lookbackPeriodDays = 7
		countForAPI = strconv.Itoa(7 * 24)
		expectedIntervalDuration = 1 * time.Hour
	case Interval4h:
		lookbackPeriodDays = 30
		countForAPI = strconv.Itoa(30 * 6)
		expectedIntervalDuration = 4 * time.Hour
	default:
		lookbackPeriodDays = 90
		countForAPI = "90"
		expectedIntervalDuration = 24 * time.Hour
	}

	now := time.Now().UTC()
	cacheStartTime := now.AddDate(0, 0, -lookbackPeriodDays)

	// --- FULL CACHE CHECKING AND API FETCH LOGIC ---
	if c.cache != nil {
		cachedBars, errCache := c.cache.GetBars(providerName, assetSymbol, cacheStartTime.UnixMilli(), now.UnixMilli())
		if errCache == nil && len(cachedBars) > 0 {
			latestCachedBarTime := time.UnixMilli(cachedBars[len(cachedBars)-1].Timestamp).UTC()
			if now.Sub(latestCachedBarTime) < 2*expectedIntervalDuration {
				c.logger.LogInfo("CMC GetOHLCVHistorical: Using %d bars from cache for %s.", len(cachedBars), assetSymbol)
				utils.SortBarsByTimestamp(cachedBars)
				return cachedBars, nil
			}
		}
	}

	params := url.Values{
		"symbol":   {strings.ToUpper(assetSymbol)},
		"convert":  {strings.ToUpper(vsCurrency)},
		"interval": {cmcInterval},
		"count":    {countForAPI},
	}

	var response cmcOHLCVHistoricalResponse
	apiErr := c.makeAPICall(ctx, "/v2/cryptocurrency/ohlcv/historical", params, &response)
	if apiErr != nil {
		return nil, apiErr
	}
	if response.Status.ErrorCode != 0 {
		return nil, fmt.Errorf("CMC API error: %s", response.Status.ErrorMessage)
	}

	apiData, dataExists := response.Data[numericalCoinID]
	if !dataExists {
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
			c.cache.SaveBar(providerName, assetSymbol, bar)
		}
	}
	utils.SortBarsByTimestamp(fetchedBars)
	return fetchedBars, nil
}

// GetHistoricalPrice retrieves the price of a coin on a specific past date.
// id: This is the numericalCoinID.
// date: "YYYY-MM-DD" (ISO 8601 date) is preferred by CMC for time_start/time_end.
// The original interface specified "DD-MM-YYYY", so we might need to parse and reformat.
func (c *Client) GetHistoricalPrice(ctx context.Context, numericalCoinID, dateStr string) (dataprovider.HistoricalPrice, error) {
	// Parse dateStr from "DD-MM-YYYY" to "YYYY-MM-DD"
	parsedDate, err := time.Parse("02-01-2006", dateStr)
	if err != nil {
		return dataprovider.HistoricalPrice{}, fmt.Errorf("invalid date format '%s', please use DD-MM-YYYY: %w", dateStr, err)
	}
	cmcDateStr := parsedDate.Format("2006-01-02")

	// The historical quotes endpoint for CMC often returns OHLCV for the day.
	// We need to use their specific historical quote endpoint if available, or derive from OHLCV.
	// Using /v2/cryptocurrency/ohlcv/historical for a single day.
	params := url.Values{
		"id":         {numericalCoinID}, // This endpoint takes numerical ID
		"time_start": {cmcDateStr},
		"time_end":   {cmcDateStr}, // For a single day's data
		"count":      {"1"},        // Expect 1 day of data
		"interval":   {"daily"},    // Daily interval
		"convert_id": {"2781"},     // Default to USD (numerical ID for USD)
	}
	// TODO: Allow vsCurrency to be passed and mapped to convert_id
	// quoteCurrencyID := "2781" // USD
	// if c.cfg.QuoteCurrency != "" { lookup c.cfg.QuoteCurrency to its numerical ID for CMC }

	var response cmcOHLCVHistoricalResponse
	apiErr := c.makeAPICall(ctx, "/v2/cryptocurrency/quotes/historical", params, &response) // Switched to quotes/historical v2
	if apiErr != nil {
		return dataprovider.HistoricalPrice{}, fmt.Errorf("CMC GetHistoricalPrice for NumID %s on %s failed: %w", numericalCoinID, cmcDateStr, apiErr)
	}

	if response.Status.ErrorCode != 0 {
		return dataprovider.HistoricalPrice{}, fmt.Errorf("CMC API error for GetHistoricalPrice NumID %s: %s (Code: %d)", numericalCoinID, response.Status.ErrorMessage, response.Status.ErrorCode)
	}

	responseData, dataExists := response.Data[numericalCoinID]
	if !dataExists || len(responseData.Quotes) == 0 {
		return dataprovider.HistoricalPrice{}, fmt.Errorf("no historical data found for NumID %s on %s in CMC response", numericalCoinID, cmcDateStr)
	}

	// Assuming USD quote for now, key is "USD" or its convert_id like "2781"
	// The response structure for quotes/historical (if different from ohlcv/historical) needs to be checked.
	// For now, assuming it's similar to OHLCV and we take the close price.
	vsQuoteKey := "USD" // This should be the actual vsCurrency used in convert_id

	dailyQuote, quoteExists := responseData.Quotes[0].QuoteMap[vsQuoteKey]
	if !quoteExists {
		// Try to find the first available quote if "USD" is not present (e.g. if convert_id was different)
		foundKey := ""
		for k := range responseData.Quotes[0].QuoteMap {
			vsQuoteKey = k
			dailyQuote = responseData.Quotes[0].QuoteMap[k]
			foundKey = k
			break
		}
		if foundKey == "" {
			return dataprovider.HistoricalPrice{}, fmt.Errorf("no quote data for any currency found for NumID %s on %s", numericalCoinID, cmcDateStr)
		}
		c.logger.LogWarn("GetHistoricalPrice: Target quote 'USD' not found, using first available: '%s'", foundKey)
	}

	// Timestamp of the historical data point
	ts, parseErr := time.Parse(time.RFC3339Nano, responseData.Quotes[0].TimeOpen)
	if parseErr != nil {
		c.logger.LogWarn("GetHistoricalPrice (CMC): Failed parsing historical quote TimeOpen for %s ('%s'): %v", responseData.Symbol, responseData.Quotes[0].TimeOpen, parseErr)
		// Fallback or error out
	}

	return dataprovider.HistoricalPrice{
		ID:        numericalCoinID,  // Return the numerical ID
		Price:     dailyQuote.Close, // Use the closing price for that day
		Date:      dateStr,          // Original requested date
		Timestamp: strconv.FormatInt(ts.UnixMilli(), 10),
	}, nil
}

// GetExchangeDetails retrieves information about a specific exchange.
// exchangeID: This could be slug or numerical ID. CMC's /v1/exchange/info takes 'id' or 'slug'.
func (c *Client) GetExchangeDetails(ctx context.Context, exchangeIdentifier string) (dataprovider.ExchangeDetails, error) {
	params := url.Values{}
	// Determine if exchangeIdentifier is numerical (id) or string (slug)
	if _, err := strconv.Atoi(exchangeIdentifier); err == nil {
		params.Set("id", exchangeIdentifier)
	} else {
		params.Set("slug", exchangeIdentifier)
	}

	var response cmcExchangeInfoResponse
	err := c.makeAPICall(ctx, "/v1/exchange/info", params, &response)
	if err != nil {
		return dataprovider.ExchangeDetails{}, fmt.Errorf("CMC GetExchangeDetails for '%s' failed: %w", exchangeIdentifier, err)
	}
	if response.Status.ErrorCode != 0 {
		return dataprovider.ExchangeDetails{}, fmt.Errorf("CMC API error for GetExchangeDetails '%s': %s (Code: %d)", exchangeIdentifier, response.Status.ErrorMessage, response.Status.ErrorCode)
	}

	// The response data is keyed by the input identifier (id or slug).
	// We need to iterate to find the actual data as the key might differ from `exchangeIdentifier` if it was a slug.
	var details cmcExchangeInfo
	found := false
	for _, data := range response.Data { // Iterate through map values
		details = data
		found = true
		break
	}

	if !found {
		return dataprovider.ExchangeDetails{}, fmt.Errorf("no exchange details found for identifier '%s' in CMC response", exchangeIdentifier)
	}

	return dataprovider.ExchangeDetails{
		Name:      details.Name,
		Volume24h: details.SpotVolumeUsd, // This is spot volume in USD
	}, nil
}

// GetGlobalMarketData retrieves overall cryptocurrency market metrics.
func (c *Client) GetGlobalMarketData(ctx context.Context) (dataprovider.GlobalMarketData, error) {
	params := url.Values{}

	// Use the configured quote currency, defaulting to USD if not set.
	quoteCurrency := "USD"
	if c.cfg != nil && c.cfg.QuoteCurrency != "" {
		quoteCurrency = c.cfg.QuoteCurrency
	}
	params.Set("convert", strings.ToUpper(quoteCurrency))

	var response cmcGlobalMetricsResponse
	err := c.makeAPICall(ctx, "/v1/global-metrics/quotes/latest", params, &response)
	if err != nil {
		return dataprovider.GlobalMarketData{}, fmt.Errorf("CMC GetGlobalMarketData failed: %w", err)
	}
	if response.Status.ErrorCode != 0 {
		return dataprovider.GlobalMarketData{}, fmt.Errorf("CMC API error for GetGlobalMarketData: %s (Code: %d)", response.Status.ErrorMessage, response.Status.ErrorCode)
	}

	quote, ok := response.Data.Quote[strings.ToUpper(quoteCurrency)]
	if !ok {
		// Fallback to first available quote if the primary one isn't present
		foundKey := ""
		for k, qVal := range response.Data.Quote {
			quote = qVal
			foundKey = k
			break
		}
		if foundKey == "" {
			return dataprovider.GlobalMarketData{}, errors.New("no quote data found in CMC global metrics response")
		}
		c.logger.LogWarn("GetGlobalMarketData: Target quote '%s' not found, using first available: '%s'", quoteCurrency, foundKey)
	}

	return dataprovider.GlobalMarketData{
		TotalMarketCap: quote.TotalMarketCap,
		BTCDominance:   response.Data.BtcDominance,
	}, nil
}

// --- Helper Functions ---
func mapToCMCInterval(userInterval string) (string, error) {
	// CMC intervals: "5m", "10m", "15m", "30m", "1h", "2h", "3h", "4h", "6h", "12h", "1d", "2d", "3d", "7d", "14d", "15d", "30d", "60d", "90d", "365d"
	// And also "hourly" (same as "1h"), "daily" (same as "1d"), "weekly" (same as "7d"), "monthly" (same as "30d"), "yearly" (same as "365d")
	// And specific endings like "15min" "30minute" "4hour" "1day"

	lowInterval := strings.ToLower(userInterval)
	switch lowInterval {
	case "5m", "5min", "5minute":
		return Interval5m, nil
	case "15m", "15min", "15minute":
		return Interval15m, nil
	case "30m", "30min", "30minute":
		return Interval30m, nil
	case "1h", "hourly", "1hour":
		return Interval1h, nil
	case "2h", "2hour":
		return Interval2h, nil
	case "4h", "4hour":
		return Interval4h, nil
	case "6h", "6hour":
		return Interval6h, nil
	case "12h", "12hour":
		return Interval12h, nil
	case "1d", "daily", "1day":
		return Interval1d, nil
	case "7d", "weekly", "1week":
		return Interval7d, nil
	// Add more mappings as needed for other supported CMC intervals
	default:
		// Check if it's one of the direct "Xd" formats if not caught above
		if strings.HasSuffix(lowInterval, "d") && !strings.ContainsAny(lowInterval, "hms") {
			daysStr := strings.TrimSuffix(lowInterval, "d")
			if _, err := strconv.Atoi(daysStr); err == nil {
				// It's a valid "Xd" like "2d", "30d" etc. Return it as is.
				return lowInterval, nil
			}
		}
		return "", fmt.Errorf("unsupported or ambiguous interval for CoinMarketCap: %s", userInterval)
	}
}

// PrimeHistoricalData fetches a large amount of historical data to populate the cache.
func (c *Client) PrimeHistoricalData(ctx context.Context, id, vsCurrency, interval string, days int) error {
	numericalCoinID := id
	userInterval := interval
	c.logger.LogInfo("PRIMING CoinMarketCap Data: Fetching %d days for ID=%s, Interval=%s", days, numericalCoinID, userInterval)

	if days <= 0 {
		return fmt.Errorf("priming days must be positive, got %d", days)
	}

	c.idMapMu.RLock()
	assetSymbol, symbolFound := c.numericalIDToSymbolMap[numericalCoinID]
	c.idMapMu.RUnlock()
	if !symbolFound {
		return fmt.Errorf("symbol for numerical ID %s not found", numericalCoinID)
	}

	cmcInterval, err := mapToCMCInterval(userInterval)
	if err != nil {
		cmcInterval = Interval1d
	}

	var countForAPI string
	if cmcInterval == Interval1d {
		countForAPI = strconv.Itoa(days)
	} else if cmcInterval == Interval1h {
		countForAPI = strconv.Itoa(days * 24)
	} else if cmcInterval == Interval4h {
		countForAPI = strconv.Itoa(days * 6)
	} else {
		countForAPI = "5000"
	} // Max count for CMC

	params := url.Values{
		"symbol":   {strings.ToUpper(assetSymbol)},
		"convert":  {strings.ToUpper(vsCurrency)},
		"interval": {cmcInterval},
		"count":    {countForAPI},
	}

	var response cmcOHLCVHistoricalResponse
	apiErr := c.makeAPICall(ctx, "/v2/cryptocurrency/ohlcv/historical", params, &response)
	if apiErr != nil {
		return apiErr
	}
	if response.Status.ErrorCode != 0 {
		return fmt.Errorf("CMC API error: %s", response.Status.ErrorMessage)
	}

	apiData, dataExists := response.Data[numericalCoinID]
	if !dataExists {
		return fmt.Errorf("no OHLCV data for ID %s in response", numericalCoinID)
	}

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
		if c.cache != nil {
			c.cache.SaveBar(providerName, assetSymbol, bar)
		}
	}
	c.logger.LogInfo("PRIMING CoinMarketCap Data: Successfully processed and cached %d data points for %s.", len(apiData.Quotes), assetSymbol)
	return nil
}
