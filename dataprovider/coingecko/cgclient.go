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
	idMapOnce            sync.Once
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
	TotalMarketCap      map[string]float64 `json:"total_market_cap"`
	MarketCapPercentage map[string]float64 `json:"market_cap_percentage"`
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
type cgTicker struct {
	Base   string `json:"base"`
	Target string `json:"target"`
	Market struct {
		Name string `json:"name"`
	} `json:"market"`
	Last                   float64            `json:"last"`
	ConvertedLast          map[string]float64 `json:"converted_last"`
	Volume                 float64            `json:"volume"`
	ConvertedVolume        map[string]float64 `json:"converted_volume"`
	TrustScore             string             `json:"trust_score"`
	BidAskSpreadPercentage float64            `json:"bid_ask_spread_percentage"`
	Timestamp              string             `json:"timestamp"`
	LastTradedAt           string             `json:"last_traded_at"`
	LastFetchAt            string             `json:"last_fetch_at"`
	IsAnomaly              bool               `json:"is_anomaly"`
	IsStale                bool               `json:"is_stale"`
	TradeURL               string             `json:"trade_url"`
}

type cgTickersResponse struct {
	Name    string     `json:"name"`
	Tickers []cgTicker `json:"tickers"`
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
		cgCfg.RateLimitPerSec = 1
		logger.LogWarn("CoinGecko Client: Invalid RateLimitPerSec, defaulting to 1")
	}

	if cgCfg.RateLimitBurst <= 0 {
		cgCfg.RateLimitBurst = 1
		logger.LogWarn("CoinGecko Client: Invalid RateLimitBurst, defaulting to 1")
	}

	if cgCfg.RequestTimeoutSec <= 0 {
		cgCfg.RequestTimeoutSec = 10
		logger.LogWarn("CoinGecko Client: Invalid RequestTimeoutSec, defaulting to 10 seconds")
	}

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
		idMapOnce:            sync.Once{},
		symbolToIDMap:        make(map[string]string),
		idMapRefreshInterval: time.Duration(cgCfg.IDMapRefreshIntervalHours) * time.Hour,
		cfg:                  cgCfg,
		cache:                cache,
	}

	// This block ensures the map is populated once, and safely, before any lookups.
	client.idMapOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := client.refreshCoinIDMapIfNeeded(ctx, true); err != nil {
			client.logger.LogError("CoinGecko Client: Initial coin ID map refresh failed: %v", err)
		}
	})

	logger.LogInfo("CoinGecko client initialized with URL: %s, RateLimit: %.2f req/sec", client.BaseURL, cgCfg.RateLimitPerSec)

	return client, nil
}

// GetTopAssetsByMarketCap fetches the top N assets by market capitalization from CoinGecko.
func (c *Client) GetTopAssetsByMarketCap(ctx context.Context, quoteCurrency string, topN int) ([]string, error) {
	var response []cgMarketData
	params := url.Values{}
	params.Add("vs_currency", strings.ToLower(quoteCurrency))
	params.Add("order", "market_cap_desc")
	params.Add("per_page", strconv.Itoa(topN))
	params.Add("page", "1")
	params.Add("sparkline", "false")

	err := c.request(ctx, "/coins/markets", params, &response)
	if err != nil {
		return nil, fmt.Errorf("GetTopAssetsByMarketCap failed: %w", err)
	}

	var assetPairs []string
	for _, market := range response {
		// This creates the pair in the format your bot expects, e.g., "BTC/USD"
		// Note: This assumes a direct mapping. The symbol collision issue you mentioned
		// is important and will require a dedicated mapping utility later on.
		assetPairs = append(assetPairs, fmt.Sprintf("%s/%s", strings.ToUpper(market.Symbol), strings.ToUpper(quoteCurrency)))
	}

	return assetPairs, nil
}

// GetAllTickersForAsset fetches all market tickers for a specific coin ID from CoinGecko.
func (c *Client) GetAllTickersForAsset(ctx context.Context, coinID string) ([]dataprovider.CrossExchangeTicker, error) {
	var response cgTickersResponse
	endpoint := fmt.Sprintf("/coins/%s/tickers", coinID)
	params := url.Values{}
	params.Add("include_exchange_logo", "false")
	// You can add pagination here if a coin is listed on more than 100 exchanges
	params.Add("page", "1")

	err := c.request(ctx, endpoint, params, &response)
	if err != nil {
		return nil, fmt.Errorf("GetAllTickersForAsset for coin '%s' failed: %w", coinID, err)
	}

	tickers := make([]dataprovider.CrossExchangeTicker, 0, len(response.Tickers))
	for _, ticker := range response.Tickers {
		// The volume is often in the base currency. We use the 'converted_volume' in USD for a consistent measure.
		usdVolume, ok := ticker.ConvertedVolume["usd"]
		if !ok {
			continue // Skip tickers without USD volume data for accurate comparison
		}

		tickers = append(tickers, dataprovider.CrossExchangeTicker{
			ExchangeName: ticker.Market.Name,
			Price:        ticker.Last,
			Volume24h:    usdVolume,
		})
	}

	return tickers, nil
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
		ctx = context.Background()
	}

	if err := c.limiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter error for endpoint %s: %w", endpoint, err)
	}

	fullURL := c.BaseURL + endpoint
	if !strings.HasPrefix(endpoint, "/") && !strings.HasSuffix(c.BaseURL, "/") {
		fullURL = c.BaseURL + "/" + endpoint
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request for %s: %w", fullURL, err)
	}

	if queryParams == nil {
		queryParams = url.Values{}
	}

	if c.APIKey != "" {
		req.Header.Set("x-cg-pro-api-key", c.APIKey)
	}

	if len(queryParams) > 0 {
		req.URL.RawQuery = queryParams.Encode()
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "SnowballinBot/1.0")
	c.logger.LogDebug("CoinGecko Request: %s %s", req.Method, req.URL.String())

	maxRetries := c.cfg.MaxRetries
	retryDelay := time.Duration(c.cfg.RetryDelaySec) * time.Second

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
	up := strings.ToUpper(sym)
	lo := strings.ToLower(sym)

	// --- ADDED: Priority map for critical assets ---
	priorityMap := map[string]string{
		"BTC": "bitcoin",
		"ETH": "ethereum",
		"SOL": "solana",
		"XRP": "ripple",
	}
	if id, ok := priorityMap[up]; ok {
		c.logger.LogDebug("CoinGecko GetCoinID: Found '%s' in priority map. Using ID: '%s'", up, id)
		return id, nil
	}

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
	mapLen := len(c.idMap) + len(c.symbolToIDMap)
	if mapLen == 0 {
		c.idMapMu.RUnlock()
		return "", fmt.Errorf(
			"CoinGecko ID map is empty for asset '%s'; initial population may be pending or failed",
			sym,
		)
	}

	// 4) Data-driven lookup strategies
	if id, ok := c.idMap[up]; ok {
		c.idMapMu.RUnlock()
		return id, nil
	}
	if id, ok := c.idMap[lo]; ok {
		c.idMapMu.RUnlock()
		return id, nil
	}
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
		if id, ok := c.idMap["bitcoin"]; ok {
			c.idMapMu.RUnlock()
			return id, nil
		}
	}

	c.idMapMu.RUnlock()

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

// GetOHLCDaily fetches true daily OHLCV data from the /ohlc endpoint.
// This data is required for indicators that rely on full candle structure, like ATR and liquidity hunt detection.
func (c *Client) GetOHLCDaily(ctx context.Context, id, vsCurrency string, days int) ([]utils.OHLCVBar, error) {
	c.logger.LogDebug("CoinGecko GetOHLCDaily: Fetching %d days of daily OHLC for ID=%s", days, id)

	cacheProvider := "coingecko"
	// Use a distinct cache key for this daily data to avoid conflicts with chart data.
	cacheCoinID := fmt.Sprintf("%s-%s-daily-ohlc", id, vsCurrency)

	// --- Cache Checking Logic ---
	// Check if we have fresh daily data in the cache first.
	now := time.Now()
	startTime := now.AddDate(0, 0, -days)
	cachedBars, err := c.cache.GetBars(cacheProvider, cacheCoinID, startTime.UnixMilli(), now.UnixMilli())
	if err != nil {
		c.logger.LogWarn("cgclient GetOHLCDaily [%s]: Failed to get bars from cache: %v", id, err)
	}

	if len(cachedBars) > 0 {
		latestCachedBarTime := time.UnixMilli(cachedBars[len(cachedBars)-1].Timestamp)
		// Since this is daily data, we consider it fresh if the last bar is less than ~23 hours old.
		if time.Since(latestCachedBarTime) < 23*time.Hour {
			c.logger.LogInfo("cgclient GetOHLCDaily [%s]: Using %d fresh daily bars from cache.", id, len(cachedBars))
			return cachedBars, nil
		}
	}

	c.logger.LogInfo("cgclient GetOHLCDaily [%s]: Cache miss or stale daily data. Fetching from API.", id)

	// --- API Fetching Logic ---
	var ohlcData [][5]float64 // This is the specific response type for the /ohlc endpoint
	params := url.Values{}
	params.Add("vs_currency", strings.ToLower(vsCurrency))
	params.Add("days", strconv.Itoa(days))
	// No "interval" parameter is needed, as the /ohlc endpoint defaults to daily when `days` > 1.

	endpoint := fmt.Sprintf("/coins/%s/ohlc", id)
	err = c.request(ctx, endpoint, params, &ohlcData)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch daily OHLC data from CoinGecko for %s: %w", id, err)
	}

	if len(ohlcData) == 0 {
		c.logger.LogWarn("CoinGecko API returned no daily OHLC data for %s over %d days.", id, days)
		return []utils.OHLCVBar{}, nil
	}

	// --- Parsing and Caching Logic ---
	mergedBars := make([]utils.OHLCVBar, 0, len(ohlcData))
	for _, ohlcPoint := range ohlcData {
		if len(ohlcPoint) != 5 {
			continue // Skip malformed data points
		}
		// Volume is not provided by this endpoint, so it is set to 0.
		// This is acceptable as ATR, Stochastics, and candle patterns do not use volume.
		bar := utils.OHLCVBar{
			Timestamp: int64(ohlcPoint[0]),
			Open:      ohlcPoint[1],
			High:      ohlcPoint[2],
			Low:       ohlcPoint[3],
			Close:     ohlcPoint[4],
			Volume:    0,
		}
		mergedBars = append(mergedBars, bar)

		// Save the accurately fetched daily bar to the cache.
		if err := c.cache.SaveBar(cacheProvider, cacheCoinID, bar); err != nil {
			c.logger.LogWarn("cgclient GetOHLCDaily [%s]: Failed to save daily bar to cache: %v", id, err)
		}
	}

	sort.Slice(mergedBars, func(i, j int) bool {
		return mergedBars[i].Timestamp < mergedBars[j].Timestamp
	})

	return mergedBars, nil
}

// GetOHLCVHistorical fetches higher-resolution historical data using the /market_chart endpoint.
// This provides hourly data for requests of 2-90 days, which is better for the strategy's indicators.
// NOTE: This endpoint does not provide true Open, High, and Low values. They are approximated using the close price.
func (c *Client) GetOHLCVHistorical(ctx context.Context, id, vsCurrency, interval string) ([]utils.OHLCVBar, error) {
	// Use a default number of days for general purpose fetching.
	// Your PrimeHistoricalData function will handle variable-day lookbacks.
	days := 90
	c.logger.LogDebug("CoinGecko GetOHLCVHistorical: Fetching for ID=%s, VS=%s, Interval=%s, Days=%d", id, vsCurrency, interval, days)

	cacheProvider := "coingecko"
	// The cache key now reflects that it's chart data, not specific OHLC.
	cacheCoinID := fmt.Sprintf("%s-%s-%s-chart", id, vsCurrency, interval)

	// --- The original, robust cache-checking logic is preserved ---
	var expectedIntervalDuration time.Duration
	switch strings.ToLower(interval) {
	case "4h":
		expectedIntervalDuration = 4 * time.Hour
	case "1h":
		expectedIntervalDuration = 1 * time.Hour
	default:
		expectedIntervalDuration = 24 * time.Hour
	}

	now := time.Now()
	startTime := now.AddDate(0, 0, -days)

	cachedBars, err := c.cache.GetBars(cacheProvider, cacheCoinID, startTime.UnixMilli(), now.UnixMilli())
	if err != nil {
		c.logger.LogWarn("cgclient GetOHLCVHistorical [%s]: Failed to get bars from cache: %v", id, err)
	}

	// Check if the cached data is sufficient and recent
	if len(cachedBars) > 0 {
		latestCachedBarTime := time.UnixMilli(cachedBars[len(cachedBars)-1].Timestamp)
		if time.Since(latestCachedBarTime) < expectedIntervalDuration {
			c.logger.LogInfo("cgclient GetOHLCVHistorical [%s]: Using %d fresh bars from cache.", id, len(cachedBars))
			return cachedBars, nil
		}
	}

	c.logger.LogInfo("cgclient GetOHLCVHistorical [%s]: Cache miss or stale data. Fetching %d days from API.", id, days)

	// --- MODIFIED: API Fetching Logic for /market_chart ---
	var marketChartData cgMarketChartResponse // Use the correct struct for the new endpoint
	params := url.Values{}
	params.Add("vs_currency", strings.ToLower(vsCurrency))
	params.Add("days", strconv.Itoa(days))
	// The "interval" parameter is not used by the /market_chart endpoint and has been removed.

	endpoint := fmt.Sprintf("/coins/%s/market_chart", id) // Target the correct endpoint
	err = c.request(ctx, endpoint, params, &marketChartData)
	if err != nil {
		// More specific error message
		return nil, fmt.Errorf("failed to fetch market chart data from CoinGecko for %s: %w", id, err)
	}

	// Check if the primary data array is empty
	if len(marketChartData.Prices) == 0 {
		c.logger.LogWarn("CoinGecko API returned no price data for %s over %d days.", id, days)
		return []utils.OHLCVBar{}, nil
	}

	c.logger.LogInfo("Successfully fetched %d data points from CoinGecko for %s.", len(marketChartData.Prices), id)

	// --- MODIFIED: Parsing and Caching Logic for marketChartData ---
	numPoints := len(marketChartData.Prices)
	mergedBars := make([]utils.OHLCVBar, 0, numPoints)

	for i := 0; i < numPoints; i++ {
		pricePoint := marketChartData.Prices[i]
		if len(pricePoint) != 2 {
			c.logger.LogWarn("cgclient GetOHLCVHistorical [%s]: Malformed price point, skipping.", id)
			continue
		}

		// Safely get volume, defaulting to 0 if arrays are mismatched
		var volume float64
		if i < len(marketChartData.TotalVolumes) && len(marketChartData.TotalVolumes[i]) == 2 {
			volume = marketChartData.TotalVolumes[i][1]
		}

		price := pricePoint[1]
		bar := utils.OHLCVBar{
			Timestamp: int64(pricePoint[0]),
			// O, H, L are approximated with the closing price as this endpoint doesn't provide them.
			Open:   price,
			High:   price,
			Low:    price,
			Close:  price,
			Volume: volume,
		}
		mergedBars = append(mergedBars, bar)

		// Save each fetched bar to the cache
		if err := c.cache.SaveBar(cacheProvider, cacheCoinID, bar); err != nil {
			c.logger.LogWarn("cgclient GetOHLCVHistorical [%s]: Failed to save bar to cache: %v", id, err)
		}
	}

	// Sorting is still important as cache and new data might be combined in future logic.
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
	btcDominance, ok := cgGlobalResp.Data.MarketCapPercentage["btc"]
	if !ok {
		c.logger.LogWarn("GetGlobalMarketData: BTC Dominance not found in CoinGecko response.")
	}
	return dataprovider.GlobalMarketData{TotalMarketCap: totalMarketCap, BTCDominance: btcDominance}, nil
}

// GetTrendingSearches implements the DataProvider interface for CoinGecko.
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

// In coingecko/cgclient.go, add a placeholder implementation to satisfy the interface.
func (c *Client) GetGainersAndLosers(ctx context.Context, quoteCurrency string, topN int) ([]dataprovider.MarketData, []dataprovider.MarketData, error) {
	// CoinGecko API is not ideal for this specific "top 100 gainers" query compared to CMC.
	// We will rely on CMC for this feature.
	c.logger.LogDebug("CoinGecko provider does not implement GetGainersAndLosers. This is expected.")
	return nil, nil, nil
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
func (c *Client) GetCoinIDsBySymbol(ctx context.Context, sym string) ([]string, error) {
	// This is the correct way to use sync.Once for lazy initialization.
	// The `Do` call will block until the map has been populated.
	c.idMapOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := c.refreshCoinIDMapIfNeeded(ctx, true); err != nil {
			c.logger.LogError("CoinGecko Client: Initial coin ID map refresh failed: %v", err)
		}
	})

	// The rest of the function remains the same.
	c.idMapMu.RLock()
	defer c.idMapMu.RUnlock()

	loSym := strings.ToLower(sym)
	var matchingIDs []string

	// Filter the internal maps to find all matches, case-insensitively.
	for coinSymbol, coinID := range c.symbolToIDMap {
		if strings.EqualFold(coinSymbol, loSym) {
			matchingIDs = append(matchingIDs, coinID)
		}
	}

	// Also check for matches in CoinGecko's full coin name list.
	for coinName, coinID := range c.idMap {
		if strings.Contains(strings.ToLower(coinName), loSym) {
			matchingIDs = append(matchingIDs, coinID)
		}
	}

	if len(matchingIDs) == 0 {
		return nil, fmt.Errorf("no matching CoinGecko IDs found for symbol '%s'", sym)
	}

	// Remove duplicates
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range matchingIDs {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}

	return list, nil
}
func (c *Client) PrimeHistoricalData(ctx context.Context, id, vsCurrency, interval string, days int) error {
	c.logger.LogInfo("PRIMING CoinGecko Data: Fetching %d days for ID=%s, Interval=%s", days, id, interval)

	if days <= 0 {
		return fmt.Errorf("priming days must be positive, got %d", days)
	}

	var ohlcData []cgOHLCDataPoint
	ohlcParams := url.Values{}
	ohlcParams.Add("vs_currency", strings.ToLower(vsCurrency))
	ohlcParams.Add("days", strconv.Itoa(days))
	if strings.ToLower(interval) == "daily" || strings.ToLower(interval) == "1d" {
		ohlcParams.Add("interval", "daily") // Force daily for Analyst plan
	}
	ohlcEndpoint := fmt.Sprintf("/coins/%s/ohlc", id)

	err := c.request(ctx, ohlcEndpoint, ohlcParams, &ohlcData)
	if err != nil {
		return fmt.Errorf("failed to fetch priming data from CoinGecko /ohlc for %s: %w", id, err)
	}

	cacheProvider := "coingecko"
	cacheCoinID := fmt.Sprintf("%s-%s-%s", id, strings.ToLower(vsCurrency), interval)

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
		}
		if err := c.cache.SaveBar(cacheProvider, cacheCoinID, bar); err != nil {
			c.logger.LogWarn("PrimeHistoricalData [%s]: Failed to save bar to cache: %v", id, err)
		}
	}
	c.logger.LogInfo("PRIMING CoinGecko Data: Successfully processed and cached %d data points for %s.", len(ohlcData), id)
	return nil
}
