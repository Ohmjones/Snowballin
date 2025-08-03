package utilities

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// LogLevel defines the severity of a log message.
type LogLevel int

// Logging Level
const (
	Debug LogLevel = iota
	Info
	Warn
	Error
	Fatal
)

// Colors.
const (
	ColorReset  = "\033[0m"
	ColorYellow = "\033[93m" // For asset pairs
	ColorCyan   = "\033[96m" // For Buy signals
	ColorRed    = "\033[91m" // For Sell signals
	ColorWhite  = "\033[97m" // For Hold signals
)

// --- Global Logger ---
var globalLogger = NewLogger(Info) // Default to Info

// --- Types (Alphabetized) ---

// AppConfig is the root configuration structure, holding all other config sections.
type AppConfig struct {
	AppName             string                 `mapstructure:"app_name"`
	Version             string                 `mapstructure:"version"`
	Environment         string                 `mapstructure:"environment"`
	CircuitBreaker      CircuitBreakerConfig   `mapstructure:"circuit_breaker"`
	Coingecko           *CoingeckoConfig       `mapstructure:"coingecko"`
	Coinmarketcap       *CoinmarketcapConfig   `mapstructure:"coinmarketcap"`
	Consensus           ConsensusConfig        `mapstructure:"consensus"`
	DataCompleteness    DataCompletenessConfig `mapstructure:"data_completeness"`
	DataProviderWeights map[string]float64     `mapstructure:"dataprovider_weights"`
	DataDiscrepancy     DataDiscrepancyConfig  `mapstructure:"data_discrepancy"`
	DB                  DatabaseConfig         `mapstructure:"database"`
	Discord             DiscordConfig          `mapstructure:"discord"`
	ExitStrategy        ExitStrategyConfig     `mapstructure:"exit_strategy"`
	FearGreed           *FearGreedConfig       `mapstructure:"FearGreed"`
	Indicators          IndicatorsConfig       `mapstructure:"indicators"`
	Kraken              KrakenConfig           `mapstructure:"kraken"`
	Logging             LoggingConfig          `mapstructure:"logging"`
	Orders              OrdersConfig           `mapstructure:"orders"`
	Trading             TradingConfig          `mapstructure:"trading"`
	Withdrawal          WithdrawalConfig       `mapstructure:"withdrawal"`
	Preflight           PreflightConfig        `mapstructure:"preflight"`
}

// CircuitBreakerConfig defines parameters to halt trading during extreme market conditions.
type CircuitBreakerConfig struct {
	Enabled                  bool    `mapstructure:"enabled"`
	DrawdownThresholdPercent float64 `mapstructure:"drawdown_threshold_percent"`
	ATRVolatilityThreshold   float64 `mapstructure:"atr_volatility_threshold"`
}

// CoingeckoConfig holds settings for the CoinGecko data provider.
type CoingeckoConfig struct {
	BaseURL                   string `mapstructure:"base_url"`
	APIKey                    string `mapstructure:"api_key"`
	RequestTimeoutSec         int    `mapstructure:"request_timeout_sec"`
	MaxRetries                int    `mapstructure:"max_retries"`
	RetryDelaySec             int    `mapstructure:"retry_delay_sec"`
	OHLCVDaysDefault          string `mapstructure:"ohlcv_days_default"`
	IDMapRefreshIntervalHours int    `mapstructure:"id_map_refresh_interval_hours"`
	RateLimitPerSec           int    `mapstructure:"rate_limit_per_sec"`
	RateLimitBurst            int    `mapstructure:"rate_limit_burst"`
	CacheBasePath             string `mapstructure:"cache_base_path"`
	CacheRetentionDays        int    `mapstructure:"cache_retention_days"`
	QuoteCurrency             string `mapstructure:"quote_currency"`
}

// CoinmarketcapConfig holds settings for the CoinMarketCap data provider.
type CoinmarketcapConfig struct {
	BaseURL                   string            `mapstructure:"base_url"`
	APIKey                    string            `mapstructure:"api_key"`
	SymbolOverrides           map[string]string `mapstructure:"symbol_overrides"`
	QuoteCurrency             string            `mapstructure:"quote_currency"`
	RequestTimeoutSec         int               `mapstructure:"request_timeout_sec"`
	MaxRetries                int               `mapstructure:"max_retries"`
	RetryDelaySec             int               `mapstructure:"retry_delay_sec"`
	OHLCVDaysDefault          int               `mapstructure:"ohlcv_days_default"`
	IDMapRefreshIntervalHours int               `mapstructure:"id_map_refresh_interval_hours"`
	RateLimitPerSec           int               `mapstructure:"rate_limit_per_sec"`
	RateLimitBurst            int               `mapstructure:"rate_limit_burst"`
	CacheBasePath             string            `mapstructure:"cache_base_path"`
}

// ConsensusConfig defines how different signals are weighted to reach a trading decision.
type ConsensusConfig struct {
	UseMultiTimeframeConsensus bool          `mapstructure:"use_multi_timeframe_consensus"` // New field to enable MTF
	MultiTimeframe             MultiTFConfig `mapstructure:"multi_timeframe"`
}

// DatabaseConfig holds settings for database connections.
type DatabaseConfig struct {
	DBPath string `mapstructure:"database_path"`
}

// DataCompletenessConfig defines how the bot handles missing data from providers.
type DataCompletenessConfig struct {
	SubstitutionSources         []string `mapstructure:"substitution_sources"`
	DiscrepancyThresholdPercent float64  `mapstructure:"discrepancy_threshold_percent"`
	LogLevelOnSubstitution      string   `mapstructure:"log_level_on_substitution"`
	LogLevelOnDiscrepancy       string   `mapstructure:"log_level_on_discrepancy"`
}

// DataDiscrepancyConfig defines how to act when data providers show conflicting data.
type DataDiscrepancyConfig struct {
	ThresholdPercent         float64 `mapstructure:"threshold_percent"`
	ConsecutiveCyclesTrigger int     `mapstructure:"consecutive_cycles_trigger"`
	Action                   string  `mapstructure:"action"`
}

// DiscordConfig holds settings for sending notifications via Discord.
type DiscordConfig struct {
	WebhookURL string `mapstructure:"webhook_url"`
}

// ExitStrategyConfig defines parameters for taking profits or cutting losses.
type ExitStrategyConfig struct {
	FlushThresholdATR       float64 `mapstructure:"flush_threshold_atr"`
	MaxProfitExitPercentage int     `mapstructure:"max_profit_exit_percentage"`
}

// FearGreedConfig holds settings for the Fear & Greed Index data source.
type FearGreedConfig struct {
	BaseURL              string `mapstructure:"baseURL"`
	RefreshIntervalHours int    `mapstructure:"refreshIntervalHours"`
}

// IndicatorsConfig holds parameters for various technical indicators.
type IndicatorsConfig struct {
	RSIPeriod             int                 `mapstructure:"rsi_period"`
	StochRSIPeriod        int                 `mapstructure:"stoch_rsi_period"`
	StochRSIBuyThreshold  float64             `mapstructure:"stoch_rsi_buy_threshold"`
	StochRSISellThreshold float64             `mapstructure:"stoch_rsi_sell_threshold"`
	MACDFastPeriod        int                 `mapstructure:"macd_fast_period"`
	MACDSlowPeriod        int                 `mapstructure:"macd_slow_period"`
	MACDSignalPeriod      int                 `mapstructure:"macd_signal_period"`
	ATRPeriod             int                 `mapstructure:"atr_period"`
	VolumeSpikeFactor     float64             `mapstructure:"volume_spike_factor"`
	VolumeLookbackPeriod  int                 `mapstructure:"volume_lookback_period"`
	OBVPositiveThreshold  float64             `mapstructure:"obv_positive_threshold"`
	OBVNegativeThreshold  float64             `mapstructure:"obv_negative_threshold"`
	OBVMAPeriod           int                 `mapstructure:"obv_ma_period"`
	LiquidityHunt         LiquidityHuntConfig `mapstructure:"liquidity_hunt"`
	StochasticK           int                 `mapstructure:"stochastic_k_period"` // e.g., 14
	StochasticD           int                 `mapstructure:"stochastic_d_period"` // e.g., 3
}

// InMemoryNonceCounter provides a simple in-memory nonce generator.
type InMemoryNonceCounter struct {
	mu        sync.Mutex
	lastNonce int64
}

// Next generates the next nonce value.
func (nc *InMemoryNonceCounter) Next() string {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	now := time.Now().UnixNano()
	if now <= nc.lastNonce {
		now = nc.lastNonce + 1
	}
	nc.lastNonce = now
	return strconv.FormatInt(now, 10)
}

// KrakenConfig holds all settings for the Kraken exchange integration.
type KrakenConfig struct {
	APIKey                string `mapstructure:"api_key"`
	APISecret             string `mapstructure:"api_secret"`
	BaseURL               string `mapstructure:"base_url"`
	MaxRetries            int    `mapstructure:"max_retries"`
	NonceCounter          NonceCounter
	OrderPlacementDelayMs int    `mapstructure:"order_placement_delay_ms"`
	OTPSecret             string `mapstructure:"otp_secret"`
	QuoteCurrency         string `mapstructure:"quote_currency"`
	RateBurst             int
	RateLimitBurst        int
	RateLimitPerSec       rate.Limit
	RefreshIntervalHours  time.Duration `mapstructure:"refresh_interval_hours"`
	RequestTimeoutSec     int           `mapstructure:"request_timeout_sec"`
	RetryDelaySec         int           `mapstructure:"retry_delay_sec"`
	UseOTP                bool          `mapstructure:"use_otp"`
}

// KrakenNonceGenerator generates nonces for Kraken API requests.
type KrakenNonceGenerator struct {
	counter uint64
	mu      sync.Mutex
}

// NewNonceCounter creates a new KrakenNonceGenerator.
func NewNonceCounter() *KrakenNonceGenerator {
	return &KrakenNonceGenerator{counter: uint64(time.Now().UnixNano())}
}

// Nonce generates and returns a new unique nonce.
func (n *KrakenNonceGenerator) Nonce() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.counter++
	return n.counter
}

// LiquidityHuntConfig holds parameters for the liquidity hunt indicator.
type LiquidityHuntConfig struct {
	MinWickPercent     float64 `mapstructure:"min_wick_percent"`
	VolSpikeMultiplier float64 `mapstructure:"vol_spike_multiplier"`
	VolMAPeriod        int     `mapstructure:"vol_ma_period"`
}

// Logger provides a structured logger with different levels.
type Logger struct {
	Level  LogLevel
	Logger *log.Logger
}

// NewLogger creates a new Logger instance.
func NewLogger(level LogLevel) *Logger {
	return &Logger{
		Level:  level,
		Logger: log.New(os.Stdout, "[Snowballin] ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

// LogDebug logs a message at Debug level.
func (l *Logger) LogDebug(format string, v ...interface{}) {
	if l.Level <= Debug {
		_ = l.Logger.Output(2, fmt.Sprintf("[DEBUG] "+format, v...))
	}
}

// LogError logs a message at Error level.
func (l *Logger) LogError(format string, v ...interface{}) {
	if l.Level <= Error {
		_ = l.Logger.Output(2, fmt.Sprintf("[ERROR] "+format, v...))
	}
}

// LogFatal logs a message at Fatal level and then calls os.Exit(1).
func (l *Logger) LogFatal(format string, v ...interface{}) {
	_ = l.Logger.Output(2, fmt.Sprintf("[FATAL] "+format, v...))
	os.Exit(1)
}

// LogInfo logs a message at Info level.
func (l *Logger) LogInfo(format string, v ...interface{}) {
	if l.Level <= Info {
		_ = l.Logger.Output(2, fmt.Sprintf("[INFO] "+format, v...))
	}
}

// LogWarn logs a message at Warn level.
func (l *Logger) LogWarn(format string, v ...interface{}) {
	if l.Level <= Warn {
		_ = l.Logger.Output(2, fmt.Sprintf("[WARN] "+format, v...))
	}
}

// SetLogLevel updates the logging level of the logger.
func (l *Logger) SetLogLevel(level LogLevel) {
	l.Level = level
}

// LoggingConfig holds settings related to logging.
type LoggingConfig struct {
	Level           string `mapstructure:"level"`
	LogToFile       bool   `mapstructure:"log_to_file"`
	LogFilePath     string `mapstructure:"log_file_path"`
	MaxSizeMB       int    `mapstructure:"max_size_mb"`
	MaxBackups      int    `mapstructure:"max_backups"`
	MaxAgeDays      int    `mapstructure:"max_age_days"`
	CompressBackups bool   `mapstructure:"compress_backups"`
}

// minInt returns the minimum of two integers.
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MultiTFConfig holds settings for multi-timeframe analysis.
type MultiTFConfig struct {
	BaseTimeframe        string   `mapstructure:"base_timeframe"`
	AdditionalTimeframes []string `mapstructure:"additional_timeframes"`
	TFLookbackLengths    []int    `mapstructure:"tf_lookback_lengths"`
}

// NonceCounter defines an interface for generating unique nonces.
type NonceCounter interface {
	Next() string
}

// OHLCVBar represents a single Open, High, Low, Close, Volume data point.
type OHLCVBar struct {
	Timestamp int64   `json:"timestamp"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
}

// OrdersConfig holds settings for the order watcher and reaper.
type OrdersConfig struct {
	EnableWatcher                 bool    `mapstructure:"enable_watcher"`
	WatcherIntervalSec            int     `mapstructure:"watcher_interval_sec"`
	ReaperIntervalSec             int     `mapstructure:"reaper_interval_sec"`
	MaxOrderAgeMinutesForGridBase float64 `mapstructure:"max_order_age_minutes_for_grid_base"`
	MinOrderAgeMinutesDynamicCap  float64 `mapstructure:"min_order_age_minutes_dynamic_cap"`
	MaxOrderAgeMinutesDynamicCap  float64 `mapstructure:"max_order_age_minutes_dynamic_cap"`
}

// Position holds the state of an active trade, updated to support the Martingale DCA strategy.
type Position struct {
	AssetPair      string    `json:"asset_pair"`
	EntryTimestamp time.Time `json:"entry_timestamp"`

	// Core DCA State
	AveragePrice       float64 `json:"average_price"`
	TotalVolume        float64 `json:"total_volume"`
	BaseOrderPrice     float64 `json:"base_order_price"`
	FilledSafetyOrders int     `json:"filled_safety_orders"`
	IsDcaActive        bool    `json:"is_dca_active"`
	BaseOrderSize      float64 `json:"base_order_size"`

	// Target and Management
	CurrentTakeProfit float64 `json:"current_take_profit"`
	BrokerOrderID     string  `json:"broker_order_id"` // Tracks the initial base order ID

	// Trailing Stop Loss
	IsTrailingActive    bool    `json:"is_trailing_active"`
	PeakPriceSinceTP    float64 `json:"peak_price_since_tp"`
	UnrealizedPL        float64 `json:"-"`
	UnrealizedPLPercent float64 `json:"-"`
}

type PreflightConfig struct {
	PrimeHistoricalData bool `mapstructure:"prime_historical_data"`
	PrimingDays         int  `mapstructure:"priming_days"`
}

// TradingConfig holds general trading parameters.
type TradingConfig struct {
	AssetPairs                       []string `mapstructure:"asset_pairs"`
	QuoteCurrency                    string   `mapstructure:"quote_currency"`
	MaxPortfolioDrawdown             float64  `mapstructure:"max_portfolio_drawdown"`
	PortfolioRiskPerTrade            float64  `mapstructure:"portfolio_risk_per_trade"`
	TakeProfitPartialSellPercent     float64  `mapstructure:"take_profit_partial_sell_percent"`
	DustThresholdUSD                 float64  `mapstructure:"dust_threshold_usd"`
	TrailingStopDeviation            float64  `mapstructure:"trailing_stop_deviation"`
	UseOrderBookForPredictive        bool     `mapstructure:"use_order_book_for_predictive"`
	PredictiveOrderSizePercent       float64  `mapstructure:"predictive_order_size_percent"` // This will be deprecated by the new logic
	MinBookConfidenceForPredictive   float64  `mapstructure:"min_book_confidence_for_predictive"`
	DcaSpacingMode                   string   `mapstructure:"dca_spacing_mode"`
	DcaAtrPeriod                     int      `mapstructure:"dca_atr_period"`
	DcaAtrSpacingMultiplier          float64  `mapstructure:"dca_atr_spacing_multiplier"`
	BaseOrderSize                    float64  `mapstructure:"base_order_size"`
	SafetyOrderSize                  float64  `mapstructure:"safety_order_size"`
	MaxSafetyOrders                  int      `mapstructure:"max_safety_orders"`
	SafetyOrderVolumeScale           float64  `mapstructure:"safety_order_volume_scale"`
	SafetyOrderStepScale             float64  `mapstructure:"safety_order_step_scale"`
	PriceDeviationToOpenSafetyOrders float64  `mapstructure:"price_deviation_to_open_safety_orders"`
	TakeProfitPercentage             float64  `mapstructure:"take_profit_percentage"`
	TrailingStopEnabled              bool     `mapstructure:"trailing_stop_enabled"`
	StopLossEnabled                  bool     `mapstructure:"stop_loss_enabled"`
	RecalculateAfterDcaFill          bool     `mapstructure:"recalculate_after_dca_fill"`
	UseMartingaleForPredictive       bool     `mapstructure:"use_martingale_for_predictive"`
	PredictiveBuyDeviationPercent    float64  `mapstructure:"predictive_buy_deviation_percent"`
	ConsensusBuyMultiplier           float64  `mapstructure:"consensus_buy_multiplier"`
	PredictiveRsiThreshold           float64  `mapstructure:"predictive_rsi_threshold"`
	ConsensusRsiThreshold            float64  `mapstructure:"consensus_rsi_threshold"`
	RsiAdjustFactor                  float64  `mapstructure:"rsi_adjust_factor"`
	PredictiveAtrMultiplier          float64  `mapstructure:"predictive_atr_multiplier"`
	PredictiveSearchWindowPercent    float64  `mapstructure:"predictive_search_window_percent"`
	UseFearOverride                  bool     `mapstructure:"use_fear_override"`
	FearOverrideThreshold            int      `mapstructure:"fear_override_threshold"`
}

// WithdrawalConfig holds settings for automated withdrawal of funds.
type WithdrawalConfig struct {
	Enabled          bool              `mapstructure:"enabled"`
	WithdrawUSD      float64           `mapstructure:"withdraw_usd"`
	WithdrawKeys     map[string]string `mapstructure:"withdraw_keys"`
	CheckIntervalSec int               `mapstructure:"check_interval_sec"`
	CooldownMinutes  int               `mapstructure:"cooldown_minutes"`
}

// --- Standalone Functions (Alphabetized) ---

// ConvertTFToKrakenInterval converts a standard timeframe string (e.g., "1h") to Kraken's format ("60").
func ConvertTFToKrakenInterval(tf string) (string, error) {
	switch strings.ToLower(tf) {
	case "1m":
		return "1", nil
	case "5m":
		return "5", nil
	case "15m":
		return "15", nil
	case "30m":
		return "30", nil
	case "1h":
		return "60", nil
	case "4h":
		return "240", nil
	case "1d":
		return "1440", nil
	case "1w":
		return "10080", nil
	default:
		return "", fmt.Errorf("unsupported timeframe for Kraken interval conversion: %s", tf)
	}
}

// DoJSONRequest performs an HTTP request, retries on transient errors, and unmarshals a JSON response.
func DoJSONRequest(client *http.Client, req *http.Request, maxRetries int, retryDelay time.Duration, result interface{}) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		r := req
		if attempt > 0 && req.GetBody != nil {
			bodyReader, err := req.GetBody()
			if err != nil {
				return fmt.Errorf("retry %d: could not reset request body: %w", attempt, err)
			}
			r = req.Clone(req.Context())
			r.Body = bodyReader
		}

		resp, err := client.Do(r)
		if err != nil {
			lastErr = err
			time.Sleep(retryDelay)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
			lastErr = fmt.Errorf("server error %d %s", resp.StatusCode, resp.Status)
			time.Sleep(retryDelay)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return fmt.Errorf("http %d: %s", resp.StatusCode, string(snippet))
		}

		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("failed to decode JSON response: %w", err)
		}
		return nil
	}
	return fmt.Errorf("all retries failed: %w", lastErr)
}

// FilterAfter returns a subset of items that occur after a given cutoff time.
func FilterAfter[T any](items []T, getTime func(T) time.Time, cutoff time.Time) []T {
	var out []T
	for _, it := range items {
		if getTime(it).After(cutoff) {
			out = append(out, it)
		}
	}
	return out
}

// GenerateKrakenAuthHeaders builds the API-Key and API-Sign headers for Kraken private endpoints.
func GenerateKrakenAuthHeaders(apiKey, apiSecret, apiPath, nonce string, data url.Values) (map[string]string, error) {
	postData := data.Encode()
	sha256Hasher := sha256.New()
	sha256Hasher.Write([]byte(nonce + postData))
	shaSum := sha256Hasher.Sum(nil)

	message := append([]byte(apiPath), shaSum...)

	decodedSecret, err := base64.StdEncoding.DecodeString(apiSecret)
	if err != nil {
		return nil, err
	}

	mac := hmac.New(sha512.New, decodedSecret)
	mac.Write(message)
	macSum := mac.Sum(nil)

	signature := base64.StdEncoding.EncodeToString(macSum)

	return map[string]string{
		"API-Key":  apiKey,
		"API-Sign": signature,
	}, nil
}

// LogDebugF is a package-level convenience function for Debug logging.
func LogDebugF(format string, v ...interface{}) {
	globalLogger.LogDebug(format, v...)
}

// LogInfoF is a package-level convenience function for Info logging.
func LogInfoF(format string, v ...interface{}) {
	globalLogger.LogInfo(format, v...)
}

// LogWarnF is a package-level convenience function for Warn logging.
func LogWarnF(format string, v ...interface{}) {
	globalLogger.LogWarn(format, v...)
}

// ParseFloatFromInterface is a helper function to parse float64 from various numeric types.
func ParseFloatFromInterface(val interface{}) (float64, error) {
	switch v := val.(type) {
	case string:
		return strconv.ParseFloat(v, 64)
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case json.Number:
		return v.Float64()
	default:
		return 0, fmt.Errorf("unsupported type for float conversion: %T", v)
	}
}

// ParseLogLevel converts a string log level to the LogLevel type.
func ParseLogLevel(levelStr string) (LogLevel, error) {
	switch strings.ToLower(levelStr) {
	case "debug":
		return Debug, nil
	case "info":
		return Info, nil
	case "warn":
		return Warn, nil
	case "error":
		return Error, nil
	case "fatal":
		return Fatal, nil
	default:
		return Info, fmt.Errorf("invalid log level string: %s", levelStr)
	}
}

// SortBarsByTimestamp sorts a slice of OHLCVBar by ascending Timestamp.
func SortBarsByTimestamp(bars []OHLCVBar) {
	sort.Slice(bars, func(i, j int) bool {
		return bars[i].Timestamp < bars[j].Timestamp
	})
}
