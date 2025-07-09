// Package utils provides utility functions, shared types, and configurations for the Snowballin trading bot.
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
// Higher values indicate higher severity (though iota makes Debug=0 the lowest).
type LogLevel int

const (
	Debug LogLevel = iota // Debug messages for detailed tracing.
	Info                  // Info messages for general operational information.
	Warn                  // Warn messages for potential issues.
	Error                 // Error messages for recoverable errors.
	Fatal                 // Fatal messages for non-recoverable errors that cause termination.
)

// Position holds the state of an active trade for a specific asset pair.
type Position struct {
	AssetPair         string
	EntryPrice        float64
	Volume            float64
	EntryTimestamp    time.Time
	CurrentStopLoss   float64
	CurrentTakeProfit float64
	BrokerOrderID     string
}
type DCAConfig struct {
	NumLadders       int       `mapstructure:"num_ladders"`
	DCALevels        []float64 `mapstructure:"dca_levels"`
	DCAATRMultiplier float64   `mapstructure:"dca_atr_multiplier"`
}

type DiscordConfig struct {
	WebhookURL string `mapstructure:"webhook_url"`
}

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
	QuoteCurrency             string `mapstructure:"quote_currency"` // This seems like a good place for default quote
}

type CoinmarketcapConfig struct {
	BaseURL                   string `mapstructure:"base_url"`
	APIKey                    string `mapstructure:"api_key"`
	QuoteCurrency             string `mapstructure:"quote_currency"`
	RequestTimeoutSec         int    `mapstructure:"request_timeout_sec"`
	MaxRetries                int    `mapstructure:"max_retries"`
	RetryDelaySec             int    `mapstructure:"retry_delay_sec"`
	OHLCVDaysDefault          int    `mapstructure:"ohlcv_days_default"` // Note: common.go has int, config.json might imply string. Ensure consistency or parse.
	IDMapRefreshIntervalHours int    `mapstructure:"id_map_refresh_interval_hours"`
	RateLimitPerSec           int    `mapstructure:"rate_limit_per_sec"`
	RateLimitBurst            int    `mapstructure:"rate_limit_burst"`
	CacheBasePath             string `mapstructure:"cache_base_path"`
}

type KrakenConfig struct {
	APIKey                string `mapstructure:"api_key"`
	APISecret             string `mapstructure:"api_secret"`
	RequestTimeoutSec     int    `mapstructure:"request_timeout_sec"`
	OrderPlacementDelayMs int    `mapstructure:"order_placement_delay_ms"`
	BaseURL               string `mapstructure:"base_url"`
	MaxRetries            int    `mapstructure:"max_retries"`
	RetryDelaySec         int    `mapstructure:"retry_delay_sec"`
	NonceCounter          NonceCounter
	RateLimitPerSec       rate.Limit
	RateLimitBurst        int
	RefreshIntervalHours  time.Duration `mapstructure:"refresh_interval_hours"` // Ensure type consistency with how it's set/used
	RateBurst             int           // This might be redundant with RateLimitBurst
	UseOTP                bool          `mapstructure:"use_otp"`    // From config.json
	OTPSecret             string        `mapstructure:"otp_secret"` // From config.json
	QuoteCurrency         string        `mapstructure:"quote_currency"`
}

type WithdrawalConfig struct {
	Enabled          bool              `mapstructure:"enabled"`
	WithdrawUSD      float64           `mapstructure:"withdraw_usd"`
	WithdrawKeys     map[string]string `mapstructure:"withdraw_keys"`
	CheckIntervalSec int               `mapstructure:"check_interval_sec"`
	CooldownMinutes  int               `mapstructure:"cooldown_minutes"`
}

type TradingConfig struct {
	AssetPairs                []string `mapstructure:"asset_pairs"`
	QuoteCurrency             string   `mapstructure:"quote_currency"`
	MaxPortfolioDrawdown      float64  `mapstructure:"max_portfolio_drawdown"` // Critical for the circuit breaker
	LiquiditySellPercentage   float64  `mapstructure:"liquidity_sell_percentage"`
	MinPositionHoldRatio      float64  `mapstructure:"min_position_hold_ratio"`
	ATRStopLossMultiplier     float64  `mapstructure:"atr_stop_loss_multiplier"`
	ATRTakeProfitMultiplier   float64  `mapstructure:"atr_take_profit_multiplier"`
	TrailingStopEnabled       bool     `mapstructure:"trailing_stop_enabled"`
	TrailingTakeProfitEnabled bool     `mapstructure:"trailing_take_profit_enabled"`

	// These fields appear to be from older designs but are kept for completeness
	// to match some older files. They are not used in the final logic.
	DefaultStopLoss         float64 `mapstructure:"default_stop_loss"`
	DefaultTakeProfit       float64 `mapstructure:"default_take_profit"`
	DcaAtrSpacingMultiplier float64 `mapstructure:"dca_atr_spacing_multiplier"`
	RecalculateAfterDcaFill bool    `mapstructure:"recalculate_after_dca_fill"`
	PreferredDataProvider   string  `mapstructure:"preferred_dataprovider"`
	RiskProfile             string  `mapstructure:"risk_profile"`
	PortfolioValue          float64
}

// KrakenNonceGenerator generates nonces for Kraken API requests.
type KrakenNonceGenerator struct {
	counter uint64
	mu      sync.Mutex
}

func NewNonceCounter() *KrakenNonceGenerator {
	return &KrakenNonceGenerator{counter: uint64(time.Now().UnixNano())}
}

func (n *KrakenNonceGenerator) Nonce() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.counter++
	return n.counter
}

type IndicatorsConfig struct {
	RSIPeriod             int     `mapstructure:"rsi_period"`
	StochRSIPeriod        int     `mapstructure:"stoch_rsi_period"`
	StochRSIBuyThreshold  float64 `mapstructure:"stoch_rsi_buy_threshold"`
	StochRSISellThreshold float64 `mapstructure:"stoch_rsi_sell_threshold"`
	MACDFastPeriod        int     `mapstructure:"macd_fast_period"`
	MACDSlowPeriod        int     `mapstructure:"macd_slow_period"`
	MACDSignalPeriod      int     `mapstructure:"macd_signal_period"`
	ATRPeriod             int     `mapstructure:"atr_period"`
	VolumeSpikeFactor     float64 `mapstructure:"volume_spike_factor"`
	VolumeLookbackPeriod  int     `mapstructure:"volume_lookback_period"`
	OBVPositiveThreshold  float64 `mapstructure:"obv_positive_threshold"`
	OBVNegativeThreshold  float64 `mapstructure:"obv_negative_threshold"`
}
type OrdersConfig struct {
	EnableWatcher                 bool    `mapstructure:"enable_watcher"`
	WatcherIntervalSec            int     `mapstructure:"watcher_interval_sec"`
	ReaperIntervalSec             int     `mapstructure:"reaper_interval_sec"`
	MaxOrderAgeMinutesForGridBase float64 `mapstructure:"max_order_age_minutes_for_grid_base"`
	MinOrderAgeMinutesDynamicCap  float64 `mapstructure:"min_order_age_minutes_dynamic_cap"`
	MaxOrderAgeMinutesDynamicCap  float64 `mapstructure:"max_order_age_minutes_dynamic_cap"`
}

type ConsensusConfig struct {
	ThresholdBuy                 float64       `mapstructure:"threshold_buy"`
	ThresholdSell                float64       `mapstructure:"threshold_sell"`
	CoinGeckoProviderWeightShare float64       `mapstructure:"coin_gecko_provider_weight_share"`
	MultiTimeframe               MultiTFConfig `mapstructure:"multi_timeframe"`
}

type MultiTFConfig struct {
	BaseTimeframe        string   `mapstructure:"base_timeframe"`
	AdditionalTimeframes []string `mapstructure:"additional_timeframes"`
	TFLookbackLengths    []int    `mapstructure:"tf_lookback_lengths"`
}

type LoggingConfig struct {
	Level           string `mapstructure:"level"`
	LogToFile       bool   `mapstructure:"log_to_file"`      // From config.json
	LogFilePath     string `mapstructure:"log_file_path"`    // From config.json
	MaxSizeMB       int    `mapstructure:"max_size_mb"`      // From config.json
	MaxBackups      int    `mapstructure:"max_backups"`      // From config.json
	MaxAgeDays      int    `mapstructure:"max_age_days"`     // From config.json
	CompressBackups bool   `mapstructure:"compress_backups"` // From config.json
}

type DatabaseConfig struct {
	DBPath string `mapstructure:"database_path"`
}

type FearGreedConfig struct {
	BaseURL              string `mapstructure:"baseURL"`
	RefreshIntervalHours int    `mapstructure:"refreshIntervalHours"`
}
type AppConfig struct {
	AppName             string               `mapstructure:"app_name"`
	Version             string               `mapstructure:"version"`
	Environment         string               `mapstructure:"environment"`
	Discord             DiscordConfig        `mapstructure:"discord"`
	Kraken              KrakenConfig         `mapstructure:"kraken"`
	Coingecko           *CoingeckoConfig     `mapstructure:"coingecko"`
	Coinmarketcap       *CoinmarketcapConfig `mapstructure:"coinmarketcap"`
	DataProviderWeights map[string]float64   `mapstructure:"dataprovider_weights"`
	Withdrawal          WithdrawalConfig     `mapstructure:"withdrawal"`
	Trading             TradingConfig        `mapstructure:"trading"`
	DCA                 DCAConfig            `mapstructure:"dca"`
	Orders              OrdersConfig         `mapstructure:"orders"`
	Indicators          IndicatorsConfig     `mapstructure:"indicators"`
	Consensus           ConsensusConfig      `mapstructure:"consensus"`
	Logging             LoggingConfig        `mapstructure:"logging"`
	DB                  DatabaseConfig       `mapstructure:"db"`
	Debug               LogLevel
	FearGreed           *FearGreedConfig `mapstructure:"FearGreed"`
}

func ConvertTFToKrakenInterval(tf string) (string, error) {
	// Kraken intervals: 1 (1m), 5 (5m), 15 (15m), 30 (30m), 60 (1h), 240 (4h), 1440 (1d), 10080 (1w), 21600 (15d)
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
	// case "15d": // Kraken uses 21600 for 15 days, but it's less common for typical TA timeframes.
	//	return "21600", nil
	default:
		return "", fmt.Errorf("unsupported timeframe for Kraken interval conversion: %s. Supported: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w", tf)
	}
}

// --- LogLevel and Logger ---
// NonceCounter defines an interface for generating unique nonces.
type NonceCounter interface {
	Next() string
}

// InMemoryNonceCounter provides a simple in-memory nonce generator.
type InMemoryNonceCounter struct {
	mu        sync.Mutex
	lastNonce int64
}

// FilterAfter returns those items for which getTime(item).After(cutoff) is true.
func FilterAfter[T any](items []T, getTime func(T) time.Time, cutoff time.Time) []T {
	var out []T
	for _, it := range items {
		if getTime(it).After(cutoff) {
			out = append(out, it)
		}
	}
	return out
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

type Logger struct {
	Level  LogLevel
	Logger *log.Logger
}

// NewLogger creates a new Logger instance.
// It defaults to os.Stdout with standard log flags.
func NewLogger(level LogLevel) *Logger {
	return &Logger{
		Level:  level,
		Logger: log.New(os.Stdout, "[Snowballin] ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

// SetLogLevel updates the logging level of the logger.
func (l *Logger) SetLogLevel(level LogLevel) {
	l.Level = level
}

// LogDebug logs a message at Debug level.
func (l *Logger) LogDebug(format string, v ...interface{}) {
	if l.Level <= Debug {
		err := l.Logger.Output(2, fmt.Sprintf("[DEBUG] "+format, v...))
		if err != nil {
			return
		}
	}
}

// LogInfo logs a message at Info level.
func (l *Logger) LogInfo(format string, v ...interface{}) {
	if l.Level <= Info {
		err := l.Logger.Output(2, fmt.Sprintf("[INFO] "+format, v...))
		if err != nil {
			return
		}
	}
}

// LogWarn logs a message at Warn level.
func (l *Logger) LogWarn(format string, v ...interface{}) {
	if l.Level <= Warn {
		err := l.Logger.Output(2, fmt.Sprintf("[WARN] "+format, v...))
		if err != nil {
			return
		}
	}
}

// LogError logs a message at Error level.
func (l *Logger) LogError(format string, v ...interface{}) {
	if l.Level <= Error {
		err := l.Logger.Output(2, fmt.Sprintf("[ERROR] "+format, v...))
		if err != nil {
			return
		}
	}
}

// LogFatal logs a message at Fatal level and then calls os.Exit(1).
func (l *Logger) LogFatal(format string, v ...interface{}) {
	// No level check for Fatal, it always logs and exits.
	err := l.Logger.Output(2, fmt.Sprintf("[FATAL] "+format, v...))
	if err != nil {
		return
	}
	os.Exit(1)
}

// --- Global Logger (optional, for direct package-level calls if needed) ---
var globalLogger = NewLogger(Info) // Default to Info

// LogInfoF is a package-level convenience function for Info logging.
func LogInfoF(format string, v ...interface{}) { // Renamed to avoid conflict with Logger method
	globalLogger.LogInfo(format, v...)
}

// LogDebugF is a package-level convenience function for Debug logging.
func LogDebugF(format string, v ...interface{}) { // Renamed
	globalLogger.LogDebug(format, v...)
}

// LogWarnF is a package-level convenience function for Warn logging.
func LogWarnF(format string, v ...interface{}) { // Renamed
	globalLogger.LogWarn(format, v...)
}

// OHLCVBar represents a single Open, High, Low, Close, Volume data point.
// This is a common structure that data providers should map their responses to.
type OHLCVBar struct {
	Timestamp int64   `json:"timestamp"` // Unix timestamp (seconds or milliseconds, ensure consistency)
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
}

// Helper function to parse string log level from config to LogLevel type
func ParseLogLevel(levelStr string) (LogLevel, error) {
	switch strings.ToLower(levelStr) { // Added strings.ToLower for case-insensitivity
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
		// Return a default level and an error to indicate parsing failure
		return Info, fmt.Errorf("invalid log level string: %s", levelStr)
	}
}

// SortBarsByTimestamp sorts a slice of OHLCVBar by ascending Timestamp.
// Used by the CoinMarketCap caching layer to ensure bars are in time order.
func SortBarsByTimestamp(bars []OHLCVBar) {
	sort.Slice(bars, func(i, j int) bool {
		return bars[i].Timestamp < bars[j].Timestamp
	})
}

// DoJSONRequest performs an HTTP request, retries on transient errors (5xx or network),
// and unmarshals a JSON response into result.
func DoJSONRequest(client *http.Client, req *http.Request, maxRetries int, retryDelay time.Duration, result interface{}) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// If retrying and the original request has a reusable body, clone it:
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

		// Retry on server (5xx) errors
		if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
			lastErr = fmt.Errorf("server error %d %s", resp.StatusCode, resp.Status)
			time.Sleep(retryDelay)
			continue
		}

		// Fail fast on non-2xx
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return fmt.Errorf("http %d: %s", resp.StatusCode, string(snippet))
		}

		// Decode JSON into result
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("failed to decode JSON response: %w", err)
		}
		return nil
	}
	return fmt.Errorf("all retries failed: %w", lastErr)
}

// ParseFloatFromInterface is a helper function to parse float64 from various numeric types in an interface{}.
// Added to resolve missing dependency.
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
	case json.Number: // If using json.Decoder with UseNumber()
		return v.Float64()
	default:
		return 0, fmt.Errorf("unsupported type for float conversion: %T", v)
	}
}

// GenerateKrakenAuthHeaders builds the proper API-Key and API-Sign headers
// for Kraken private endpoints.
// apiPath should be the full request path, e.g. "/0/private/Balance".
func GenerateKrakenAuthHeaders(
	apiKey, apiSecret, apiPath, nonce string,
	data url.Values,
) (map[string]string, error) {

	// 1) Prepare POST data string
	postData := data.Encode() // e.g. "nonce=12345&param=value"

	// 2) SHA256(nonce + POST data)
	sha256Hasher := sha256.New()
	sha256Hasher.Write([]byte(nonce + postData))
	shaSum := sha256Hasher.Sum(nil)

	// 3) Prepend API path to sha256 sum
	message := append([]byte(apiPath), shaSum...)

	// 4) Decode API secret (base64)
	decodedSecret, err := base64.StdEncoding.DecodeString(apiSecret)
	if err != nil {
		return nil, err
	}

	// 5) HMAC-SHA512 over the message using decoded secret
	mac := hmac.New(sha512.New, decodedSecret)
	mac.Write(message)
	macSum := mac.Sum(nil)

	// 6) Base64-encode the HMAC
	signature := base64.StdEncoding.EncodeToString(macSum)

	// 7) Return headers map
	return map[string]string{
		"API-Key":  apiKey,
		"API-Sign": signature,
	}, nil
}
