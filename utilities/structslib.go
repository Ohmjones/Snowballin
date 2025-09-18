package utilities

import (
	"log"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// --- Global Logger ---
var globalLogger = NewLogger(Info) // Default to Info

// Colors.
const (
	ColorCyan   = "\033[96m" // For Buy signals
	ColorRed    = "\033[91m" // For Sell signals
	ColorReset  = "\033[0m"
	ColorWhite  = "\033[97m" // For Hold signals
	ColorYellow = "\033[93m" // For asset pairs
)

// Logging Level
const (
	Debug LogLevel = iota
	Info
	Warn
	Error
	Fatal
)

// --- Types (Alphabetized) ---

// AppConfig is the root configuration structure, holding all other config sections.
type AppConfig struct {
	AppName             string                 `mapstructure:"app_name"`
	CircuitBreaker      CircuitBreakerConfig   `mapstructure:"circuit_breaker"`
	Coingecko           *CoingeckoConfig       `mapstructure:"coingecko"`
	Coinmarketcap       *CoinmarketcapConfig   `mapstructure:"coinmarketcap"`
	Consensus           ConsensusConfig        `mapstructure:"consensus"`
	DataCompleteness    DataCompletenessConfig `mapstructure:"data_completeness"`
	DataDiscrepancy     DataDiscrepancyConfig  `mapstructure:"data_discrepancy"`
	DataProviderWeights map[string]float64     `mapstructure:"dataprovider_weights"`
	DB                  DatabaseConfig         `mapstructure:"database"`
	Discord             DiscordConfig          `mapstructure:"discord"`
	Environment         string                 `mapstructure:"environment"`
	ExitStrategy        ExitStrategyConfig     `mapstructure:"exit_strategy"`
	FearGreed           *FearGreedConfig       `mapstructure:"FearGreed"`
	Indicators          IndicatorsConfig       `mapstructure:"indicators"`
	Kraken              KrakenConfig           `mapstructure:"kraken"`
	Logging             LoggingConfig          `mapstructure:"logging"`
	Orders              OrdersConfig           `mapstructure:"orders"`
	Preflight           PreflightConfig        `mapstructure:"preflight"`
	Trading             TradingConfig          `mapstructure:"trading"`
	Version             string                 `mapstructure:"version"`
	Withdrawal          WithdrawalConfig       `mapstructure:"withdrawal"`
}

// CircuitBreakerConfig defines parameters to halt trading during extreme market conditions.
type CircuitBreakerConfig struct {
	ATRVolatilityThreshold   float64 `mapstructure:"atr_volatility_threshold"`
	DrawdownThresholdPercent float64 `mapstructure:"drawdown_threshold_percent"`
	Enabled                  bool    `mapstructure:"enabled"`
}

// CoingeckoConfig holds settings for the CoinGecko data provider.
type CoingeckoConfig struct {
	APIKey                    string `mapstructure:"api_key"`
	BaseURL                   string `mapstructure:"base_url"`
	CacheBasePath             string `mapstructure:"cache_base_path"`
	CacheRetentionDays        int    `mapstructure:"cache_retention_days"`
	IDMapRefreshIntervalHours int    `mapstructure:"id_map_refresh_interval_hours"`
	MaxRetries                int    `mapstructure:"max_retries"`
	OHLCVDaysDefault          string `mapstructure:"ohlcv_days_default"`
	QuoteCurrency             string `mapstructure:"quote_currency"`
	RateLimitBurst            int    `mapstructure:"rate_limit_burst"`
	RateLimitPerSec           int    `mapstructure:"rate_limit_per_sec"`
	RequestTimeoutSec         int    `mapstructure:"request_timeout_sec"`
	RetryDelaySec             int    `mapstructure:"retry_delay_sec"`
}

// CoinmarketcapConfig holds settings for the CoinMarketCap data provider.
type CoinmarketcapConfig struct {
	APIKey                    string            `mapstructure:"api_key"`
	BaseURL                   string            `mapstructure:"base_url"`
	CacheBasePath             string            `mapstructure:"cache_base_path"`
	IDMapRefreshIntervalHours int               `mapstructure:"id_map_refresh_interval_hours"`
	MaxRetries                int               `mapstructure:"max_retries"`
	OHLCVDaysDefault          int               `mapstructure:"ohlcv_days_default"`
	QuoteCurrency             string            `mapstructure:"quote_currency"`
	RateLimitBurst            int               `mapstructure:"rate_limit_burst"`
	RateLimitPerSec           int               `mapstructure:"rate_limit_per_sec"`
	RequestTimeoutSec         int               `mapstructure:"request_timeout_sec"`
	RetryDelaySec             int               `mapstructure:"retry_delay_sec"`
	SymbolOverrides           map[string]string `mapstructure:"symbol_overrides"`
}

// ConsensusConfig defines how different signals are weighted to reach a trading decision.
type ConsensusConfig struct {
	MultiTimeframe             MultiTFConfig `mapstructure:"multi_timeframe"`
	UseMultiTimeframeConsensus bool          `mapstructure:"use_multi_timeframe_consensus"` // New field to enable MTF
}

// DataCompletenessConfig defines how the bot handles missing data from providers.
type DataCompletenessConfig struct {
	DiscrepancyThresholdPercent float64  `mapstructure:"discrepancy_threshold_percent"`
	LogLevelOnDiscrepancy       string   `mapstructure:"log_level_on_discrepancy"`
	LogLevelOnSubstitution      string   `mapstructure:"log_level_on_substitution"`
	SubstitutionSources         []string `mapstructure:"substitution_sources"`
}

// DataDiscrepancyConfig defines how to act when data providers show conflicting data.
type DataDiscrepancyConfig struct {
	Action                   string  `mapstructure:"action"`
	ConsecutiveCyclesTrigger int     `mapstructure:"consecutive_cycles_trigger"`
	ThresholdPercent         float64 `mapstructure:"threshold_percent"`
}

// DatabaseConfig holds settings for database connections.
type DatabaseConfig struct {
	DBPath string `mapstructure:"database_path"`
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

// InMemoryNonceCounter provides a simple in-memory nonce generator.
type InMemoryNonceCounter struct {
	lastNonce int64
	mu        sync.Mutex
}

// IndicatorsConfig holds parameters for various technical indicators.
type IndicatorsConfig struct {
	ATRPeriod             int                 `mapstructure:"atr_period"`
	BollingerPeriod       int                 `mapstructure:"bollinger_period"`
	BollingerStdDev       float64             `mapstructure:"bollinger_std_dev"`
	LiquidityHunt         LiquidityHuntConfig `mapstructure:"liquidity_hunt"`
	MACDFastPeriod        int                 `mapstructure:"macd_fast_period"`
	MACDSignalPeriod      int                 `mapstructure:"macd_signal_period"`
	MACDSlowPeriod        int                 `mapstructure:"macd_slow_period"`
	OBVNegativeThreshold  float64             `mapstructure:"obv_negative_threshold"`
	OBVPositiveThreshold  float64             `mapstructure:"obv_positive_threshold"`
	OBVMAPeriod           int                 `mapstructure:"obv_ma_period"`
	RSIPeriod             int                 `mapstructure:"rsi_period"`
	StochasticD           int                 `mapstructure:"stochastic_d_period"` // e.g., 3
	StochasticK           int                 `mapstructure:"stochastic_k_period"` // e.g., 14
	StochasticSlowPeriod  int                 `mapstructure:"stochastic_slow_period"`
	StochRSIBuyThreshold  float64             `mapstructure:"stoch_rsi_buy_threshold"`
	StochRSIPeriod        int                 `mapstructure:"stoch_rsi_period"`
	StochRSISellThreshold float64             `mapstructure:"stoch_rsi_sell_threshold"`
	UseSlowStochastic     bool                `mapstructure:"use_slow_stochastic"`
	VolumeLookbackPeriod  int                 `mapstructure:"volume_lookback_period"`
	VolumeSpikeFactor     float64             `mapstructure:"volume_spike_factor"`
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

// LiquidityHuntConfig holds parameters for the liquidity hunt indicator.
type LiquidityHuntConfig struct {
	MinWickPercent     float64 `mapstructure:"min_wick_percent"`
	VolMAPeriod        int     `mapstructure:"vol_ma_period"`
	VolSpikeMultiplier float64 `mapstructure:"vol_spike_multiplier"`
}

// Logger provides a structured logger with different levels.
type Logger struct {
	Level  LogLevel
	Logger *log.Logger
}

// LoggingConfig holds settings related to logging.
type LoggingConfig struct {
	CompressBackups bool   `mapstructure:"compress_backups"`
	Level           string `mapstructure:"level"`
	LogFilePath     string `mapstructure:"log_file_path"`
	LogToFile       bool   `mapstructure:"log_to_file"`
	MaxAgeDays      int    `mapstructure:"max_age_days"`
	MaxBackups      int    `mapstructure:"max_backups"`
	MaxSizeMB       int    `mapstructure:"max_size_mb"`
}

// LogLevel defines the severity of a log message.
type LogLevel int

// MultiTFConfig holds settings for multi-timeframe analysis.
type MultiTFConfig struct {
	AdditionalTimeframes []string `mapstructure:"additional_timeframes"`
	BaseTimeframe        string   `mapstructure:"base_timeframe"`
	TFLookbackLengths    []int    `mapstructure:"tf_lookback_lengths"`
}

// NonceCounter defines an interface for generating unique nonces.
type NonceCounter interface {
	Next() string
}

// OHLCVBar represents a single Open, High, Low, Close, Volume data point.
type OHLCVBar struct {
	Close     float64 `json:"close"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Open      float64 `json:"open"`
	Timestamp int64   `json:"timestamp"`
	Volume    float64 `json:"volume"`
}

// OrdersConfig holds settings for the order watcher and reaper.
type OrdersConfig struct {
	EnableWatcher                 bool    `mapstructure:"enable_watcher"`
	MaxOrderAgeMinutesDynamicCap  float64 `mapstructure:"max_order_age_minutes_dynamic_cap"`
	MaxOrderAgeMinutesForGridBase float64 `mapstructure:"max_order_age_minutes_for_grid_base"`
	MinOrderAgeMinutesDynamicCap  float64 `mapstructure:"min_order_age_minutes_dynamic_cap"`
	ReaperIntervalSec             int     `mapstructure:"reaper_interval_sec"`
	WatcherIntervalSec            int     `mapstructure:"watcher_interval_sec"`
}

// Position holds the state of an active trade, updated to support the Martingale DCA strategy.
type Position struct {
	AssetPair      string  `json:"asset_pair"`
	AveragePrice   float64 `json:"average_price"`
	BaseOrderPrice float64 `json:"base_order_price"`
	BaseOrderSize  float64 `json:"base_order_size"`
	BrokerOrderID  string  `json:"broker_order_id"` // Tracks the initial base order ID
	// Target and Management
	CurrentTakeProfit float64   `json:"current_take_profit"`
	EntryTimestamp    time.Time `json:"entry_timestamp"`
	// Core DCA State
	FilledSafetyOrders int  `json:"filled_safety_orders"`
	IsDcaActive        bool `json:"is_dca_active"`
	// Trailing Stop Loss
	IsTrailingActive    bool    `json:"is_trailing_active"`
	PeakPriceSinceTP    float64 `json:"peak_price_since_tp"`
	TotalVolume         float64 `json:"total_volume"`
	UnrealizedPL        float64 `json:"-"`
	UnrealizedPLPercent float64 `json:"-"`
}

type PreflightConfig struct {
	PrimingDays         int  `mapstructure:"priming_days"`
	PrimeHistoricalData bool `mapstructure:"prime_historical_data"`
}

// TradingConfig holds general trading parameters.
type TradingConfig struct {
	AssetPairs                       []string `mapstructure:"asset_pairs"`
	BaseOrderSize                    float64  `mapstructure:"base_order_size"`
	ConsensusBuyMultiplier           float64  `mapstructure:"consensus_buy_multiplier"`
	ConsensusRsiThreshold            float64  `mapstructure:"consensus_rsi_threshold"`
	DcaAtrPeriod                     int      `mapstructure:"dca_atr_period"`
	DcaAtrSpacingMultiplier          float64  `mapstructure:"dca_atr_spacing_multiplier"`
	DcaSpacingMode                   string   `mapstructure:"dca_spacing_mode"`
	DustThresholdUSD                 float64  `mapstructure:"dust_threshold_usd"`
	DynamicAssetScanTopN             int      `mapstructure:"dynamic_asset_scan_top_n"`
	FearOverrideThreshold            int      `mapstructure:"fear_override_threshold"`
	MaxPortfolioDrawdown             float64  `mapstructure:"max_portfolio_drawdown"`
	MaxSafetyOrders                  int      `mapstructure:"max_safety_orders"`
	MinBookConfidenceForPredictive   float64  `mapstructure:"min_book_confidence_for_predictive"`
	PortfolioRiskPerTrade            float64  `mapstructure:"portfolio_risk_per_trade"`
	PredictiveAtrMultiplier          float64  `mapstructure:"predictive_atr_multiplier"`
	PredictiveBuyDeviationPercent    float64  `mapstructure:"predictive_buy_deviation_percent"`
	PredictiveOrderSizePercent       float64  `mapstructure:"predictive_order_size_percent"` // This will be deprecated by the new logic
	PredictiveRsiThreshold           float64  `mapstructure:"predictive_rsi_threshold"`
	PredictiveSearchWindowPercent    float64  `mapstructure:"predictive_search_window_percent"`
	PriceDeviationToOpenSafetyOrders float64  `mapstructure:"price_deviation_to_open_safety_orders"`
	QuoteCurrency                    string   `mapstructure:"quote_currency"`
	RecalculateAfterDcaFill          bool     `mapstructure:"recalculate_after_dca_fill"`
	RsiAdjustFactor                  float64  `mapstructure:"rsi_adjust_factor"`
	SafetyOrderSize                  float64  `mapstructure:"safety_order_size"`
	SafetyOrderStepScale             float64  `mapstructure:"safety_order_step_scale"`
	SafetyOrderVolumeScale           float64  `mapstructure:"safety_order_volume_scale"`
	SMAPeriodForDCA                  int      `mapstructure:"sma_period_for_dca"`
	SMAPeriodsForExit                []int    `mapstructure:"sma_periods_for_exit"`
	StopLossEnabled                  bool     `mapstructure:"stop_loss_enabled"`
	TakeProfitPartialSellPercent     float64  `mapstructure:"take_profit_partial_sell_percent"`
	TakeProfitPercentage             float64  `mapstructure:"take_profit_percentage"`
	TrailingStopDeviation            float64  `mapstructure:"trailing_stop_deviation"`
	TrailingStopEnabled              bool     `mapstructure:"trailing_stop_enabled"`
	UseDynamicAssetScanning          bool     `mapstructure:"use_dynamic_asset_scanning"`
	UseFearOverride                  bool     `mapstructure:"use_fear_override"`
	UseMartingaleForPredictive       bool     `mapstructure:"use_martingale_for_predictive"`
	UseOrderBookForPredictive        bool     `mapstructure:"use_order_book_for_predictive"`
}

// WithdrawalConfig holds settings for automated withdrawal of funds.
type WithdrawalConfig struct {
	CheckIntervalSec int               `mapstructure:"check_interval_sec"`
	CooldownMinutes  int               `mapstructure:"cooldown_minutes"`
	Enabled          bool              `mapstructure:"enabled"`
	WithdrawKeys     map[string]string `mapstructure:"withdraw_keys"`
	WithdrawUSD      float64           `mapstructure:"withdraw_usd"`
}
