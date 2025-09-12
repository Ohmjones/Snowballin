package dataprovider

import (
	"Snowballin/utilities"
	"context"
	"time"
)

// AssetIdentity holds the unified, cross-referenced identification for a tradable asset.
// This is the single source of truth for asset mapping.
type AssetIdentity struct {
	ID              int       `json:"id"`               // Primary key
	CommonSymbol    string    `json:"common_symbol"`    // e.g., "XRP"
	KrakenAsset     string    `json:"kraken_asset"`     // e.g., "XXRP"
	KrakenWsName    string    `json:"kraken_ws_name"`   // e.g., "XRP/USD"
	CoinGeckoID     string    `json:"coingecko_id"`     // e.g., "ripple"
	CoinMarketCapID string    `json:"coinmarketcap_id"` // e.g., "52"
	IconPath        string    `json:"icon_path"`        // e.g., "web/static/icons/ripple.webp"
	LastUpdated     time.Time `json:"last_updated"`
}

// DataProvider defines the interface for accessing market data from various sources.
type DataProvider interface {
	GetSupportedCoins(ctx context.Context) ([]Coin, error)
	GetMarketData(ctx context.Context, ids []string, vsCurrency string) ([]MarketData, error)
	GetOHLCVHistorical(ctx context.Context, id, vsCurrency, interval string) ([]utilities.OHLCVBar, error)
	GetOHLCDaily(ctx context.Context, id, vsCurrency string, days int) ([]utilities.OHLCVBar, error)
	GetHistoricalPrice(ctx context.Context, id, date string) (HistoricalPrice, error)
	GetExchangeDetails(ctx context.Context, exchangeID string) (ExchangeDetails, error)
	GetGlobalMarketData(ctx context.Context) (GlobalMarketData, error)
	GetCoinID(ctx context.Context, commonAssetSymbol string) (string, error)
	PrimeCache(ctx context.Context) error
	PrimeHistoricalData(ctx context.Context, id, vsCurrency, interval string, days int) error
	GetTopAssetsByMarketCap(ctx context.Context, quoteCurrency string, topN int) ([]string, error)
	GetAllTickersForAsset(ctx context.Context, coinID string) ([]CrossExchangeTicker, error)
	GetTrendingSearches(ctx context.Context) ([]TrendingCoin, error)
	GetGainersAndLosers(ctx context.Context, quoteCurrency string, topN int) (gainers []MarketData, losers []MarketData, err error)
	GetCoinIDsBySymbol(ctx context.Context, sym string) ([]string, error)
}
type CrossExchangeTicker struct {
	ExchangeName string
	Price        float64
	Volume24h    float64
}
type FearGreedProvider interface {
	GetFearGreedIndex(ctx context.Context) (FearGreedIndex, error)
}

// Coin represents basic coin information.
type Coin struct {
	ID     string
	Symbol string
	Name   string
	// Platforms map[string]string `json:"platforms"` // Uncomment if needed at interface level
}

// MarketData represents aggregated market data for a coin.
type MarketData struct {
	ID             string
	Symbol         string
	Name           string
	Image          string // Added for icon URL
	CurrentPrice   float64
	MarketCap      float64
	Volume24h      float64
	High24h        float64
	Low24h         float64
	PriceChange1h  float64
	PriceChange24h float64
	PriceChange7d  float64
	LastUpdated    time.Time
	Price          float64
}

// OrderBookData represents summarized order book info (or ticker info from CG).
// Define fields based on what the interface guarantees, might be minimal.
type OrderBookData struct {
	Exchange   string // Source exchange
	Pair       string // Trading pair
	Bid        float64
	Ask        float64
	LastPrice  float64
	Volume     float64
	TrustScore string // Often specific to CoinGecko tickers
}

// HistoricalPrice represents the price on a specific date.
type HistoricalPrice struct {
	Price     float64
	ID        string
	Date      string
	Timestamp string

	// Add Date field if needed (e.g., time.Time or string)
}

// ExchangeDetails represents basic info about an exchange.
type ExchangeDetails struct {
	Name      string
	Volume24h float64 // Usually in BTC or Normalized BTC for comparisons
}

// GlobalMarketData represents overall market stats.
type GlobalMarketData struct {
	TotalMarketCap float64 // Usually in USD
	BTCDominance   float64 `json:"btc_dominance"`
}

// FearGreedIndex represents the market sentiment index.
// Data typically sourced from alternative.me
type FearGreedIndex struct {
	Value     int    `json:"value"`     // The numerical index value (e.g., 0-100)
	Level     string `json:"level"`     // The classification (e.g., "Fear", "Neutral", "Greed")
	Timestamp int64  `json:"timestamp"` // Unix timestamp of the data point
}

// TrendingCoin represents a coin trending on a platform (like CoinGecko).
type TrendingCoin struct {
	Coin  Coin // Embed the basic Coin info
	Score int  // Ranking score if available
}
