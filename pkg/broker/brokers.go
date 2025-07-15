// File: pkg/broker/brokers.go
package broker

import (
	"Snowballin/utilities"
	"context"
	"time"
)

// Broker defines the interface for interacting with a cryptocurrency exchange.
type Broker interface {
	// PlaceOrder submits a new order to the exchange.
	PlaceOrder(ctx context.Context, assetPair, side, orderType string, volume, price, stopPrice float64, clientOrderID string) (string, error)

	// CancelOrder cancels an existing order by its ID.
	CancelOrder(ctx context.Context, orderID string) error

	// GetOrderStatus retrieves the status of a specific order.
	GetOrderStatus(ctx context.Context, orderID string) (Order, error)

	// GetBalance retrieves the account balance for a specific currency.
	GetBalance(ctx context.Context, currency string) (Balance, error)

	// GetAccountValue retrieves the total portfolio value in the specified quote currency.
	GetAccountValue(ctx context.Context, quoteCurrency string) (float64, error) // <<< NEW METHOD

	// GetTicker retrieves ticker data for a specific trading pair.
	GetTicker(ctx context.Context, pair string) (TickerData, error)

	// GetTrades retrieves the trade history for a specific trading pair.
	// The 'pair' parameter is the common pair name (e.g., "BTC/USD").
	// The 'since' parameter can be used to limit trades. If zero, fetch all/default.
	GetTrades(ctx context.Context, pair string, since time.Time) ([]Trade, error) // <<< MODIFIED GetTrades signature slightly for clarity

	// GetOrderBook retrieves the order book for a specific trading pair.
	// The 'pair' parameter is the common pair name (e.g., "BTC/USD").
	GetOrderBook(ctx context.Context, pair string, depth int) (OrderBookData, error) // <<< NEW METHOD (from 1.c)

	// GetLastNOHLCVBars retrieves the last N OHLCV bars for a specific trading pair and interval.
	// The 'pair' parameter is the common pair name (e.g., "BTC/USD").
	// Interval is in minutes (Kraken specific, e.g., "1", "5", "60").
	GetLastNOHLCVBars(ctx context.Context, pair string, intervalMinutes string, nBars int) ([]utilities.OHLCVBar, error)

	// RefreshAssetInfo ensures that the adapter's underlying client has the latest asset and pair information.
	RefreshAssetInfo(ctx context.Context) error // <<< NEW METHOD (from 1.d alternative)

	GetTradeFees(ctx context.Context, commonPair string) (makerFee float64, takerFee float64, err error)
}

type OHLCVBar struct { // Duplicating from utilities for now to avoid import cycle if pkg/broker cannot import utilities
	Timestamp int64   `json:"timestamp"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
}

// OrderParams defines the parameters required to place a new order.
type OrderParams struct {
	Pair      string  `json:"pair"`       // Trading pair, e.g., "XBTUSD", "ETHUSD"
	Side      string  `json:"side"`       // "buy" or "sell"
	Type      string  `json:"type"`       // e.g., "market", "limit", "stop-loss"
	Volume    float64 `json:"volume"`     // Quantity in base currency
	Price     float64 `json:"price"`      // Limit price (for limit orders)
	StopPrice float64 `json:"stop_price"` // Stop price (for stop orders)
}

// Order represents a trade order's state and details.
type Order struct {
	ID            string    `json:"id"`
	ClientOrderID string    `json:"client_order_id,omitempty"`
	Pair          string    `json:"pair"`
	Side          string    `json:"side"`
	Type          string    `json:"type"`
	Status        string    `json:"status"`
	Price         float64   `json:"price,omitempty"`
	StopPrice     float64   `json:"stop_price,omitempty"`
	Volume        float64   `json:"volume"`
	FilledVolume  float64   `json:"filled_volume"`
	AvgFillPrice  float64   `json:"avg_fill_price,omitempty"`
	Cost          float64   `json:"cost,omitempty"`
	Fee           float64   `json:"fee,omitempty"`
	FeeCurrency   string    `json:"fee_currency,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at,omitempty"` // <<< This is what we need for timestamp
	ExpiresAt     time.Time `json:"expires_at,omitempty"`
	Reason        string    `json:"reason,omitempty"`
	OrderType     string
	ExecutedVol   float64
	TimePlaced    time.Time
	TimeCompleted time.Time
	RequestedVol  float64

	// Fields below seem to be from a different context or older version based on your screenshot
	// OrderType     interface{} // Not in screenshot errors
	// ExecutedVol   interface{} // Not in screenshot errors
	// TimePlaced    time.Time   // Not in screenshot errors
	// TimeCompleted time.Time // Not in screenshot errors
}

// TickerData contains current market ticker information for a trading pair.
type TickerData struct {
	Pair      string    `json:"pair"`       // Exchange-specific trading pair
	Bid       float64   `json:"bid"`        // Current highest bid price
	Ask       float64   `json:"ask"`        // Current lowest ask price
	LastPrice float64   `json:"last_price"` // Price of the last trade
	Volume    float64   `json:"volume"`     // Typically 24h volume in base currency
	High      float64   `json:"high"`       // Typically 24h high
	Low       float64   `json:"low"`        // Typically 24h low
	Open      float64   `json:"open"`       // Typically 24h open price (or today's open)
	Timestamp time.Time `json:"timestamp"`  // Timestamp of when the ticker data was fetched or generated
}

// Trade represents an executed trade fill.
type Trade struct {
	ID          string    `json:"id"`           // Exchange's unique trade ID
	OrderID     string    `json:"order_id"`     // Associated order ID that this trade is part of
	Pair        string    `json:"pair"`         // Exchange-specific trading pair
	Side        string    `json:"side"`         // "buy" or "sell"
	Price       float64   `json:"price"`        // Price at which this trade was executed
	Volume      float64   `json:"volume"`       // Volume of this trade in base currency
	Cost        float64   `json:"cost"`         // Total cost of this trade (price * volume) in quote currency
	Fee         float64   `json:"fee"`          // Fee paid for this trade
	FeeCurrency string    `json:"fee_currency"` // Currency of the fee
	Timestamp   time.Time `json:"timestamp"`    // Timestamp of when the trade was executed
}

// Balance represents the balance of a single currency.
type Balance struct {
	Currency  string  `json:"currency"`  // e.g., "XBT", "USD"
	Available float64 `json:"available"` // Amount available for trading
	Total     float64 `json:"total"`     // Total amount (available + on hold)
}

// OrderBookLevel represents a single price level in an order book.
type OrderBookLevel struct {
	Price  float64 `json:"price"`
	Volume float64 `json:"volume"`
}

// OrderBookData represents a snapshot of the order book for a trading pair.
type OrderBookData struct {
	Pair      string           `json:"pair"`      // Common trading pair symbol
	Bids      []OrderBookLevel `json:"bids"`      // Slice of bid levels, typically sorted highest to lowest price
	Asks      []OrderBookLevel `json:"asks"`      // Slice of ask levels, typically sorted lowest to highest price
	Timestamp time.Time        `json:"timestamp"` // Timestamp of the order book snapshot
}
