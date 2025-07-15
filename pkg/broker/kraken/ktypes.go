package kraken

// File: pkg/broker/kraken/types.go

type AssetPairInfo struct {
	Altname           string          `json:"altname"`
	WSName            string          `json:"wsname"`
	ACLASSBase        string          `json:"aclass_base"`
	Base              string          `json:"base"`
	ACLASSQuote       string          `json:"aclass_quote"`
	Quote             string          `json:"quote"`
	Lot               string          `json:"lot"`
	PairDecimals      int             `json:"pair_decimals"`
	LotDecimals       int             `json:"lot_decimals"`
	LotMultiplier     int             `json:"lot_multiplier"`
	LeverageBuy       []int           `json:"leverage_buy"`
	LeverageSell      []int           `json:"leverage_sell"`
	Fees              [][]interface{} `json:"fees"`
	FeesMaker         [][]interface{} `json:"fees_maker"`
	FeeVolumeCurrency string          `json:"fee_volume_currency"`
	MarginCall        int             `json:"margin_call"`
	MarginStop        int             `json:"margin_stop"`
	OrderMin          string          `json:"ordermin"`
}
type AssetPairAPIInfo struct {
	Altname       string `json:"altname"`
	Base          string `json:"base"`
	Quote         string `json:"quote"`
	LotDecimals   int    `json:"lot_decimals"`
	PairDecimals  int    `json:"pair_decimals"`
	OrderMin      string `json:"ordermin"`
	OrderMax      string `json:"ordermax"`
	PriceDecimals int    `json:"price_decimals"`
	PriceLot      string `json:"price_lot"`
}

// KrakenAPIRawOrderBook directly maps the structure from Kraken's /0/public/Depth endpoint result.
type KrakenAPIRawOrderBook struct {
	Asks [][]interface{} `json:"asks"` // Each inner slice is typically [price_string, volume_string, timestamp_int]
	Bids [][]interface{} `json:"bids"` // Each inner slice is typically [price_string, volume_string, timestamp_int]
}
type AssetInfo struct {
	Altname         string `json:"altname"`          // Alternate name (common symbol, e.g., ETH, USD)
	ACLASS          string `json:"aclass"`           // Asset class (e.g., "currency")
	Decimals        int    `json:"decimals"`         // Standard number of decimal places for calculations
	DisplayDecimals int    `json:"display_decimals"` // Number of decimal places for display
}

type TickerInfo struct {
	Ask               []string `json:"a"` // [price, wholeLotVolume, lotVolume]
	Bid               []string `json:"b"` // [price, wholeLotVolume, lotVolume]
	LastTradeClosed   []string `json:"c"` // [price, lotVolume]
	Volume            []string `json:"v"` // [today, last24Hours]
	VolumeWeightedAvg []string `json:"p"` // [today, last24Hours]
	Trades            []int    `json:"t"` // [today, last24Hours]
	Low               []string `json:"l"` // [today, last24Hours]
	High              []string `json:"h"` // [today, last24Hours]
	OpenPrice         string   `json:"o"`
}

// TickerData is a simplified/processed version of TickerInfo, used internally by the client
// before being mapped to broker.TickerData by the adapter.
type TickerData struct {
	Pair         string // Kraken pair name, e.g., XBTUSD
	AskPrice     float64
	BidPrice     float64
	LastPrice    float64
	VolumeToday  float64
	Volume24h    float64
	VWAPToday    float64
	VWAP24h      float64
	TradesToday  int
	Trades24h    int
	LowToday     float64
	Low24h       float64
	HighToday    float64
	High24h      float64
	OpeningPrice float64
}

// KrakenOrderDescription is part of Kraken's order info response.
type KrakenOrderDescription struct {
	Pair      string `json:"pair"`
	Type      string `json:"type"`      // e.g., "buy", "sell"
	OrderType string `json:"ordertype"` // e.g., "limit", "market"
	Price     string `json:"price"`     // Primary price (limit price)
	Price2    string `json:"price2"`    // Secondary price (e.g., for stop-loss-profit)
	Leverage  string `json:"leverage"`
	Order     string `json:"order"` // Original order description
	Close     string `json:"close"` // Conditional close order description
}

// KrakenOrderInfo represents Kraken's structure for an order (open or closed).
// Used for QueryOrders (closed) and OpenOrders responses.
type KrakenOrderInfo struct {
	Refid      interface{}            `json:"refid"`
	Userref    interface{}            `json:"userref"`
	Status     string                 `json:"status"`
	Reason     string                 `json:"reason,omitempty"`
	Opentm     float64                `json:"opentm"`
	Closetm    float64                `json:"closetm,omitempty"`
	Starttm    float64                `json:"starttm"`
	Expiretm   float64                `json:"expiretm"`
	Descr      KrakenOrderDescription `json:"descr"`
	Volume     string                 `json:"vol"`
	VolExec    string                 `json:"vol_exec"`
	Cost       string                 `json:"cost"`
	Fee        string                 `json:"fee"`
	Price      string                 `json:"price"`
	StopPrice  string                 `json:"stopprice"`
	LimitPrice string                 `json:"limitprice"`
	Misc       string                 `json:"misc"`
	Oflags     string                 `json:"oflags"`
	Trades     []string               `json:"trades,omitempty"`
}

// KrakenFeeTierInfo represents fee information for a trading pair.
type KrakenFeeTierInfo struct {
	Fee        string `json:"fee"`        // Current fee percentage for the tier
	MinFee     string `json:"minfee"`     // Minimum fee for the pair (if not fixed per-order)
	MaxFee     string `json:"maxfee"`     // Maximum fee for the pair
	NextFee    string `json:"nextfee"`    // Fee for next volume tier (if available)
	NextVolume string `json:"nextvolume"` // Volume for the next tier (if available)
	TierVolume string `json:"tiervolume"` // Volume in current tier (if applicable)
}

// KrakenTradeInfo represents a single trade detail from Kraken's TradesHistory.
type KrakenTradeInfo struct {
	Ordtxid        string  `json:"ordertxid"`
	Pair           string  `json:"pair"`
	Time           float64 `json:"time"`
	Type           string  `json:"type"`
	Ordertype      string  `json:"ordertype"`
	Price          string  `json:"price"`
	Cost           string  `json:"cost"`
	Fee            string  `json:"fee"`
	Vol            string  `json:"vol"`
	Margin         string  `json:"margin"`
	Misc           string  `json:"misc"`
	PosStatus      string  `json:"posstatus,omitempty"`
	ClosedAvgPrice string  `json:"cprice,omitempty"`
	ClosedCost     string  `json:"ccost,omitempty"`
	ClosedFee      string  `json:"cfee,omitempty"`
	ClosedVol      string  `json:"cvol,omitempty"`
	ClosedMargin   string  `json:"cmargin,omitempty"`
	Net            string  `json:"net,omitempty"`
	TradeID        string  // Map key from TradesHistory result
}

// TradeVolumeResponse parses the top-level response from the TradeVolume endpoint.
type TradeVolumeResponse struct {
	Error  []string          `json:"error"`
	Result TradeVolumeResult `json:"result"`
}

// TradeVolumeResult contains the core data, including the separate fee structures for maker and taker.
type TradeVolumeResult struct {
	Currency  string                       `json:"currency"`
	Volume    string                       `json:"volume"`
	Fees      map[string]KrakenFeeTierInfo `json:"fees"`
	FeesMaker map[string]KrakenFeeTierInfo `json:"fees_maker"`
}
