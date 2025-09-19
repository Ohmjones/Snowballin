package kraken

import "encoding/json"

type AssetPairInfo struct {
	PairName     string          `json:"-"` // Augmented: the response key (primary)
	Altname      string          `json:"altname"`
	Base         string          `json:"base"`
	Quote        string          `json:"quote"`
	PairDecimals int             `json:"pair_decimals"`
	LotDecimals  int             `json:"lot_decimals"`
	OrderMin     string          `json:"ordermin"`
	WSName       string          `json:"wsname"`
	Fees         [][]json.Number `json:"fees"`       // Taker fees schedule
	FeesMaker    [][]json.Number `json:"fees_maker"` // Maker fees schedule
	MakerFee     float64         `json:"-"`
	TakerFee     float64         `json:"-"`
}

type AssetInfo struct {
	Altname string `json:"altname"`
	// Other fields pruned
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

// TickerResponse is the top-level response for the Ticker endpoint.
type TickerResponse struct {
	Error  []string              `json:"error"`
	Result map[string]TickerInfo `json:"result"`
}

// AssetPairsResponse is the top-level response for the AssetPairs endpoint.
type AssetPairsResponse struct {
	Error  []string                 `json:"error"`
	Result map[string]AssetPairInfo `json:"result"`
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
