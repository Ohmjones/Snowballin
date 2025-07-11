package strategy

import (
	"Snowballin/pkg/broker"
	"Snowballin/utilities"
	"fmt"
	"math"
	"sort"
)

// ComputeEMASeries explicitly computes the Exponential Moving Average (EMA).
func ComputeEMASeries(data []float64, period int) ([]float64, []float64) {
	if period <= 0 || len(data) == 0 {
		return nil, nil
	}

	ema := make([]float64, len(data))
	multiplier := 2.0 / float64(period+1)

	ema[0] = data[0]
	for i := 1; i < len(data); i++ {
		ema[i] = (data[i]-ema[i-1])*multiplier + ema[i-1]
	}
	return ema, nil
}

// --- ADDED: Helper function to calculate SMA for a series of floats ---
func CalculateSMA(data []float64, period int) float64 {
	if len(data) < period {
		return 0.0
	}

	segment := data[len(data)-period:]
	sum := 0.0
	for _, v := range segment {
		sum += v
	}
	return sum / float64(period)
}

// CalculateATR explicitly calculates the Average True Range (ATR) over the last 'period' bars.
func CalculateATR(bars []utilities.OHLCVBar, period int) (float64, error) { // Changed signature
	n := len(bars)
	if period <= 0 || n < period+1 {
		// Return an error if conditions are not met
		return 0.0, fmt.Errorf("not enough bars (%d) for ATR calculation of period %d", n, period)
	}

	sum := 0.0
	for i := 1; i <= period; i++ {
		curr := bars[n-i]
		prev := bars[n-i-1]

		highLow := curr.High - curr.Low
		highClose := math.Abs(curr.High - prev.Close)
		lowClose := math.Abs(curr.Low - prev.Close)

		trueRange := math.Max(highLow, math.Max(highClose, lowClose))
		sum += trueRange
	}
	return sum / float64(period), nil // Return result and nil error
}

// AnalyzeOrderBookDepth calculates the ratio of bid volume to ask volume within a
// certain percentage of the mid-price, returning a score from -1.0 to 1.0.
func AnalyzeOrderBookDepth(orderBook broker.OrderBookData, depthPercent float64) float64 {
	if len(orderBook.Bids) == 0 || len(orderBook.Asks) == 0 {
		return 0 // Not enough data to analyze
	}

	bestBid := orderBook.Bids[0].Price
	bestAsk := orderBook.Asks[0].Price
	midPrice := (bestBid + bestAsk) / 2.0

	priceRange := midPrice * (depthPercent / 100.0)
	minPrice := midPrice - priceRange
	maxPrice := midPrice + priceRange

	var bidVolume float64
	for _, bid := range orderBook.Bids {
		if bid.Price >= minPrice {
			bidVolume += bid.Volume
		} else {
			break // Bids are sorted high to low
		}
	}

	var askVolume float64
	for _, ask := range orderBook.Asks {
		if ask.Price <= maxPrice {
			askVolume += ask.Volume
		} else {
			break // Asks are sorted low to high
		}
	}

	if askVolume == 0 {
		if bidVolume > 0 {
			return 1.0 // Infinite bid support, strongly bullish
		}
		return 0.0 // No volume on either side
	}

	ratio := bidVolume / askVolume

	// Normalize the ratio to a score between -1.0 and 1.0.
	// A ratio > 2.0 is strongly bullish, < 0.5 is strongly bearish.
	if ratio >= 2.0 {
		return 1.0
	}
	if ratio <= 0.5 {
		return -1.0
	}

	// Linearly scale the ratio between 0.5 and 2.0 to the range [-1.0, 1.0]
	// Formula: ((value - old_min) / (old_max - old_min)) * (new_max - new_min) + new_min
	return ((ratio-0.5)/(2.0-0.5))*(1.0-(-1.0)) + (-1.0)
}

// VolatilityAdjustedOrderPrice explicitly adjusts an order price based on volatility (ATR).
func VolatilityAdjustedOrderPrice(lastPrice, atr, multiplier float64, buy bool) float64 {
	if buy {
		return lastPrice - atr*multiplier
	}
	return lastPrice + atr*multiplier
}

func CheckMultiIndicatorConfirmation(rsi, stochRSI, macdHist, obv float64, volumeSpike, liquidityHunt bool, bars []utilities.OHLCVBar, cfg utilities.IndicatorsConfig) (bool, string) {
	if liquidityHunt && volumeSpike {
		return true, "Liquidity event with volume spike"
	}

	if rsi < 30 && stochRSI < cfg.StochRSIBuyThreshold && macdHist > 0 && isOBVBullish(obv, bars) {
		return true, "Bullish reversal confirmed by OBV trend"
	}

	if rsi > 70 && stochRSI > cfg.StochRSISellThreshold && macdHist < 0 && isOBVBearish(obv, bars) {
		return true, "Bearish reversal confirmed by OBV trend"
	}

	return false, "No strong multi-indicator confirmation"
}

func isOBVBullish(currentOBV float64, bars []utilities.OHLCVBar) bool {
	historicalOBV := CalculateOBV(bars[:len(bars)-1])
	return currentOBV > historicalOBV
}

func isOBVBearish(currentOBV float64, bars []utilities.OHLCVBar) bool {
	historicalOBV := CalculateOBV(bars[:len(bars)-1])
	return currentOBV < historicalOBV
}

// AdjustPositionSize explicitly adjusts position sizing based on portfolio drawdown risk and volatility.
func AdjustPositionSize(portfolioValue, atr, maxDrawdown float64) float64 {
	riskAmount := portfolioValue * maxDrawdown
	if atr == 0 {
		return 0
	}
	return riskAmount / atr
}

// HandlePartialFills explicitly handles partial fills by calculating a new limit price for unfilled quantity.
func HandlePartialFills(requested, filled, lastPrice, atr, multiplier float64) float64 {
	unfilled := requested - filled
	if unfilled <= 0 {
		return 0
	}
	return VolatilityAdjustedOrderPrice(lastPrice, atr, multiplier, true)
}

// VPVREntry explicitly defines a price bucket and its aggregated volume.
type VPVREntry struct {
	PriceLevel float64
	Volume     float64
}

// CalculateVPVR explicitly calculates the Volume Profile Visible Range (VPVR) by
// binning each bar's volume into the bucket corresponding to its close price,
// then returning entries sorted by descending volume.
func CalculateVPVR(bars []utilities.OHLCVBar, bins int) []VPVREntry {
	entries := []VPVREntry{}
	if bins <= 0 || len(bars) == 0 {
		return entries
	}
	// Determine price range and total volume
	minPrice, maxPrice := bars[0].Low, bars[0].High
	totalVol := 0.0
	for _, bar := range bars {
		totalVol += bar.Volume
		if bar.Low < minPrice {
			minPrice = bar.Low
		}
		if bar.High > maxPrice {
			maxPrice = bar.High
		}
	}

	priceRange := maxPrice - minPrice
	if priceRange == 0 {
		// all prices equal, single bucket
		entries = append(entries, VPVREntry{PriceLevel: minPrice, Volume: totalVol})
		return entries
	}

	binSize := priceRange / float64(bins)
	// accumulate volume per bucket based on close price
	volBuckets := make([]float64, bins)
	for _, bar := range bars {
		idx := int((bar.Close - minPrice) / binSize)
		if idx < 0 {
			idx = 0
		} else if idx >= bins {
			idx = bins - 1
		}
		volBuckets[idx] += bar.Volume
	}

	// build entries at bucket centers
	for i, v := range volBuckets {
		center := minPrice + binSize*float64(i) + binSize/2.0
		entries = append(entries, VPVREntry{PriceLevel: center, Volume: v})
	}

	// sort by volume descending
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Volume > entries[j].Volume
	})
	return entries
}
