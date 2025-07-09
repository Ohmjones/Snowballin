package strategy

import (
	"Snowballin/utilities"
	"math"
)

// CalculateRSI explicitly calculates the Relative Strength Index (RSI) over the given bars.
func CalculateRSI(bars []utilities.OHLCVBar, period int) float64 {
	if len(bars) < period+1 || period <= 0 {
		return 50.0 // neutral
	}
	gains, losses := 0.0, 0.0
	for i := len(bars) - period; i < len(bars); i++ {
		change := bars[i].Close - bars[i-1].Close
		if change > 0 {
			gains += change
		} else {
			losses -= change
		}
	}
	avgGain := gains / float64(period)
	avgLoss := losses / float64(period)
	if avgLoss == 0 {
		return 100.0
	}
	rs := avgGain / avgLoss
	return 100.0 - (100.0 / (1.0 + rs))
}

// CalculateStochRSI explicitly calculates the Stochastic RSI over the given bars.
func CalculateStochRSI(bars []utilities.OHLCVBar, rsiPeriod, stochPeriod int) float64 {
	// build RSI series
	rsiValues := make([]float64, len(bars)-rsiPeriod)
	for i := rsiPeriod; i < len(bars); i++ {
		rsiValues[i-rsiPeriod] = CalculateRSI(bars[i-rsiPeriod:i+1], rsiPeriod)
	}
	if len(rsiValues) < stochPeriod {
		return 50.0
	}
	// recent window
	recent := rsiValues[len(rsiValues)-stochPeriod:]
	minVal, maxVal := recent[0], recent[0]
	for _, v := range recent {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	if maxVal == minVal {
		return 50.0
	}
	return 100.0 * (recent[len(recent)-1] - minVal) / (maxVal - minVal)
}

// CalculateMACD explicitly computes the MACD histogram over the given bars.
func CalculateMACD(bars []utilities.OHLCVBar, fastPeriod, slowPeriod, signalPeriod int) float64 {
	// extract closes
	closes := make([]float64, len(bars))
	for i, bar := range bars {
		closes[i] = bar.Close
	}
	// EMAs
	fastEMA, _ := ComputeEMASeries(closes, fastPeriod)
	slowEMA, _ := ComputeEMASeries(closes, slowPeriod)
	// MACD line series
	histSeries := make([]float64, len(closes))
	for i := range closes {
		histSeries[i] = fastEMA[i] - slowEMA[i]
	}
	// signal line
	signalEMA, _ := ComputeEMASeries(histSeries, signalPeriod)
	// histogram = macd line - signal line
	idx := len(histSeries) - 1
	return histSeries[idx] - signalEMA[idx]
}

// CalculateOBV explicitly computes final cumulative On-Balance Volume over the given bars.
func CalculateOBV(bars []utilities.OHLCVBar) float64 {
	obv := 0.0
	for i := 1; i < len(bars); i++ {
		if bars[i].Close > bars[i-1].Close {
			obv += bars[i].Volume
		} else if bars[i].Close < bars[i-1].Close {
			obv -= bars[i].Volume
		}
	}
	return obv
}

// CheckVolumeSpike explicitly checks for volume spikes in the given bars.
func CheckVolumeSpike(bars []utilities.OHLCVBar, factor float64, period int) bool {
	if len(bars) <= period {
		return false
	}
	sum := 0.0
	for i := len(bars) - period - 1; i < len(bars)-1; i++ {
		sum += bars[i].Volume
	}
	avg := sum / float64(period)
	return bars[len(bars)-1].Volume >= avg*factor
}

// IsLiquidityHuntDetected explicitly detects liquidity-hunt candlestick patterns.
func IsLiquidityHuntDetected(bars []utilities.OHLCVBar) bool {
	if len(bars) < 2 {
		return false
	}
	last := bars[len(bars)-1]
	body := math.Abs(last.Close - last.Open)
	if body == 0 {
		return false
	}
	wickUpper := last.High - math.Max(last.Open, last.Close)
	wickLower := math.Min(last.Open, last.Close) - last.Low
	threshold := 2.0
	return (wickUpper/body >= threshold) || (wickLower/body >= threshold)
}

// CalculateIndicators explicitly aggregates all primary indicators.
func CalculateIndicators(bars []utilities.OHLCVBar, cfg utilities.IndicatorsConfig) (
	rsi float64,
	stochRSI float64,
	macdHist float64,
	obv float64,
	volumeSpike bool,
	liquidityHunt bool,
) {
	rsi = CalculateRSI(bars, cfg.RSIPeriod)
	stochRSI = CalculateStochRSI(bars, cfg.RSIPeriod, cfg.StochRSIPeriod)
	macdHist = CalculateMACD(bars, cfg.MACDFastPeriod, cfg.MACDSlowPeriod, cfg.MACDSignalPeriod)
	obv = CalculateOBV(bars)
	volumeSpike = CheckVolumeSpike(bars, cfg.VolumeSpikeFactor, cfg.VolumeLookbackPeriod)
	liquidityHunt = IsLiquidityHuntDetected(bars)
	return
}
