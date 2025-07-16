package strategy

import (
	"Snowballin/utilities"
	"math"
)

// CalculateRSI calculates the Relative Strength Index (RSI).
func CalculateRSI(bars []utilities.OHLCVBar, period int) float64 {
	if len(bars) < period+1 || period <= 0 {
		return 50.0 // neutral
	}

	closes := make([]float64, len(bars))
	for i, v := range bars {
		closes[i] = v.Close
	}

	var gains, losses float64
	for i := len(closes) - period; i < len(closes); i++ {
		change := closes[i] - closes[i-1]
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

// extractCloses is a helper function to get a slice of close prices from OHLCV bars.
func extractClosesForIndicators(bars []utilities.OHLCVBar) []float64 {
	closes := make([]float64, len(bars))
	for i, bar := range bars {
		closes[i] = bar.Close
	}
	return closes
}

// CalculateStochRSI calculates the Stochastic RSI.
func CalculateStochRSI(bars []utilities.OHLCVBar, rsiPeriod, stochPeriod int) float64 {
	if len(bars) < rsiPeriod+stochPeriod {
		return 0
	}

	closePrices := extractClosesForIndicators(bars)

	// Calculate RSI series
	rsiSeries := make([]float64, 0, len(closePrices)-rsiPeriod)
	for i := rsiPeriod; i < len(closePrices); i++ {
		segment := closePrices[i-rsiPeriod : i+1]
		var gain, loss float64
		for j := 1; j < len(segment); j++ {
			change := segment[j] - segment[j-1]
			if change > 0 {
				gain += change
			} else {
				loss -= change
			}
		}

		if loss == 0 {
			rsiSeries = append(rsiSeries, 100)
		} else {
			rs := (gain / float64(rsiPeriod)) / (loss / float64(rsiPeriod))
			rsiSeries = append(rsiSeries, 100-(100/(1+rs)))
		}
	}

	if len(rsiSeries) < stochPeriod {
		return 0
	}

	relevantRSI := rsiSeries[len(rsiSeries)-stochPeriod:]
	currentRSI := relevantRSI[len(relevantRSI)-1]
	minRSI, maxRSI := relevantRSI[0], relevantRSI[0]
	for _, rsi := range relevantRSI {
		if rsi < minRSI {
			minRSI = rsi
		}
		if rsi > maxRSI {
			maxRSI = rsi
		}
	}

	if maxRSI == minRSI {
		return 100
	}

	return ((currentRSI - minRSI) / (maxRSI - minRSI)) * 100
}

// CalculateMACD calculates the MACD line, signal line, and histogram.
func CalculateMACD(data []float64, fastPeriod, slowPeriod, signalPeriod int) (macdLine, signalLine, macdHist float64) {
	if len(data) < slowPeriod+signalPeriod {
		return 0, 0, 0
	}

	ema := func(series []float64, period int) []float64 {
		if len(series) < period {
			return nil
		}
		res := make([]float64, len(series))
		multiplier := 2.0 / float64(period+1)
		sum := 0.0
		for i := 0; i < period; i++ {
			sum += series[i]
		}
		res[period-1] = sum / float64(period)
		for i := period; i < len(series); i++ {
			res[i] = (series[i]-res[i-1])*multiplier + res[i-1]
		}
		return res
	}

	fastEMA := ema(data, fastPeriod)
	slowEMA := ema(data, slowPeriod)

	if fastEMA == nil || slowEMA == nil {
		return 0, 0, 0
	}

	macdSeries := make([]float64, 0, len(data)-slowPeriod+1)
	for i := slowPeriod - 1; i < len(data); i++ {
		macdSeries = append(macdSeries, fastEMA[i]-slowEMA[i])
	}

	if len(macdSeries) < signalPeriod {
		return 0, 0, 0
	}

	signalSeries := ema(macdSeries, signalPeriod)
	if signalSeries == nil {
		return 0, 0, 0
	}

	finalMacdLine := macdSeries[len(macdSeries)-1]
	finalSignalLine := signalSeries[len(signalSeries)-1]
	finalMacdHist := finalMacdLine - finalSignalLine

	return finalMacdLine, finalSignalLine, finalMacdHist
}

// CalculateOBV calculates the On-Balance Volume for a series of bars.
func CalculateOBV(bars []utilities.OHLCVBar) float64 {
	var obv float64
	if len(bars) < 2 {
		return 0
	}
	for i := 1; i < len(bars); i++ {
		if bars[i].Close > bars[i-1].Close {
			obv += bars[i].Volume
		} else if bars[i].Close < bars[i-1].Close {
			obv -= bars[i].Volume
		}
	}
	return obv
}

// CheckVolumeSpike checks for volume spikes.
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

// IsLiquidityHuntDetected identifies a potential liquidity hunt.
func IsLiquidityHuntDetected(bars []utilities.OHLCVBar, minWickPercent, volSpikeMultiplier float64, volMAPeriod int) bool {
	if len(bars) < volMAPeriod+2 {
		return false
	}
	wickyCandle := bars[len(bars)-2]
	reversalCandle := bars[len(bars)-1]

	if wickyCandle.Close >= wickyCandle.Open {
		return false
	}

	candleRange := wickyCandle.High - wickyCandle.Low
	if candleRange == 0 {
		return false
	}
	lowerWick := wickyCandle.Close - wickyCandle.Low
	if wickyCandle.Open < wickyCandle.Close {
		lowerWick = wickyCandle.Open - wickyCandle.Low
	}

	if (lowerWick / candleRange) < (minWickPercent / 100.0) {
		return false
	}

	if reversalCandle.Close <= reversalCandle.Open || reversalCandle.Close <= wickyCandle.Open {
		return false
	}

	totalVolume := 0.0
	volumeBars := bars[len(bars)-2-volMAPeriod : len(bars)-2]
	for _, bar := range volumeBars {
		totalVolume += bar.Volume
	}
	avgVolume := totalVolume / float64(volMAPeriod)

	wickyVolSpike := wickyCandle.Volume > (avgVolume * volSpikeMultiplier)
	reversalVolSpike := reversalCandle.Volume > (avgVolume * volSpikeMultiplier)

	return wickyVolSpike || reversalVolSpike
}

// IsBearishReversalAfterHuntDetected identifies a potential bearish reversal after a bullish liquidity sweep.
// This is a robust EXIT signal
func IsBearishReversalAfterHuntDetected(bars []utilities.OHLCVBar, minWickPercent, volSpikeMultiplier float64, volMAPeriod int) bool {
	// We need at least the volume lookback period + the wicky candle + the reversal candle.
	if len(bars) < volMAPeriod+2 {
		return false
	}

	wickyCandle := bars[len(bars)-2]    // The bullish "sweep" candle.
	reversalCandle := bars[len(bars)-1] // The bearish "confirmation" candle.

	// The sweep candle must be bullish (a push upwards).
	if wickyCandle.Close <= wickyCandle.Open {
		return false
	}

	candleRange := wickyCandle.High - wickyCandle.Low
	if candleRange == 0 {
		return false // Avoid division by zero on doji candles.
	}

	// Calculate the upper wick size. It must be a significant portion of the candle.
	upperWick := wickyCandle.High - wickyCandle.Close
	if (upperWick / candleRange) < (minWickPercent / 100.0) {
		return false // The upper wick isn't long enough to suggest a rejection.
	}

	// The next candle must be a bearish reversal candle, confirming the rejection.
	// It must be a red candle AND it must close below the open of the wicky candle.
	if reversalCandle.Close >= reversalCandle.Open || reversalCandle.Close >= wickyCandle.Open {
		return false
	}

	// Check for a volume spike on either the sweep or the reversal candle to confirm conviction.
	totalVolume := 0.0
	volumeBars := bars[len(bars)-2-volMAPeriod : len(bars)-2]
	for _, bar := range volumeBars {
		totalVolume += bar.Volume
	}
	avgVolume := totalVolume / float64(volMAPeriod)

	wickyVolSpike := wickyCandle.Volume > (avgVolume * volSpikeMultiplier)
	reversalVolSpike := reversalCandle.Volume > (avgVolume * volSpikeMultiplier)

	return wickyVolSpike || reversalVolSpike
}

// CalculateBollingerBands calculates the upper and lower Bollinger Bands for the last bar.
func CalculateBollingerBands(bars []utilities.OHLCVBar, period int, stdDev float64) (upperBand, lowerBand float64) {
	if len(bars) < period {
		return 0, 0
	}

	closes := extractClosesForIndicators(bars)
	segment := closes[len(closes)-period:]

	// Calculate SMA (middle band)
	sum := 0.0
	for _, close := range segment {
		sum += close
	}
	sma := sum / float64(period)

	// Calculate standard deviation
	variance := 0.0
	for _, close := range segment {
		variance += math.Pow(close-sma, 2)
	}
	std := math.Sqrt(variance / float64(period))

	upperBand = sma + (std * stdDev)
	lowerBand = sma - (std * stdDev)

	return upperBand, lowerBand
}

// CalculateStochastic calculates the Stochastic Oscillator (%K and %D) for the last bar.
func CalculateStochastic(bars []utilities.OHLCVBar, kPeriod, dPeriod int) (percentK, percentD float64) {
	if len(bars) < kPeriod {
		return 0, 0
	}

	// For %K
	segment := bars[len(bars)-kPeriod:]
	lowestLow := segment[0].Low
	highestHigh := segment[0].High
	for _, bar := range segment {
		if bar.Low < lowestLow {
			lowestLow = bar.Low
		}
		if bar.High > highestHigh {
			highestHigh = bar.High
		}
	}
	currentClose := segment[len(segment)-1].Close
	percentK = ((currentClose - lowestLow) / (highestHigh - lowestLow)) * 100

	// For %D: SMA of %K over dPeriod (need historical %K if dPeriod >1)
	// Simplified: If dPeriod==1, %D = %K; else compute full series if needed
	// For now, assume simple %D as SMA of last dPeriod closes as proxy (expand for accuracy)
	if dPeriod > 1 {
		kSeries := make([]float64, 0, len(bars)-kPeriod+1)
		for i := kPeriod - 1; i < len(bars); i++ {
			subSegment := bars[i-kPeriod+1 : i+1]
			subLow := subSegment[0].Low
			subHigh := subSegment[0].High
			for _, b := range subSegment {
				if b.Low < subLow {
					subLow = b.Low
				}
				if b.High > subHigh {
					subHigh = b.High
				}
			}
			k := ((bars[i].Close - subLow) / (subHigh - subLow)) * 100
			kSeries = append(kSeries, k)
		}
		dSegment := kSeries[len(kSeries)-dPeriod:]
		sumD := 0.0
		for _, k := range dSegment {
			sumD += k
		}
		percentD = sumD / float64(dPeriod)
	} else {
		percentD = percentK
	}

	return percentK, percentD
}

// CalculateIndicators aggregates all primary indicators.
func CalculateIndicators(bars []utilities.OHLCVBar, cfg utilities.AppConfig) (
	rsi float64,
	stochRSI float64,
	macdHist float64,
	obv float64,
	volumeSpike bool,
	liquidityHunt bool,
	upperBB float64,
	lowerBB float64,
	stochK float64,
	stochD float64,
) {
	indicatorCfg := cfg.Indicators
	closePrices := extractClosesForIndicators(bars)

	rsi = CalculateRSI(bars, indicatorCfg.RSIPeriod)
	stochRSI = CalculateStochRSI(bars, indicatorCfg.RSIPeriod, indicatorCfg.StochRSIPeriod)
	_, _, macdHist = CalculateMACD(closePrices, indicatorCfg.MACDFastPeriod, indicatorCfg.MACDSlowPeriod, indicatorCfg.MACDSignalPeriod)
	obv = CalculateOBV(bars)
	volumeSpike = CheckVolumeSpike(bars, indicatorCfg.VolumeSpikeFactor, indicatorCfg.VolumeLookbackPeriod)

	// Call our NEW function here
	bearishHuntReversal := IsBearishReversalAfterHuntDetected(
		bars,
		indicatorCfg.LiquidityHunt.MinWickPercent,
		indicatorCfg.LiquidityHunt.VolSpikeMultiplier,
		indicatorCfg.LiquidityHunt.VolMAPeriod,
	)

	// New: Bollinger Bands (using defaults or add config if needed)
	upperBB, lowerBB = CalculateBollingerBands(bars, 20, 2.0) // Standard 20-period, 2 stdDev

	// New: Stochastic Oscillator (using config periods; assume added to IndicatorsConfig)
	stochK, stochD = CalculateStochastic(bars, indicatorCfg.StochasticK, indicatorCfg.StochasticD) // e.g., 14,3

	// Explicitly return all named variables to resolve the "unused variable" error.
	return rsi, stochRSI, macdHist, obv, volumeSpike, bearishHuntReversal, upperBB, lowerBB, stochK, stochD
}
