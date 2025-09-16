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

// ExtractCloses is a helper function to get a slice of close prices from OHLCV bars.
func ExtractCloses(bars []utilities.OHLCVBar) []float64 {
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

	closePrices := ExtractCloses(bars)

	// Calculate RSI series using Wilder's method
	rsiSeries := make([]float64, 0, len(closePrices)-rsiPeriod)
	for i := rsiPeriod; i < len(closePrices); i++ {
		segment := bars[i-rsiPeriod : i+1] // Use full OHLCV bars for consistency
		var gains, losses float64

		// Initial simple averages for first period of the segment
		for j := 1; j <= rsiPeriod && j < len(segment); j++ {
			change := segment[j].Close - segment[j-1].Close
			if change > 0 {
				gains += change
			} else {
				losses -= change
			}
		}
		avgGain := gains / float64(rsiPeriod)
		avgLoss := losses / float64(rsiPeriod)

		// Apply Wilder's EMA smoothing for the latest change if more bars available
		if len(segment) > rsiPeriod+1 {
			lastChange := segment[len(segment)-1].Close - segment[len(segment)-2].Close
			currentGain := math.Max(lastChange, 0)
			currentLoss := math.Abs(math.Min(lastChange, 0))
			avgGain = (avgGain*float64(rsiPeriod-1) + currentGain) / float64(rsiPeriod)
			avgLoss = (avgLoss*float64(rsiPeriod-1) + currentLoss) / float64(rsiPeriod)
		}

		if avgLoss == 0 {
			rsiSeries = append(rsiSeries, 100)
		} else {
			rs := avgGain / avgLoss
			rsiSeries = append(rsiSeries, 100.0-(100.0/(1.0+rs)))
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

func CalculateMACD(data []float64, fastPeriod, slowPeriod, signalPeriod int) (macdLine, signalLine, macdHist float64, isCrossoverBullish, isCrossoverBearish, isBearishDivergence bool) {
	if len(data) < slowPeriod+signalPeriod+1 { // +1 for prev comparison
		return 0, 0, 0, false, false, false
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
		return 0, 0, 0, false, false, false
	}

	macdSeries := make([]float64, 0, len(data)-slowPeriod+1)
	for i := slowPeriod - 1; i < len(data); i++ {
		macdSeries = append(macdSeries, fastEMA[i]-slowEMA[i])
	}

	if len(macdSeries) < signalPeriod {
		return 0, 0, 0, false, false, false
	}

	signalSeries := ema(macdSeries, signalPeriod)
	if signalSeries == nil {
		return 0, 0, 0, false, false, false
	}

	// Ensure we have enough data for previous values
	if len(macdSeries) < 2 || len(signalSeries) < 2 {
		finalMacdLine := macdSeries[len(macdSeries)-1]
		finalSignalLine := signalSeries[len(signalSeries)-1]
		finalMacdHist := finalMacdLine - finalSignalLine
		return finalMacdLine, finalSignalLine, finalMacdHist, false, false, false
	}

	finalMacdLine := macdSeries[len(macdSeries)-1]
	finalSignalLine := signalSeries[len(signalSeries)-1]
	finalMacdHist := finalMacdLine - finalSignalLine

	// Crossover: Compare current vs previous values with bounds check
	prevMacdLine := macdSeries[len(macdSeries)-2]
	prevSignalLine := signalSeries[len(signalSeries)-2]
	isCrossoverBullish = (prevMacdLine <= prevSignalLine && finalMacdLine > finalSignalLine)
	isCrossoverBearish = (prevMacdLine >= prevSignalLine && finalMacdLine < finalSignalLine)

	// Bearish Divergence: Over lookback=20, price higher high but MACD lower high
	lookback := 20
	if len(data) < lookback+1 { // +1 for current bar
		return finalMacdLine, finalSignalLine, finalMacdHist, isCrossoverBullish, isCrossoverBearish, false
	}
	recentData := data[len(data)-lookback-1:]             // Include current and lookback
	recentMacd := macdSeries[len(macdSeries)-lookback-1:] // Align with data
	if len(recentData) < 2 || len(recentMacd) < 2 {
		return finalMacdLine, finalSignalLine, finalMacdHist, isCrossoverBullish, isCrossoverBearish, false
	}
	priceHighIdx := argmax(recentData)
	macdHighIdx := argmax(recentMacd)
	isBearishDivergence = (recentData[len(recentData)-1] > recentData[priceHighIdx] && recentMacd[len(recentMacd)-1] < recentMacd[macdHighIdx])

	return finalMacdLine, finalSignalLine, finalMacdHist, isCrossoverBullish, isCrossoverBearish, isBearishDivergence
}

// Helper function to find the index of the maximum value in a slice
func argmax(slice []float64) int {
	maxIdx := 0
	for i := 1; i < len(slice); i++ {
		if slice[i] > slice[maxIdx] {
			maxIdx = i
		}
	}
	return maxIdx
}

// CalculateMACDSeries calculates and returns the full series for MACD line, signal line, and histogram.
func CalculateMACDSeries(data []float64, fastPeriod, slowPeriod, signalPeriod int) ([]float64, []float64, []float64) {
	if len(data) < slowPeriod+signalPeriod {
		return nil, nil, nil
	}

	// ema is a local helper function for this calculation.
	ema := func(series []float64, period int) []float64 {
		if len(series) < period {
			return nil
		}
		res := make([]float64, len(series))
		multiplier := 2.0 / float64(period+1)

		// Calculate initial SMA for the first EMA value
		sum := 0.0
		for i := 0; i < period; i++ {
			sum += series[i]
		}
		res[period-1] = sum / float64(period)

		// Calculate the rest of the EMA series
		for i := period; i < len(series); i++ {
			res[i] = (series[i]-res[i-1])*multiplier + res[i-1]
		}
		return res
	}

	fastEMA := ema(data, fastPeriod)
	slowEMA := ema(data, slowPeriod)

	if fastEMA == nil || slowEMA == nil {
		return nil, nil, nil
	}

	// Calculate the MACD line series
	macdSeries := make([]float64, len(data))
	for i := slowPeriod - 1; i < len(data); i++ {
		macdSeries[i] = fastEMA[i] - slowEMA[i]
	}
	macdSeries = macdSeries[slowPeriod-1:] // Trim leading empty values

	if len(macdSeries) < signalPeriod {
		return nil, nil, nil
	}

	// Calculate the signal line series
	signalSeries := ema(macdSeries, signalPeriod)
	if signalSeries == nil {
		return nil, nil, nil
	}
	signalSeries = signalSeries[signalPeriod-1:] // Trim leading empty values
	macdSeries = macdSeries[signalPeriod-1:]     // Align MACD series with signal series

	// Calculate the histogram series
	histSeries := make([]float64, len(signalSeries))
	for i := range signalSeries {
		histSeries[i] = macdSeries[i] - signalSeries[i]
	}

	return macdSeries, signalSeries, histSeries
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

func CalculateVWAP(bars []utilities.OHLCVBar, period int) float64 {
	if len(bars) < period {
		return 0
	}
	segment := bars[len(bars)-period:]
	var sumPV, sumV float64
	for _, bar := range segment {
		typicalPrice := (bar.High + bar.Low + bar.Close) / 3
		sumPV += typicalPrice * bar.Volume
		sumV += bar.Volume
	}
	if sumV == 0 {
		return 0
	}
	return sumPV / sumV
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

// CalculateBollingerBands calculates Bollinger Bands using configurable period and standard deviation.
// Uses defaults (period=20, stdDev=2.0) if config values are invalid or missing.
func CalculateBollingerBands(bars []utilities.OHLCVBar, cfg utilities.AppConfig) (upperBand, lowerBand float64) {
	period := cfg.Indicators.BollingerPeriod
	if period <= 0 { // Fallback to default if invalid
		period = 20
	}
	stdDev := cfg.Indicators.BollingerStdDev
	if stdDev <= 0 { // Fallback to default if invalid
		stdDev = 2.0
	}

	if len(bars) < period {
		return 0, 0
	}

	closes := ExtractCloses(bars)
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

// CalculateWilderRSI calculates RSI using Wilder's EMA smoothing method.
func CalculateWilderRSI(bars []utilities.OHLCVBar, period int) float64 {
	if len(bars) < period+1 || period <= 0 {
		return 50.0 // neutral
	}

	closes := make([]float64, len(bars))
	for i, v := range bars {
		closes[i] = v.Close
	}

	// Initial simple averages for first period
	var gains, losses float64
	for i := 1; i <= period; i++ {
		change := closes[i] - closes[i-1]
		if change > 0 {
			gains += change
		} else {
			losses -= change
		}
	}
	avgGain := gains / float64(period)
	avgLoss := losses / float64(period)

	// For the latest value, use EMA smoothing if more bars available
	if len(closes) > period+1 {
		change := closes[len(closes)-1] - closes[len(closes)-2]
		currentGain := math.Max(change, 0)
		currentLoss := math.Abs(math.Min(change, 0))
		avgGain = (avgGain*float64(period-1) + currentGain) / float64(period)
		avgLoss = (avgLoss*float64(period-1) + currentLoss) / float64(period)
	}

	if avgLoss == 0 {
		return 100.0
	}
	rs := avgGain / avgLoss
	return 100.0 - (100.0 / (1.0 + rs))
}

// CalculateStochastic calculates the Stochastic Oscillator (%K and %D), with optional Slow Stochastic smoothing.
// Uses Fast Stochastic by default; enables Slow if use_slow_stochastic is true.
func CalculateStochastic(bars []utilities.OHLCVBar, cfg utilities.AppConfig) (percentK, percentD float64) {
	kPeriod := cfg.Indicators.StochasticK
	dPeriod := cfg.Indicators.StochasticD
	slowPeriod := cfg.Indicators.StochasticSlowPeriod
	useSlow := cfg.Indicators.UseSlowStochastic

	if len(bars) < kPeriod {
		return 0, 0
	}

	// Calculate Fast %K series
	kSeries := make([]float64, 0, len(bars)-kPeriod+1)
	for i := kPeriod - 1; i < len(bars); i++ {
		subSegment := bars[i-kPeriod+1 : i+1]
		subLow := subSegment[0].Low
		subHigh := subSegment[0].High
		for _, bar := range subSegment {
			if bar.Low < subLow {
				subLow = bar.Low
			}
			if bar.High > subHigh {
				subHigh = bar.High
			}
		}
		currentClose := bars[i].Close
		k := ((currentClose - subLow) / (subHigh - subLow)) * 100
		kSeries = append(kSeries, k)
	}

	// Determine %K based on mode
	if len(kSeries) == 0 {
		return 0, 0
	}
	var slowKSeries []float64
	if useSlow && len(kSeries) >= slowPeriod {
		slowKSeries = make([]float64, 0, len(kSeries)-slowPeriod+1)
		for i := slowPeriod - 1; i < len(kSeries); i++ {
			sum := 0.0
			for j := 0; j < slowPeriod; j++ {
				sum += kSeries[i-j]
			}
			slowKSeries = append(slowKSeries, sum/float64(slowPeriod))
		}
		kSeries = slowKSeries // Replace with Slow %K
	}
	percentK = kSeries[len(kSeries)-1] // Latest %K (Fast or Slow)

	// Calculate %D: SMA of %K (or Slow %K) over dPeriod
	if dPeriod > 1 && len(kSeries) >= dPeriod {
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
	vwap float64,
) {
	indicatorCfg := cfg.Indicators
	closePrices := ExtractCloses(bars)

	rsi = CalculateWilderRSI(bars, indicatorCfg.RSIPeriod)
	stochRSI = CalculateStochRSI(bars, indicatorCfg.RSIPeriod, indicatorCfg.StochRSIPeriod)
	_, _, macdHist, _, _, _ = CalculateMACD(closePrices, indicatorCfg.MACDFastPeriod, indicatorCfg.MACDSlowPeriod, indicatorCfg.MACDSignalPeriod)
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
	upperBB, lowerBB = CalculateBollingerBands(bars, cfg) // Standard 20-period, 2 stdDev

	// New: Stochastic Oscillator (using config periods; assume added to IndicatorsConfig)
	stochK, stochD = CalculateStochastic(bars, cfg)
	vwap = CalculateVWAP(bars, 20)

	// Explicitly return all named variables to resolve the "unused variable" error.
	return rsi, stochRSI, macdHist, obv, volumeSpike, bearishHuntReversal, upperBB, lowerBB, stochK, stochD, vwap
}
