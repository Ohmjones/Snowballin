package strategy

import (
	"Snowballin/utilities"
	"context"
	"fmt"
	"math"
	"strings"
	"time"
)

// NewStrategy constructs a new strategy instance.
func NewStrategy(logger *utilities.Logger) Strategy {
	return &strategyImpl{logger: logger}
}

// extractCloses is a helper function to get a slice of close prices from OHLCV bars.
func extractCloses(bars []utilities.OHLCVBar) []float64 {
	closes := make([]float64, len(bars))
	for i, bar := range bars {
		closes[i] = bar.Close
	}
	return closes
}

// getConsensusIndicatorValue calculates a weighted average for a given indicator across all available data providers.
// It takes a calculation function `calcFunc` which defines how to get the desired value from a set of bars.
func getConsensusIndicatorValue(
	data ConsolidatedMarketPicture,
	timeframe string,
	minBars int,
	calcFunc func(bars []utilities.OHLCVBar) (float64, bool),
) (float64, bool) {
	var weightedSum, totalWeight float64
	var providersWithValue int

	for _, provider := range data.ProvidersData {
		bars, hasBars := provider.OHLCVByTF[timeframe]
		if !hasBars || len(bars) < minBars {
			continue // Skip provider if it doesn't have enough data for this timeframe.
		}

		// Execute the provided calculation function to get the indicator value.
		value, ok := calcFunc(bars)
		if ok {
			weightedSum += value * provider.Weight
			totalWeight += provider.Weight
			providersWithValue++
		}
	}

	// Ensure we have data from at least one provider to avoid division by zero.
	if totalWeight > 0 {
		return weightedSum / totalWeight, true
	}

	return 0, false
}

// checkMultiTimeframeConsensus applies a hierarchical filter using a weighted consensus from all data providers.
func (s *strategyImpl) checkMultiTimeframeConsensus(data ConsolidatedMarketPicture, cfg utilities.AppConfig) (bool, string) {
	// --- Gate 1: Daily Trend (EMA) ---
	// This is our primary safety rail. We MUST be in a long-term uptrend.
	consensusLastClose, priceOk := getConsensusIndicatorValue(data, "1d", 50, func(bars []utilities.OHLCVBar) (float64, bool) {
		return bars[len(bars)-1].Close, true
	})
	consensusEMA50, emaOk := getConsensusIndicatorValue(data, "1d", 50, func(bars []utilities.OHLCVBar) (float64, bool) {
		closes := extractCloses(bars)
		ema, _ := ComputeEMASeries(closes, 50)
		if len(ema) > 0 {
			return ema[len(ema)-1], true
		}
		return 0, false
	})

	if !priceOk || !emaOk {
		return false, "Hold: Insufficient daily data for trend analysis."
	}
	if consensusLastClose < consensusEMA50 {
		return false, fmt.Sprintf("Hold: Daily trend is bearish. Price (%.2f) is below Daily EMA(50) (%.2f)", consensusLastClose, consensusEMA50)
	}
	s.logger.LogInfo("MTF Consensus [GATE 1 PASSED]: Daily trend is bullish.")

	// --- Gate 2: 4-Hour Momentum (MACD) ---
	// We check if the downward momentum is starting to ease up.
	var isMomentumShifting bool
	var momentumReason string
	consensusMacdHistSeries, macdOk := getConsensusIndicatorValue(data, "4h", cfg.Indicators.MACDSlowPeriod+cfg.Indicators.MACDSignalPeriod, func(bars []utilities.OHLCVBar) (float64, bool) {
		closes := extractCloses(bars)
		_, _, macdHist := CalculateMACDSeries(closes, cfg.Indicators.MACDFastPeriod, cfg.Indicators.MACDSlowPeriod, cfg.Indicators.MACDSignalPeriod)
		if len(macdHist) < 2 {
			return 0, false // Not enough data for comparison
		}
		if macdHist[len(macdHist)-1] > macdHist[len(macdHist)-2] {
			return 1.0, true // Momentum is shifting up
		}
		return -1.0, true
	})
	if macdOk && consensusMacdHistSeries > 0 {
		isMomentumShifting = true
		momentumReason = "4H Momentum Shift"
	}

	// --- Gate 3: 1-Hour Entry Trigger (RSI) ---
	// We check if the asset is currently in a short-term dip.
	var isRsiLow bool
	var rsiReason string
	consensusRSI, rsiOk := getConsensusIndicatorValue(data, "1h", cfg.Indicators.RSIPeriod, func(bars []utilities.OHLCVBar) (float64, bool) {
		rsi := CalculateRSI(bars, cfg.Indicators.RSIPeriod)
		return rsi, true
	})
	if rsiOk && consensusRSI <= 45 {
		isRsiLow = true
		rsiReason = fmt.Sprintf("1H RSI Low (%.2f)", consensusRSI)
	}

	// --- FINAL LOGIC: Gate 1 AND (Gate 2 OR Gate 3) ---
	// This is the new, more aggressive logic. As long as the daily trend is bullish,
	// we only need ONE of the other conditions to be true to generate a buy signal.
	if isMomentumShifting || isRsiLow {
		var finalReason string
		if isMomentumShifting && isRsiLow {
			finalReason = fmt.Sprintf("%s & %s", momentumReason, rsiReason)
		} else if isMomentumShifting {
			finalReason = momentumReason
		} else {
			finalReason = rsiReason
		}
		s.logger.LogInfo("MTF Consensus [GATES 2/3 PASSED]: %s", finalReason)
		return true, finalReason
	}

	// If we reach here, neither of the secondary conditions were met.
	return false, "Hold: Neither 4H momentum shift nor 1H low RSI conditions were met."
}

func getConsensusBars(data ConsolidatedMarketPicture, timeframe string, minBars int) ([]utilities.OHLCVBar, bool) {
	var allBars [][]utilities.OHLCVBar
	for _, provider := range data.ProvidersData {
		bars, hasBars := provider.OHLCVByTF[timeframe]
		if hasBars && len(bars) >= minBars {
			allBars = append(allBars, bars)
		}
	}
	if len(allBars) == 0 {
		return nil, false
	}

	// Align by timestamp (assume sorted and same timestamps; in prod, interpolate if needed)
	consensusBars := make([]utilities.OHLCVBar, len(allBars[0]))
	for i := range consensusBars {
		var o, h, l, c, v float64
		count := float64(len(allBars))
		for _, bars := range allBars {
			o += bars[i].Open / count
			h += bars[i].High / count
			l += bars[i].Low / count
			c += bars[i].Close / count
			v += bars[i].Volume / count
		}
		consensusBars[i] = utilities.OHLCVBar{
			Timestamp: allBars[0][i].Timestamp,
			Open:      o,
			High:      h,
			Low:       l,
			Close:     c,
			Volume:    v,
		}
	}
	return consensusBars, true
}

// GenerateSignals produces intelligent entry signals with calculated price, size, and stop-loss.
func (s *strategyImpl) GenerateSignals(ctx context.Context, data ConsolidatedMarketPicture, cfg utilities.AppConfig) ([]StrategySignal, error) {
	_ = ctx // Acknowledge unused parameter

	if len(data.ProvidersData) == 0 {
		return nil, fmt.Errorf("cannot generate signals without any provider data")
	}

	// For final confirmation and ATR, we use the primary (broker) bars for simplicity and direct execution context.
	primaryBars, ok := data.PrimaryOHLCVByTF[cfg.Consensus.MultiTimeframe.BaseTimeframe]
	if !ok || len(primaryBars) < cfg.Indicators.MACDSlowPeriod {
		s.logger.LogWarn("GenerateSignals [%s]: Insufficient primary (broker) bars (%d). Falling back to provider consensus for indicators.", data.AssetPair, len(primaryBars))

		// Fallback: Build consensus primary bars from providers (daily for trend)
		fallbackBars, fallbackOk := getConsensusBars(data, cfg.Consensus.MultiTimeframe.BaseTimeframe, cfg.Indicators.MACDSlowPeriod)
		if !fallbackOk {
			s.logger.LogWarn("GenerateSignals [%s]: Fallback failed—insufficient provider bars for consensus.", data.AssetPair)
			return nil, nil
		}
		primaryBars = fallbackBars // Override with fallback
	}
	atr, err := CalculateATR(primaryBars, cfg.Indicators.ATRPeriod)
	if err != nil {
		s.logger.LogError("GenerateSignals [%s]: Could not calculate ATR: %v", data.AssetPair, err)
		return nil, err
	}

	// Perform Multi-Timeframe Consensus check
	isBuy, reason := s.checkMultiTimeframeConsensus(data, cfg)

	// Extract currentPrice early for use in predictive logic
	currentPrice := data.ProvidersData[0].CurrentPrice

	// --- PREDICTIVE BUY LOGIC V2 ---
	// This logic attempts to "snipe" a whipsaw by placing a limit order at a deep,
	// volatility-defined support level, bypassing the standard MTF consensus.
	bookAnalysis := PerformOrderBookAnalysis(data.BrokerOrderBook, 1.0, 10, 1.5)
	if !isBuy && bookAnalysis.DepthScore > cfg.Trading.MinBookConfidenceForPredictive {
		s.logger.LogInfo("GenerateSignals [%s]: MTF Consensus failed, but strong book (%.2f). Checking for predictive snipe...", data.AssetPair, bookAnalysis.DepthScore)

		// Fetch primary bars for ATR and RSI calculation to ensure data integrity.
		primaryBars, hasPrimaryBars := data.PrimaryOHLCVByTF[cfg.Consensus.MultiTimeframe.BaseTimeframe]
		if !hasPrimaryBars || len(primaryBars) < cfg.Indicators.ATRPeriod {
			s.logger.LogWarn("GenerateSignals [%s]: Insufficient broker bars for predictive buy. Skipping.", data.AssetPair)
			return nil, nil
		}

		// 1. Calculate ATR to determine a dynamic "snipe" distance based on current volatility.
		atr, err := CalculateATR(primaryBars, cfg.Indicators.ATRPeriod)
		if err != nil || atr == 0 {
			s.logger.LogError("GenerateSignals [%s]: Could not calculate ATR for predictive buy: %v", data.AssetPair, err)
			return nil, nil
		}

		// 2. Define the ideal target price for the snipe. This is a deep level.
		targetSnipePrice := currentPrice - (atr * cfg.Trading.PredictiveAtrMultiplier)

		// 3. Search for the strongest buy wall (support) *around* that ideal target price.
		snipePrice, foundSupport := FindSupportNearPrice(data.BrokerOrderBook, targetSnipePrice, cfg.Trading.PredictiveSearchWindowPercent)
		if !foundSupport {
			s.logger.LogInfo("GenerateSignals [%s]: Predictive snipe skipped. No significant support wall found near the target price of %.2f.", data.AssetPair, targetSnipePrice)
			return nil, nil
		}

		// 4. Final check: Ensure we are not trying to catch a rip on high volume.
		volumeSpike := CheckVolumeSpike(primaryBars, cfg.Indicators.VolumeSpikeFactor, cfg.Indicators.VolumeLookbackPeriod)
		if volumeSpike {
			s.logger.LogInfo("GenerateSignals [%s]: Predictive snipe skipped due to recent volume spike.", data.AssetPair)
			return nil, nil
		}

		// 5. Generate the signal.
		s.logger.LogWarn("GenerateSignals [%s]: PREDICTIVE BUY SIGNAL TRIGGERED!", data.AssetPair)
		predictiveSignal := StrategySignal{
			Direction:        "predictive_buy",
			Reason:           fmt.Sprintf("Predictive Snipe: Strong book (Conf: %.2f) at volatility-defined support. Target: %.2f", bookAnalysis.DepthScore, snipePrice),
			RecommendedPrice: snipePrice,
			CalculatedSize:   0, // Size will be calculated in the app layer based on Martingale logic.
		}
		return []StrategySignal{predictiveSignal}, nil
	}
	// --- END OF PREDICTIVE BUY LOGIC ---

	if !isBuy {
		s.logger.LogInfo("GenerateSignals: %s -> %s - Reason: %s",
			utilities.ColorYellow+data.AssetPair+utilities.ColorReset,
			utilities.ColorWhite+"HOLD"+utilities.ColorReset,
			strings.TrimPrefix(reason, "Hold: "),
		)
		return nil, nil
	}
	s.logger.LogInfo("GenerateSignals [%s]: MTF Consensus PASSED. Reason: %s. Performing final confirmation...", data.AssetPair, reason)

	rsi, stochRSI, macdHist, obv, volumeSpike, liquidityHunt, _, _, _, _ := CalculateIndicators(primaryBars, cfg) // Discard unused
	isConfirmed, confirmationReason := CheckMultiIndicatorConfirmation(
		rsi, stochRSI, macdHist, obv, volumeSpike, liquidityHunt, primaryBars, cfg.Indicators,
	)

	if !isConfirmed {
		s.logger.LogInfo("GenerateSignals [%s]: Final confirmation FAILED. Reason: %s", data.AssetPair, confirmationReason)
		return nil, nil
	}
	s.logger.LogInfo("GenerateSignals [%s]: Final confirmation PASSED. Reason: %s. Generating signal.", data.AssetPair, confirmationReason)

	var signals []StrategySignal
	recommendedPrice := FindBestLimitPrice(data.BrokerOrderBook, currentPrice-atr, 1.0)
	// For a standard buy, we still calculate the size here based on portfolio risk.
	calculatedSize := AdjustPositionSize(data.PortfolioValue, atr, cfg.Trading.PortfolioRiskPerTrade)
	stopLossPrice := VolatilityAdjustedOrderPrice(recommendedPrice, atr, 2.0, true)
	orderBookConfidence := AnalyzeOrderBookDepth(data.BrokerOrderBook, 1.0)

	s.logger.LogInfo("GenerateSignals [%s]: BUY signal triggered. Price: %.2f, Size: %.4f, SL: %.2f, OB-Conf: %.2f",
		data.AssetPair, recommendedPrice, calculatedSize, stopLossPrice, orderBookConfidence)

	signals = append(signals, StrategySignal{
		AssetPair:        data.AssetPair,
		Direction:        "buy",
		Confidence:       (1.0 + orderBookConfidence) / 2.0,
		Reason:           fmt.Sprintf("%s | %s", reason, confirmationReason),
		GeneratedAt:      time.Now(),
		FearGreedIndex:   data.FearGreedIndex.Value,
		RecommendedPrice: recommendedPrice,
		CalculatedSize:   calculatedSize,
		StopLossPrice:    stopLossPrice,
	})

	return signals, nil
}

// GenerateExitSignal checks for liquidity hunts or bearish confluence to generate an exit signal.
func (s *strategyImpl) GenerateExitSignal(ctx context.Context, data ConsolidatedMarketPicture, cfg utilities.AppConfig) (StrategySignal, bool) {
	_ = ctx // Acknowledge context is not used in this specific function body

	// This initial check is correct and remains.
	if len(data.ProvidersData) == 0 {
		return StrategySignal{}, false
	}

	// This check for sufficient bar data is also correct and remains.
	primaryBars, ok := data.PrimaryOHLCVByTF[cfg.Consensus.MultiTimeframe.BaseTimeframe]
	if !ok || len(primaryBars) < 2 {
		return StrategySignal{}, false
	}

	// [FIX] Call the updated CalculateIndicators function to get all signals at once.
	// We use '_' to ignore the indicator values we don't need for this specific exit logic.
	// The important variable is the final boolean we named 'bearishHuntReversal'.
	_, _, _, _, _, bearishHuntReversal, _, _, _, _ := CalculateIndicators(primaryBars, cfg)

	// [FIX] Use the new 'bearishHuntReversal' boolean for the check.
	if bearishHuntReversal {
		// [FIX] Updated the log message for clarity.
		s.logger.LogWarn("GenerateExitSignal [%s]: EXIT triggered. Reason: Confirmed Bearish Reversal after Liquidity Hunt", data.AssetPair)
		return StrategySignal{
			AssetPair: data.AssetPair,
			Direction: "sell",
			// The reason is also updated to be more descriptive.
			Reason: "Confirmed Bearish Reversal after Liquidity Hunt",
		}, true
	}

	// This existing check for bearish confluence is a great fallback and remains untouched.
	isBearish, reason := checkBearishConfluence(primaryBars, cfg)
	if isBearish {
		s.logger.LogWarn("GenerateExitSignal [%s]: EXIT triggered. Reason: %s", data.AssetPair, reason)
		return StrategySignal{
			AssetPair: data.AssetPair,
			Direction: "sell",
			Reason:    reason,
		}, true
	}

	return StrategySignal{}, false
}

func isHammerCandle(bar utilities.OHLCVBar) bool {
	body := math.Abs(bar.Close - bar.Open)
	lowerShadow := math.Min(bar.Open, bar.Close) - bar.Low
	return lowerShadow > 2*body && (bar.High-math.Max(bar.Open, bar.Close)) < body // Long lower shadow for dip reversal
}

// checkBearishConfluence checks for a confluence of bearish signals for an exit.
func checkBearishConfluence(bars []utilities.OHLCVBar, cfg utilities.AppConfig) (bool, string) {
	if len(bars) < cfg.Indicators.MACDSlowPeriod {
		return false, "Insufficient data for bearish confluence"
	}

	rsi, stochRSI, macdHist, _, _, _, upperBB, _, stochK, stochD := CalculateIndicators(bars, cfg) // Discard unused

	isMacdBearish := macdHist < 0
	isRsiOverbought := rsi > 70
	isStochRsiOverbought := stochRSI > cfg.Indicators.StochRSISellThreshold

	// New: Bollinger Bands check (e.g., price touching upper band for overbought)
	currentClose := bars[len(bars)-1].Close
	isBollingerOverbought := currentClose >= upperBB

	// New: Stochastic check (overbought >80 and bearish crossover: K crosses below D)
	isStochOverbought := stochK > 80 && stochK < stochD // Bearish crossover

	score := 0
	var reasons []string
	if isMacdBearish {
		score++
		reasons = append(reasons, "MACD<0")
	}
	if isRsiOverbought {
		score++
		reasons = append(reasons, "RSI>70")
	}
	if isStochRsiOverbought {
		score++
		reasons = append(reasons, "StochRSI>80")
	}
	if isBollingerOverbought {
		score++
		reasons = append(reasons, "Price >= Upper Bollinger")
	}
	if isStochOverbought {
		score++
		reasons = append(reasons, "Stochastic >80 with bearish crossover")
	}

	if score >= 3 { // Require at least 3 for confluence
		return true, fmt.Sprintf("Bearish Confluence: %s", strings.Join(reasons, " & "))
	}

	return false, ""
}
