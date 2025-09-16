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
	var isTrendBullish bool
	var trendReason string

	consensusLastClose, priceOk := getConsensusIndicatorValue(data, "1d", 50, func(bars []utilities.OHLCVBar) (float64, bool) {
		if len(bars) > 0 {
			return bars[len(bars)-1].Close, true
		}
		return 0, false
	})
	consensusEMA50, emaOk := getConsensusIndicatorValue(data, "1d", 50, func(bars []utilities.OHLCVBar) (float64, bool) {
		closes := ExtractCloses(bars)
		ema, _ := ComputeEMASeries(closes, 50)
		if len(ema) > 0 {
			return ema[len(ema)-1], true
		}
		return 0, false
	})

	if !priceOk || !emaOk {
		return false, "Hold: Insufficient daily data for trend analysis."
	}

	if consensusLastClose >= consensusEMA50 {
		isTrendBullish = true
		trendReason = "Daily trend is bullish."
		s.logger.LogInfo("MTF Consensus [GATE 1 PASSED]: %s", trendReason)
	} else {
		// --- THE FEAR OVERRIDE LOGIC ---
		if cfg.Trading.UseFearOverride && data.FearGreedIndex.Value <= cfg.Trading.FearOverrideThreshold {
			isTrendBullish = true // Override the failed trend check.
			trendReason = fmt.Sprintf("FEAR OVERRIDE ACTIVE (F&G: %d)", data.FearGreedIndex.Value)
			s.logger.LogWarn("MTF Consensus [GATE 1 OVERRIDDEN]: %s", trendReason)
		} else {
			// If override is disabled or F&G is not low enough, fail the check.
			return false, fmt.Sprintf("Hold: Daily trend is bearish. Price (%.2f) is below Daily EMA(50) (%.2f)", consensusLastClose, consensusEMA50)
		}
	}

	// If Gate 1 (or its override) did not pass, we can't proceed.
	if !isTrendBullish {
		return false, "Hold: Daily trend is bearish and Fear Override conditions not met."
	}

	// --- Gate 2: 4-Hour Momentum (MACD) ---
	var isMomentumShifting bool
	var momentumReason string
	consensusMacdHistSeries, macdOk := getConsensusIndicatorValue(data, "4h", cfg.Indicators.MACDSlowPeriod+cfg.Indicators.MACDSignalPeriod, func(bars []utilities.OHLCVBar) (float64, bool) {
		closes := ExtractCloses(bars)
		_, _, macdHist := CalculateMACDSeries(closes, cfg.Indicators.MACDFastPeriod, cfg.Indicators.MACDSlowPeriod, cfg.Indicators.MACDSignalPeriod)
		if len(macdHist) < 2 {
			return 0, false
		}
		if macdHist[len(macdHist)-1] > macdHist[len(macdHist)-2] {
			return 1.0, true
		}
		return -1.0, true
	})
	if macdOk && consensusMacdHistSeries > 0 {
		isMomentumShifting = true
		momentumReason = "4H Momentum Shift"
	}

	// --- Gate 3: 1-Hour Entry Trigger (RSI) ---
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
		// Prepend the trend reason (which could be the override reason)
		return true, fmt.Sprintf("%s | %s", trendReason, finalReason)
	}

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

	// Use the primary broker's data for execution-critical calculations like ATR.
	primaryBars, ok := data.PrimaryOHLCVByTF[cfg.Consensus.MultiTimeframe.BaseTimeframe]
	if !ok || len(primaryBars) < cfg.Indicators.ATRPeriod+1 {
		s.logger.LogWarn("GenerateSignals [%s]: Insufficient primary (broker) bars for signal generation.", data.AssetPair)
		return nil, nil // Not an error, just not enough data to proceed.
	}

	// --- 1. Perform Multi-Timeframe Consensus check ---
	isBuy, reason := s.checkMultiTimeframeConsensus(data, cfg)
	currentPrice := data.ProvidersData[0].CurrentPrice // Use broker's current price

	// --- 2. Predictive Buy Logic (Snipe Opportunity) ---
	// This logic runs independently of the main consensus check.
	bookAnalysis := PerformOrderBookAnalysis(data.BrokerOrderBook, 1.0, 10, 1.5)
	if !isBuy && bookAnalysis.DepthScore > cfg.Trading.MinBookConfidenceForPredictive {
		s.logger.LogInfo("GenerateSignals [%s]: MTF Consensus failed, but strong book (%.2f). Checking for predictive snipe...", data.AssetPair, bookAnalysis.DepthScore)
		atr, err := CalculateATR(primaryBars, cfg.Indicators.ATRPeriod)
		if err != nil || atr == 0 {
			s.logger.LogError("GenerateSignals [%s]: Could not calculate ATR for predictive buy: %v", data.AssetPair, err)
		} else {
			targetSnipePrice := currentPrice - (atr * cfg.Trading.PredictiveAtrMultiplier)
			snipePrice, foundSupport := FindSupportNearPrice(data.BrokerOrderBook, targetSnipePrice, cfg.Trading.PredictiveSearchWindowPercent)
			if foundSupport {
				s.logger.LogWarn("GenerateSignals [%s]: PREDICTIVE BUY SIGNAL TRIGGERED!", data.AssetPair)
				predictiveSignal := StrategySignal{
					Direction:        "predictive_buy",
					Reason:           fmt.Sprintf("Predictive Snipe: Strong book (Conf: %.2f) at volatility-defined support. Target: %.2f", bookAnalysis.DepthScore, snipePrice),
					RecommendedPrice: snipePrice,
					CalculatedSize:   cfg.Trading.BaseOrderSize / snipePrice, // Use base size for predictive entries
				}
				return []StrategySignal{predictiveSignal}, nil
			}
		}
	}

	// --- 3. Main Buy Signal Logic (Requires MTF Consensus) ---
	if !isBuy {
		s.logger.LogInfo("GenerateSignals: %s -> %s - Reason: %s",
			utilities.ColorYellow+data.AssetPair+utilities.ColorReset,
			utilities.ColorWhite+"HOLD"+utilities.ColorReset,
			strings.TrimPrefix(reason, "Hold: "),
		)
		return nil, nil
	}

	s.logger.LogInfo("GenerateSignals [%s]: MTF Consensus PASSED. Reason: %s. Performing final checks...", data.AssetPair, reason)

	// Final Confirmation with VWAP integration
	rsi, stochRSI, macdHist, obv, volumeSpike, liquidityHunt, _, _, _, _, vwap := CalculateIndicators(primaryBars, cfg) // Updated to include vwap
	// VWAP Check: Only proceed if current price is below VWAP, indicating undervaluation
	if currentPrice >= vwap {
		s.logger.LogInfo("GenerateSignals [%s]: VWAP Check FAILED. Current Price %.2f >= VWAP %.2f", data.AssetPair, currentPrice, vwap)
		return nil, nil
	}
	s.logger.LogInfo("GenerateSignals [%s]: VWAP Check PASSED. Current Price %.2f < VWAP %.2f", data.AssetPair, currentPrice, vwap)

	isConfirmed, confirmationReason := CheckMultiIndicatorConfirmation(rsi, stochRSI, macdHist, obv, volumeSpike, liquidityHunt, primaryBars, cfg.Indicators)
	if !isConfirmed {
		s.logger.LogInfo("GenerateSignals [%s]: Final confirmation FAILED. Reason: %s", data.AssetPair, confirmationReason)
		return nil, nil
	}

	// --- 4. Integrate All Sentiment Layers ---
	// a) Asset-specific trending consensus
	baseAssetSymbol := strings.Split(data.AssetPair, "/")[0]
	sentiment := CalculateSentimentConsensus(baseAssetSymbol, data.CgTrendingData, data.CmcTrendingData, s.logger)

	// b) Build the comprehensive reason string for the signal
	finalReason := fmt.Sprintf("%s | %s | VWAP Undervaluation (Price %.2f < VWAP %.2f)", reason, confirmationReason, currentPrice, vwap)
	if sentiment.IsTrending {
		finalReason += fmt.Sprintf(" | Trending Score: %.2f (%s)", sentiment.AttentionScore, strings.Join(sentiment.TrendingOn, ", "))
	}
	if data.MarketSentiment.State != "Neutral" {
		finalReason += " | Market Mood: " + data.MarketSentiment.State
	}

	// --- 5. Calculate Order Parameters & Apply Sentiment Modulation ---
	atr, _ := CalculateATR(primaryBars, cfg.Indicators.ATRPeriod) // We know this works from the check above
	recommendedPrice := FindBestLimitPrice(data.BrokerOrderBook, currentPrice-atr, 1.0)
	calculatedSize := cfg.Trading.BaseOrderSize / recommendedPrice

	// New: Compounding Reinvestment - Scale size based on portfolio growth
	profitFactor := data.PortfolioValue / data.PeakPortfolioValue // >1 if profits
	if profitFactor > 1.0 {
		originalSize := calculatedSize
		calculatedSize *= profitFactor // Reinvest proportional to growth
		s.logger.LogInfo("COMPOUNDING: Scaling size by %.2f (from %.4f to %.4f) due to profits", profitFactor, originalSize, calculatedSize)
	}

	// **MODULATION**: Increase buy size if market mood is bullish
	if data.MarketSentiment.State == "Bullish" {
		originalSize := calculatedSize
		// Use the multiplier from the sentiment-modulated config
		calculatedSize *= cfg.Trading.ConsensusBuyMultiplier
		s.logger.LogInfo("BULLISH SENTIMENT OVERLAY: Increasing buy size by x%.2f from %.4f to %.4f",
			cfg.Trading.ConsensusBuyMultiplier, originalSize, calculatedSize)
	}

	stopLossPrice := VolatilityAdjustedOrderPrice(recommendedPrice, atr, 2.0, true)
	orderBookConfidence := AnalyzeOrderBookDepth(data.BrokerOrderBook, 1.0)

	s.logger.LogInfo("GenerateSignals [%s]: BUY signal triggered. Price: %.2f, Size: %.4f, SL: %.2f, OB-Conf: %.2f",
		data.AssetPair, recommendedPrice, calculatedSize, stopLossPrice, orderBookConfidence)

	// --- 6. Return the Final Signal ---
	signals := []StrategySignal{
		{
			AssetPair:        data.AssetPair,
			Direction:        "buy",
			Confidence:       (1.0 + orderBookConfidence) / 2.0,
			Reason:           finalReason,
			GeneratedAt:      time.Now(),
			FearGreedIndex:   data.FearGreedIndex.Value,
			RecommendedPrice: recommendedPrice,
			CalculatedSize:   calculatedSize,
			StopLossPrice:    stopLossPrice,
		},
	}

	return signals, nil
}

// GenerateExitSignal checks for high-probability technical exit conditions.
// Note: The sentiment-based exit logic (tightening a trailing stop) is applied
// in the `manageOpenPosition` function in `app.go`, as it modifies an existing state
// rather than generating a new, independent "sell" signal.
func (s *strategyImpl) GenerateExitSignal(ctx context.Context, data ConsolidatedMarketPicture, cfg utilities.AppConfig) (StrategySignal, bool) {
	_ = ctx // Acknowledge unused parameter

	if len(data.ProvidersData) == 0 {
		return StrategySignal{}, false
	}

	// Use the primary broker's data for exit signals to match the execution environment.
	primaryBars, ok := data.PrimaryOHLCVByTF[cfg.Consensus.MultiTimeframe.BaseTimeframe]
	if !ok || len(primaryBars) < 2 {
		return StrategySignal{}, false
	}

	// Check for a confirmed bearish reversal after a liquidity hunt (a strong exit signal).
	_, _, _, _, _, bearishHuntReversal, _, _, _, _, _ := CalculateIndicators(primaryBars, cfg)

	if bearishHuntReversal {
		s.logger.LogWarn("GenerateExitSignal [%s]: EXIT triggered. Reason: Confirmed Bearish Reversal after Liquidity Hunt", data.AssetPair)
		return StrategySignal{
			AssetPair: data.AssetPair,
			Direction: "sell",
			Reason:    "Confirmed Bearish Reversal after Liquidity Hunt",
		}, true
	}

	// As a fallback, check for a broader confluence of multiple bearish indicators.
	isBearish, reason := CheckBearishConfluence(primaryBars, cfg)
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

func CheckBearishConfluence(bars []utilities.OHLCVBar, cfg utilities.AppConfig) (bool, string) {
	if len(bars) < cfg.Indicators.MACDSlowPeriod {
		return false, "Insufficient data for bearish confluence"
	}

	rsi, stochRSI, macdHist, obv, _, _, upperBB, _, stochK, stochD, _ := CalculateIndicators(bars, cfg) // Include obv

	isMacdBearish := macdHist < 0
	isRsiOverbought := rsi > 70
	isStochRsiOverbought := stochRSI > cfg.Indicators.StochRSISellThreshold
	currentClose := bars[len(bars)-1].Close
	isBollingerOverbought := currentClose >= upperBB
	isStochOverbought := stochK > 80 && stochK < stochD
	isSMACrossoverBearish := false
	var smaReason string
	if len(cfg.Trading.SMAPeriodsForExit) == 2 {
		shortPeriod := cfg.Trading.SMAPeriodsForExit[0]
		longPeriod := cfg.Trading.SMAPeriodsForExit[1]
		if len(bars) >= longPeriod {
			closePrices := ExtractCloses(bars)
			shortSMA := CalculateSMA(closePrices, shortPeriod)
			longSMA := CalculateSMA(closePrices, longPeriod)
			if shortSMA < longSMA {
				isSMACrossoverBearish = true
				smaReason = fmt.Sprintf("Death Cross: SMA(%d) is below SMA(%d)", shortPeriod, longPeriod)
			}
		}
	}

	// New: Check OBV for bearish divergence
	prevObv := CalculateOBV(bars[:len(bars)-1]) // OBV without last bar
	isObvBearishDivergence := obv < prevObv && bars[len(bars)-1].Close > bars[len(bars)-2].Close

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
	if isSMACrossoverBearish {
		score++
		reasons = append(reasons, smaReason)
	}
	if isObvBearishDivergence {
		score++
		reasons = append(reasons, "OBV Bearish Divergence")
	}

	if score >= 4 {
		return true, fmt.Sprintf("Bearish Confluence: %s", strings.Join(reasons, " & "))
	}

	return false, ""
}
