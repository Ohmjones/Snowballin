package strategy

import (
	"Snowballin/utilities"
	"context"
	"fmt"
	"sort"
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
	// --- Filter 1: Daily Trend (EMA) ---
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
		return false, "Hold: Insufficient daily data from providers for trend analysis."
	}
	if consensusLastClose < consensusEMA50 {
		return false, fmt.Sprintf("Hold: Consensus Price (%.2f) below Consensus Daily EMA(50) (%.2f)", consensusLastClose, consensusEMA50)
	}
	s.logger.LogInfo("MTF Consensus [1/3 PASSED]: Daily trend is bullish (Consensus Price > Consensus EMA50).")

	// --- Filter 2: 4-Hour Momentum (MACD) ---
	consensusMacdHist, macdOk := getConsensusIndicatorValue(data, "4h", cfg.Indicators.MACDSlowPeriod, func(bars []utilities.OHLCVBar) (float64, bool) {
		closes := extractCloses(bars)
		_, _, macdHist := CalculateMACD(closes, cfg.Indicators.MACDFastPeriod, cfg.Indicators.MACDSlowPeriod, cfg.Indicators.MACDSignalPeriod)
		return macdHist, true
	})

	if !macdOk {
		return false, "Hold: Insufficient 4H data from providers for momentum analysis."
	}
	if consensusMacdHist <= 0 {
		return false, fmt.Sprintf("Hold: 4H Consensus MACD Hist (%.4f) not positive", consensusMacdHist)
	}
	s.logger.LogInfo("MTF Consensus [2/3 PASSED]: 4H momentum is bullish (Consensus MACD Hist > 0).")

	// --- Filter 3: 1-Hour Entry Trigger (RSI) ---
	consensusRSI, rsiOk := getConsensusIndicatorValue(data, "1h", cfg.Indicators.RSIPeriod, func(bars []utilities.OHLCVBar) (float64, bool) {
		rsi := CalculateRSI(bars, cfg.Indicators.RSIPeriod)
		return rsi, true
	})

	if !rsiOk {
		return false, "Hold: Insufficient 1H data from providers for entry trigger."
	}
	if consensusRSI > 45 {
		return false, fmt.Sprintf("Hold: 1H Consensus RSI (%.2f) not low enough for entry", consensusRSI)
	}
	s.logger.LogInfo("MTF Consensus [3/3 PASSED]: 1H Consensus RSI is low (%.2f), indicating a dip.", consensusRSI)

	finalReason := fmt.Sprintf("Daily Trend OK | 4H MACD Bullish | 1H RSI Low (%.2f)", consensusRSI)
	return true, finalReason
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
		s.logger.LogWarn("GenerateSignals [%s]: Not enough primary bars for analysis.", data.AssetPair)
		return nil, nil
	}
	atr, err := CalculateATR(primaryBars, cfg.Indicators.ATRPeriod)
	if err != nil {
		s.logger.LogError("GenerateSignals [%s]: Could not calculate ATR: %v", data.AssetPair, err)
		return nil, err
	}

	// Perform Multi-Timeframe Consensus check
	isBuy, reason := s.checkMultiTimeframeConsensus(data, cfg)

	// --- PREDICTIVE BUY LOGIC (RESTORED) ---
	// This block checks for strong order book support to place small orders,
	// even if the main multi-timeframe consensus check fails.
	bookAnalysis := PerformOrderBookAnalysis(data.BrokerOrderBook, 1.0, 10, 1.5)
	if !isBuy && bookAnalysis.DepthScore > cfg.Trading.MinBookConfidenceForPredictive {
		s.logger.LogInfo("GenerateSignals [%s]: Consensus failed, but strong book (%f). Placing predictive buy.", data.AssetPair, bookAnalysis.DepthScore)
		if len(bookAnalysis.SupportLevels) == 0 {
			return nil, nil
		}
		sort.Slice(bookAnalysis.SupportLevels, func(i, j int) bool {
			return bookAnalysis.SupportLevels[i].PriceLevel > bookAnalysis.SupportLevels[j].PriceLevel
		})

		signals := []StrategySignal{}
		baseSize := cfg.Trading.BaseOrderSize * cfg.Trading.PredictiveOrderSizePercent
		topLevels := bookAnalysis.SupportLevels[:utilities.MinInt(3, len(bookAnalysis.SupportLevels))]

		for i, level := range topLevels {
			signals = append(signals, StrategySignal{
				AssetPair:        data.AssetPair,
				Direction:        "predictive_buy",
				Confidence:       (1.0 + bookAnalysis.DepthScore) / 2.0,
				Reason:           fmt.Sprintf("Predictive: Strong book support at %.2f", level.PriceLevel),
				GeneratedAt:      time.Now(),
				FearGreedIndex:   data.FearGreedIndex.Value,
				RecommendedPrice: level.PriceLevel,
				CalculatedSize:   baseSize / float64(i+1),
				StopLossPrice:    level.PriceLevel - atr*2.0,
			})
		}
		return signals, nil
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

	rsi, stochRSI, macdHist, obv, volumeSpike, liquidityHunt := CalculateIndicators(primaryBars, cfg)
	isConfirmed, confirmationReason := CheckMultiIndicatorConfirmation(
		rsi, stochRSI, macdHist, obv, volumeSpike, liquidityHunt, primaryBars, cfg.Indicators,
	)

	if !isConfirmed {
		s.logger.LogInfo("GenerateSignals [%s]: Final confirmation FAILED. Reason: %s", data.AssetPair, confirmationReason)
		return nil, nil
	}
	s.logger.LogInfo("GenerateSignals [%s]: Final confirmation PASSED. Reason: %s. Generating signal.", data.AssetPair, confirmationReason)

	var signals []StrategySignal
	vpvr := CalculateVPVR(primaryBars, 20)
	vpvrLevels := []float64{}
	if len(vpvr) > 0 {
		for _, entry := range vpvr {
			vpvrLevels = append(vpvrLevels, entry.PriceLevel)
		}
		sort.Float64s(vpvrLevels)
	}

	currentPrice := data.ProvidersData[0].CurrentPrice
	recommendedPrice := FindBestLimitPrice(data.BrokerOrderBook, currentPrice-atr, 1.0)
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
	_ = ctx

	if len(data.ProvidersData) == 0 {
		return StrategySignal{}, false
	}

	primaryBars, ok := data.PrimaryOHLCVByTF[cfg.Consensus.MultiTimeframe.BaseTimeframe]
	if !ok || len(primaryBars) < 2 {
		return StrategySignal{}, false
	}

	huntDetected := IsLiquidityHuntDetected(
		primaryBars,
		cfg.Indicators.LiquidityHunt.MinWickPercent,
		cfg.Indicators.LiquidityHunt.VolSpikeMultiplier,
		cfg.Indicators.LiquidityHunt.VolMAPeriod,
	)

	if huntDetected {
		s.logger.LogWarn("GenerateExitSignal [%s]: EXIT triggered. Reason: Bearish Liquidity Hunt Pattern Detected", data.AssetPair)
		return StrategySignal{
			AssetPair: data.AssetPair,
			Direction: "sell",
			Reason:    "Liquidity Hunt Pattern Detected",
		}, true
	}

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

// checkBearishConfluence checks for a confluence of bearish signals for an exit.
func checkBearishConfluence(bars []utilities.OHLCVBar, cfg utilities.AppConfig) (bool, string) {
	if len(bars) < cfg.Indicators.MACDSlowPeriod {
		return false, "Insufficient data for bearish confluence"
	}

	rsi, stochRSI, macdHist, _, _, _ := CalculateIndicators(bars, cfg)

	isMacdBearish := macdHist < 0
	isRsiOverbought := rsi > 70
	isStochRsiOverbought := stochRSI > cfg.Indicators.StochRSISellThreshold

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

	if score >= 2 {
		return true, fmt.Sprintf("Bearish Confluence: %s", strings.Join(reasons, " & "))
	}

	return false, ""
}
