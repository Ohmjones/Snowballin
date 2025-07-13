package strategy

import (
	"Snowballin/utilities"
	"context"
	"fmt"
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

// checkMultiTimeframeConsensus applies a hierarchical filter for a high-probability entry.
func (s *strategyImpl) checkMultiTimeframeConsensus(data ConsolidatedMarketPicture, cfg utilities.AppConfig) (bool, string) {
	// --- Filter 1: Daily Trend (EMA) ---
	dailyBars, ok := data.PrimaryOHLCVByTF["1d"]
	if !ok || len(dailyBars) < 50 {
		return false, "Hold: Insufficient daily data for trend analysis."
	}
	dailyCloses := extractCloses(dailyBars)
	ema50, _ := ComputeEMASeries(dailyCloses, 50)
	if len(ema50) == 0 {
		return false, "Hold: Could not compute Daily EMA."
	}
	lastClose := dailyCloses[len(dailyCloses)-1]
	lastEMA50 := ema50[len(ema50)-1]

	if lastClose < lastEMA50 {
		return false, fmt.Sprintf("Hold: Price (%.2f) below Daily EMA(50) (%.2f)", lastClose, lastEMA50)
	}
	s.logger.LogInfo("MTF Consensus [1/3 PASSED]: Daily trend is bullish (Price > EMA50).")

	// --- Filter 2: 4-Hour Momentum (MACD) ---
	fourHourBars, ok := data.PrimaryOHLCVByTF["4h"]
	if !ok || len(fourHourBars) < cfg.Indicators.MACDSlowPeriod {
		return false, "Hold: Insufficient 4H data for momentum analysis."
	}
	fourHourCloses := extractCloses(fourHourBars)
	_, _, macdHist := CalculateMACD(fourHourCloses, cfg.Indicators.MACDFastPeriod, cfg.Indicators.MACDSlowPeriod, cfg.Indicators.MACDSignalPeriod)

	if macdHist <= 0 {
		return false, fmt.Sprintf("Hold: 4H MACD Hist (%.4f) not positive", macdHist)
	}
	s.logger.LogInfo("MTF Consensus [2/3 PASSED]: 4H momentum is bullish (MACD Hist > 0).")

	// --- Filter 3: 1-Hour Entry Trigger (RSI) ---
	oneHourBars, ok := data.PrimaryOHLCVByTF["1h"]
	if !ok || len(oneHourBars) < cfg.Indicators.RSIPeriod {
		return false, "Hold: Insufficient 1H data for entry trigger."
	}
	rsi := CalculateRSI(oneHourBars, cfg.Indicators.RSIPeriod)
	if rsi > 45 {
		return false, fmt.Sprintf("Hold: 1H RSI (%.2f) not low enough for entry", rsi)
	}
	s.logger.LogInfo("MTF Consensus [3/3 PASSED]: 1H RSI is low (%.2f), indicating a dip.", rsi)

	finalReason := fmt.Sprintf("Daily Trend OK | 4H MACD Bullish | 1H RSI Low (%.2f)", rsi)
	return true, finalReason
}

// GenerateSignals produces intelligent entry signals with calculated price, size, and stop-loss.
func (s *strategyImpl) GenerateSignals(ctx context.Context, data ConsolidatedMarketPicture, cfg utilities.AppConfig) ([]StrategySignal, error) {
	_ = ctx // Acknowledge unused parameter

	if len(data.ProvidersData) == 0 || data.ProvidersData[0].CurrentPrice == 0 {
		return nil, fmt.Errorf("cannot generate signals without primary provider data")
	}

	if !cfg.Consensus.UseMultiTimeframeConsensus {
		s.logger.LogWarn("GenerateSignals: Multi-Timeframe Consensus is disabled. No entry signals will be generated.")
		return nil, nil
	}

	isBuy, reason := s.checkMultiTimeframeConsensus(data, cfg)
	if !isBuy {
		s.logger.LogInfo("GenerateSignals: %s -> %s - Reason: %s",
			utilities.ColorYellow+data.AssetPair+utilities.ColorReset,
			utilities.ColorWhite+"HOLD"+utilities.ColorReset,
			strings.TrimPrefix(reason, "Hold: "), // Removes the redundant "Hold: " from the reason string.
		)
		return nil, nil
	}
	s.logger.LogInfo("GenerateSignals [%s]: MTF Consensus PASSED. Reason: %s. Performing final confirmation...", data.AssetPair, reason)

	primaryBars := data.PrimaryOHLCVByTF[cfg.Consensus.MultiTimeframe.BaseTimeframe]
	if len(primaryBars) < cfg.Indicators.MACDSlowPeriod { // Use a long enough lookback for all indicators
		s.logger.LogWarn("GenerateSignals [%s]: Not enough primary bars for final analysis.", data.AssetPair)
		return nil, nil
	}

	// --- [ENHANCEMENT] Use CheckMultiIndicatorConfirmation for final validation ---
	rsi, stochRSI, macdHist, obv, volumeSpike, liquidityHunt := CalculateIndicators(primaryBars, cfg)

	isConfirmed, confirmationReason := CheckMultiIndicatorConfirmation(
		rsi, stochRSI, macdHist, obv, volumeSpike, liquidityHunt, primaryBars, cfg.Indicators,
	)

	if !isConfirmed {
		s.logger.LogInfo("GenerateSignals [%s]: Final confirmation FAILED. Reason: %s", data.AssetPair, confirmationReason)
		return nil, nil
	}
	s.logger.LogInfo("GenerateSignals [%s]: Final confirmation PASSED. Reason: %s. Generating signal.", data.AssetPair, confirmationReason)
	// --- End of Enhancement ---

	var signals []StrategySignal

	// 1. Calculate Volatility (ATR) for position sizing and stop-loss
	atr, err := CalculateATR(primaryBars, cfg.Indicators.ATRPeriod)
	if err != nil {
		s.logger.LogError("GenerateSignals [%s]: Could not calculate ATR: %v", data.AssetPair, err)
		return nil, err
	}

	// 2. Use VPVR to find a strategic entry price (Point of Control)
	vpvr := CalculateVPVR(primaryBars, 20)
	recommendedPrice := data.ProvidersData[0].CurrentPrice
	if len(vpvr) > 0 {
		for _, entry := range vpvr {
			if entry.PriceLevel < recommendedPrice {
				recommendedPrice = entry.PriceLevel
				s.logger.LogInfo("GenerateSignals [%s]: Found VPVR support at %.2f", data.AssetPair, recommendedPrice)
				break
			}
		}
	}

	// 3. Use ATR to calculate a dynamic position size and stop-loss
	calculatedSize := AdjustPositionSize(data.PortfolioValue, atr, cfg.Trading.MaxPortfolioDrawdown)
	stopLossPrice := VolatilityAdjustedOrderPrice(recommendedPrice, atr, 2.0, true) // 2x ATR stop-loss

	// 4. Analyze Order Book for final confidence score
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
	_ = ctx // Acknowledge unused parameter

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
