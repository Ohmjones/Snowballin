package strategy

import (
	"Snowballin/utilities"
	"context"
	"fmt"
	"strings"
	"time"
)

// NewStrategy constructs a new strategy instance.
// It returns the 'Strategy' interface, which is a Go best practice.
func NewStrategy(logger *utilities.Logger) Strategy {
	return &strategyImpl{logger: logger}
}

// --- MODIFIED: Replaced entire function with hierarchical filter logic ---
func (s *strategyImpl) checkMultiTimeframeConsensus(data ConsolidatedMarketPicture, cfg utilities.AppConfig) (bool, string) {
	// The research blueprint requires a hierarchical filter for a high-probability entry
	// 1. Long-Term Trend Filter on Daily chart
	// 2. Medium-Term Momentum Confirmation on 4-Hour chart
	// 3. Short-Term Entry Trigger on 1-Hour chart

	// --- Filter 1: Daily Trend (EMA) ---
	dailyBars, ok := data.PrimaryOHLCVByTF["1d"]
	if !ok || len(dailyBars) < cfg.Indicators.MACDSlowPeriod { // Use a reasonable lookback check
		return false, "Hold: Insufficient daily data for trend analysis."
	}
	dailyCloses := make([]float64, len(dailyBars))
	for i, bar := range dailyBars {
		dailyCloses[i] = bar.Close
	}
	// Using 50-period EMA as per research [cite: 1131]
	ema50, _ := ComputeEMASeries(dailyCloses, 50)
	if len(ema50) == 0 {
		return false, "Hold: Could not compute Daily EMA."
	}
	lastClose := dailyCloses[len(dailyCloses)-1]
	lastEMA50 := ema50[len(ema50)-1]

	if lastClose < lastEMA50 {
		return false, fmt.Sprintf("Hold: Price (%.2f) is below Daily EMA(50) (%.2f). Long-term trend is not bullish.", lastClose, lastEMA50)
	}
	s.logger.LogInfo("MTF Consensus [1/3 PASSED]: Daily trend is bullish (Price > EMA50).")

	// --- Filter 2: 4-Hour Momentum (MACD) ---
	fourHourBars, ok := data.PrimaryOHLCVByTF["4h"]
	if !ok || len(fourHourBars) < cfg.Indicators.MACDSlowPeriod {
		return false, "Hold: Insufficient 4H data for momentum analysis."
	}
	// MACD Crossover check as per research [cite: 1127, 1131]
	macdHist := CalculateMACD(fourHourBars, cfg.Indicators.MACDFastPeriod, cfg.Indicators.MACDSlowPeriod, cfg.Indicators.MACDSignalPeriod)
	if macdHist <= 0 {
		return false, fmt.Sprintf("Hold: 4H MACD Histogram (%.4f) is not positive. Medium-term momentum is not bullish.", macdHist)
	}
	s.logger.LogInfo("MTF Consensus [2/3 PASSED]: 4H momentum is bullish (MACD Hist > 0).")

	// --- Filter 3: 1-Hour Entry Trigger (RSI) ---
	oneHourBars, ok := data.PrimaryOHLCVByTF["1h"]
	if !ok || len(oneHourBars) < cfg.Indicators.RSIPeriod {
		return false, "Hold: Insufficient 1H data for entry trigger."
	}
	// RSI crossover from oversold territory check as per research [cite: 1129, 1131]
	// For simplicity in a single-pass check, we'll just check if RSI is low, indicating a dip.
	// A true crossover check would require storing the previous state.
	rsi := CalculateRSI(oneHourBars, cfg.Indicators.RSIPeriod)
	// We are looking to buy a dip, so a low RSI is a good sign. The research suggests crossing UP from 30.
	// A practical implementation is to check if RSI is below a slightly higher threshold, like 40,
	// to catch the start of the recovery.
	if rsi > 40 {
		return false, fmt.Sprintf("Hold: 1H RSI (%.2f) is not low enough to indicate a good entry dip.", rsi)
	}
	s.logger.LogInfo("MTF Consensus [3/3 PASSED]: 1H RSI is low (%.2f), indicating a dip.", rsi)

	// --- All filters passed ---
	finalReason := fmt.Sprintf("Daily Trend OK (Price > EMA50) | 4H MACD Bullish | 1H RSI Low (%.2f)", rsi)
	return true, finalReason
}

// GenerateSignals is simplified to only produce initial entry signals based on the MTF consensus.
func (s *strategyImpl) GenerateSignals(ctx context.Context, data ConsolidatedMarketPicture, cfg utilities.AppConfig) ([]StrategySignal, error) {
	_ = ctx // Acknowledge unused parameter to fix warning.

	// --- Sanity Checks ---
	if len(data.ProvidersData) == 0 {
		s.logger.LogWarn("GenerateSignals: No provider data available, cannot generate signals.")
		return nil, nil
	}
	primaryData := data.ProvidersData[0]
	if primaryData.CurrentPrice == 0 {
		return nil, fmt.Errorf("cannot generate signals without a current price from primary provider")
	}

	// --- Generate BUY Signal (Entry Mechanism) ---
	var signals []StrategySignal

	if cfg.Consensus.UseMultiTimeframeConsensus {
		isBuy, reason := s.checkMultiTimeframeConsensus(data, cfg)
		if isBuy {
			s.logger.LogInfo("GenerateSignals [%s]: BUY signal triggered. Reason: %s", data.AssetPair, reason)
			signals = append(signals, StrategySignal{
				AssetPair:      data.AssetPair,
				Direction:      "buy",
				Confidence:     1.0,
				Reason:         reason,
				GeneratedAt:    time.Now(),
				FearGreedIndex: data.FearGreedIndex.Value,
			})
		} else {
			s.logger.LogInfo("GenerateSignals [%s]: Hold condition. Reason: %s", data.AssetPair, reason)
		}
	} else {
		s.logger.LogWarn("GenerateSignals: Multi-Timeframe Consensus is disabled in config. No signals will be generated.")
	}

	return signals, nil
}

// checkBearishConfluence checks for a confluence of bearish signals to increase confidence in an exit.
func checkBearishConfluence(data ConsolidatedMarketPicture, cfg utilities.AppConfig) (bool, string) {
	if len(data.ProvidersData) == 0 {
		return false, ""
	}

	// We'll analyze the data from the primary provider (Kraken)
	primaryData := data.ProvidersData[0]
	if len(primaryData.OHLCVBars) < cfg.Indicators.MACDSlowPeriod {
		return false, "Insufficient data for bearish confluence check"
	}

	// Calculate all primary indicators using the existing function
	rsi, stochRSI, macdHist, _, _, _ := CalculateIndicators(primaryData.OHLCVBars, cfg.Indicators)

	// Define the conditions for a bearish confluence
	isMacdBearish := macdHist < 0
	isRsiOverbought := rsi > 70
	isStochRsiOverbought := stochRSI > cfg.Indicators.StochRSISellThreshold

	// Require at least two of the three conditions to be met for a valid signal
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

// GenerateExitSignal checks for trend-reversal or liquidity-hunt patterns to generate an exit signal.
func (s *strategyImpl) GenerateExitSignal(ctx context.Context, data ConsolidatedMarketPicture, cfg utilities.AppConfig) (StrategySignal, bool) {
	_ = ctx // Acknowledge unused parameter

	// Per research, prioritize specific, identifiable patterns like liquidity hunts for exits. [cite: 527]
	primaryData := data.ProvidersData[0]
	if len(primaryData.OHLCVBars) < 2 {
		return StrategySignal{}, false
	}

	// Check for a liquidity hunt pattern on the primary broker's data
	if IsLiquidityHuntDetected(primaryData.OHLCVBars) {
		s.logger.LogWarn("GenerateExitSignal [%s]: EXIT signal triggered. Reason: Bearish Liquidity Hunt Pattern Detected", data.AssetPair)
		return StrategySignal{
			AssetPair: data.AssetPair,
			Direction: "sell",
			Reason:    "Liquidity Hunt Pattern Detected",
		}, true
	}

	// If no specific pattern, use general multi-indicator confluence for trend reversal. [cite: 583]
	isBearish, reason := checkBearishConfluence(data, cfg)
	if isBearish {
		s.logger.LogWarn("GenerateExitSignal [%s]: EXIT signal triggered. Reason: %s", data.AssetPair, reason)
		return StrategySignal{
			AssetPair: data.AssetPair,
			Direction: "sell",
			Reason:    reason,
		}, true
	}

	// No exit conditions met
	return StrategySignal{}, false
}
