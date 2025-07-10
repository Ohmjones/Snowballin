package strategy

import (
	"Snowballin/dataprovider"
	"Snowballin/utilities"
	"context"
	"fmt"
	"math"
	"strings"
	"time"
)

// NewStrategy constructs a new strategy with an embedded logger.
func NewStrategy(logger *utilities.Logger) *strategyImpl {
	return &strategyImpl{logger: logger}
}

// GenerateDCAEntryLevels calculates ATR-based dynamic DCA entry points.
func (s *strategyImpl) GenerateDCAEntryLevels(bars []utilities.OHLCVBar, currentPrice float64, cfg utilities.AppConfig) ([]float64, error) {
	if len(bars) < cfg.Indicators.ATRPeriod {
		return nil, fmt.Errorf("not enough bars (%d) for ATR calculation", len(bars))
	}

	atr, err := CalculateATR(bars, cfg.Indicators.ATRPeriod)
	if err != nil {
		return nil, fmt.Errorf("ATR calculation failed: %v", err)
	}

	var entryLevels []float64
	cumulativeMultiplier := 0.0
	for _, level := range cfg.DCA.DCALevels {
		cumulativeMultiplier += level
		priceOffset := atr * cfg.DCA.DCAATRMultiplier * cumulativeMultiplier
		entryPrice := currentPrice - priceOffset
		entryLevels = append(entryLevels, entryPrice)
	}

	return entryLevels, nil
}

// CalculateATRBasedSLTP calculates adaptive stop-loss and take-profit using ATR.
func (s *strategyImpl) CalculateATRBasedSLTP(bars []utilities.OHLCVBar, entryPrice float64, cfg utilities.AppConfig) (float64, float64, error) {
	if len(bars) < cfg.Indicators.ATRPeriod {
		return 0, 0, fmt.Errorf("insufficient data for ATR calculation")
	}

	atr, err := CalculateATR(bars, cfg.Indicators.ATRPeriod)
	if err != nil {
		return 0, 0, fmt.Errorf("ATR calculation error: %v", err)
	}

	stopLoss := entryPrice - (atr * cfg.Trading.ATRStopLossMultiplier)
	takeProfit := entryPrice + (atr * cfg.Trading.ATRTakeProfitMultiplier)

	return stopLoss, takeProfit, nil
}

func (s *strategyImpl) ApplyTrailingSLTP(currentPrice, previousPrice float64, currentSL, currentTP float64, cfg utilities.AppConfig) (newSL, newTP float64) {
	newSL, newTP = currentSL, currentTP

	if cfg.Trading.TrailingStopEnabled && currentPrice > previousPrice {
		slMove := currentPrice - currentSL
		newSL += slMove
	}

	if cfg.Trading.TrailingTakeProfitEnabled && currentPrice < previousPrice {
		tpMove := currentTP - currentPrice
		newTP -= tpMove
	}

	return newSL, newTP
}

func (s *strategyImpl) AdjustSizeBasedOnFearGreed(baseSize float64, fg dataprovider.FearGreedIndex) float64 {
	switch fg.Level {
	case "Greed":
		return baseSize * 0.75 // reduce size, market might be overheated
	case "Fear":
		return baseSize * 1.25 // increase size, market might be undervalued
	default:
		return baseSize // neutral
	}
}

func (s *strategyImpl) CheckLiquidityFlush(bars []utilities.OHLCVBar, thresholdPct float64) bool {
	if len(bars) < 2 {
		return false
	}

	lastClose := bars[len(bars)-1].Close
	prevClose := bars[len(bars)-2].Close

	priceDropPct := ((prevClose - lastClose) / prevClose) * 100
	return priceDropPct >= thresholdPct
}

func (s *strategyImpl) GenerateSignals(ctx context.Context, data ConsolidatedMarketPicture, cfg utilities.AppConfig) ([]StrategySignal, error) {
	// --- 0. Pre-computation and Sanity Checks ---
	if len(data.ProvidersData) == 0 {
		s.logger.LogWarn("GenerateSignals: No provider data available, cannot generate signals.")
		return nil, nil
	}
	primaryData := data.ProvidersData[0]
	currentPrice := primaryData.CurrentPrice
	if currentPrice == 0 && len(primaryData.OHLCVBars) > 0 {
		currentPrice = primaryData.OHLCVBars[len(primaryData.OHLCVBars)-1].Close
		s.logger.LogWarn("GenerateSignals: Primary provider price is zero, using last close price (%.2f) as current.", currentPrice)
	}
	if len(primaryData.OHLCVBars) == 0 {
		return nil, fmt.Errorf("cannot generate signals without OHLCV data from primary provider")
	}

	// --- 1. Portfolio-Level Circuit Breaker (Corrected Logic) ---
	// This critical override prevents trading during a severe portfolio drawdown.
	if cfg.CircuitBreaker.Enabled && data.PeakPortfolioValue > 0 {
		drawdownPercent := cfg.CircuitBreaker.DrawdownThresholdPercent / 100.0 // Convert from 10.0 to 0.10
		drawdownThresholdValue := data.PeakPortfolioValue * (1.0 - drawdownPercent)
		if data.PortfolioValue <= drawdownThresholdValue {
			s.logger.LogWarn("CIRCUIT BREAKER: Portfolio value (%.2f) is below drawdown threshold (%.2f). Generating immediate sell signal.", data.PortfolioValue, drawdownThresholdValue)
			return []StrategySignal{{
				AssetPair:        data.AssetPair,
				Direction:        "sell",
				Confidence:       1.0,
				Reason:           fmt.Sprintf("Circuit Breaker: Portfolio drawdown exceeded %.2f%%", cfg.CircuitBreaker.DrawdownThresholdPercent),
				GeneratedAt:      time.Now(),
				RecommendedPrice: currentPrice,
				CalculatedSize:   data.PortfolioValue, // Implies sell all
				FearGreedIndex:   data.FearGreedIndex.Value,
			}}, nil
		}
	}

	// --- 2. Consensus Calculation from Multiple Providers ---
	var totalWeightedScore float64
	var providerReasons []string
	s.logger.LogInfo("GenerateSignals [%s]: Starting consensus calculation across %d providers.", data.AssetPair, len(data.ProvidersData))

	var btcDominanceModifier float64
	baseAsset := strings.Split(data.AssetPair, "/")[0]
	if strings.ToUpper(baseAsset) != "BTC" && data.BTCDominance > 0 {
		if data.BTCDominance > 55.0 { // BTC is strongly dominant, risk-off for alts
			btcDominanceModifier = -0.5
			s.logger.LogInfo("GenerateSignals [%s]: Applying BEARISH BTC Dominance modifier (%.2f) as dominance is high (%.2f%%).", data.AssetPair, btcDominanceModifier, data.BTCDominance)
		} else if data.BTCDominance < 45.0 { // BTC is weak, risk-on for alts
			btcDominanceModifier = 0.25
			s.logger.LogInfo("GenerateSignals [%s]: Applying BULLISH BTC Dominance modifier (+%.2f) as dominance is low (%.2f%%).", data.AssetPair, btcDominanceModifier, data.BTCDominance)
		}
	}

	for _, providerData := range data.ProvidersData {
		if len(providerData.OHLCVBars) < cfg.Indicators.ATRPeriod {
			s.logger.LogWarn("GenerateSignals: Skipping provider '%s' due to insufficient OHLCV data (%d bars).", providerData.Name, len(providerData.OHLCVBars))
			continue
		}

		// Calculate all indicators for this provider's data
		rsi, stochRSI, macdHist, obv, volumeSpike, liquidityHunt := CalculateIndicators(providerData.OHLCVBars, cfg.Indicators)

		var score float64
		var reason string

		// Per research, prioritize specific, identifiable patterns like liquidity hunts for exits.
		if IsLiquidityHuntDetected(providerData.OHLCVBars) {
			score = -2.5 // Assign a strong negative score for a liquidity hunt pattern.
			reason = "Liquidity Hunt Pattern Detected"
		} else {
			// If no specific pattern, use general multi-indicator confluence.
			confirmed, confirmationReason := CheckMultiIndicatorConfirmation(rsi, stochRSI, macdHist, obv, volumeSpike, liquidityHunt, providerData.OHLCVBars, cfg.Indicators)
			if confirmed {
				if strings.Contains(confirmationReason, "Bullish") {
					score = 2.0
				} else if strings.Contains(confirmationReason, "Bearish") {
					score = -2.0
				} else {
					score = -1.0 // Treat general liquidity events as moderately risk-off.
				}
				reason = confirmationReason
			} else {
				// Fallback to a simple additive score if no strong confirmation.
				var tempScore int
				if rsi < 30 {
					tempScore++
				}
				if rsi > 70 {
					tempScore--
				}
				if stochRSI < cfg.Indicators.StochRSIBuyThreshold {
					tempScore++
				}
				if stochRSI > cfg.Indicators.StochRSISellThreshold {
					tempScore--
				}
				if macdHist > 0 {
					tempScore++
				}
				if macdHist < 0 {
					tempScore--
				}
				score = float64(tempScore)
				reason = "Standard Indicator Score"
			}
		}
		// Add order book score for the primary broker (Kraken).
		if providerData.Name == "kraken" {
			// Analyze 2% of the order book depth from the mid-price.
			orderBookScore := AnalyzeOrderBookDepth(data.BrokerOrderBook, 2.0)
			if orderBookScore != 0 {
				score += orderBookScore // Add order book bias to the provider's score.
				reason += fmt.Sprintf(" + OB Score: %.2f", orderBookScore)
			}
		}

		weightedScore := score * providerData.Weight
		totalWeightedScore += weightedScore
		providerReasons = append(providerReasons, fmt.Sprintf("%s: %s (Score:%.1f, W:%.2f -> %.2f)", providerData.Name, reason, score, providerData.Weight, weightedScore))
	}

	if len(providerReasons) == 0 {
		s.logger.LogInfo("GenerateSignals: No providers could be scored. No signal generated.")
		return nil, nil
	}

	consensusReason := strings.Join(providerReasons, " | ")
	s.logger.LogInfo("GenerateSignals [%s]: Provider Consensus Score: %.2f.", data.AssetPair, totalWeightedScore)

	// Apply the BTC Dominance modifier if it has a non-zero value
	if btcDominanceModifier != 0 {
		s.logger.LogInfo("GenerateSignals [%s]: Applying BTC Dominance modifier (%.2f) to score.", data.AssetPair, btcDominanceModifier)
		totalWeightedScore += btcDominanceModifier
	}

	s.logger.LogInfo("GenerateSignals [%s]: Final Score after mods: %.2f. Reason: %s", data.AssetPair, totalWeightedScore, consensusReason)

	// --- 3. Final Decision Making Based on Consensus ---
	var signals []StrategySignal

	// BUY Signal (Entry Mechanism)
	if totalWeightedScore >= cfg.Consensus.ThresholdBuy {
		s.logger.LogInfo("GenerateSignals [%s]: BUY signal triggered by consensus score.", data.AssetPair)
		atr, err := CalculateATR(primaryData.OHLCVBars, cfg.Indicators.ATRPeriod)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate ATR for BUY signal: %w", err)
		}

		positionSize := AdjustPositionSize(data.PortfolioValue, atr, cfg.Trading.MaxPortfolioDrawdown)
		finalSize := s.AdjustSizeBasedOnFearGreed(positionSize, data.FearGreedIndex)
		stopLoss, takeProfit, _ := s.CalculateATRBasedSLTP(primaryData.OHLCVBars, currentPrice, cfg)

		// Using DCA levels to find the first entry point, per the original logic's intent.
		entryLevels, _ := s.GenerateDCAEntryLevels(primaryData.OHLCVBars, currentPrice, cfg)
		recommendedPrice := currentPrice // Default to market if no DCA levels
		if len(entryLevels) > 0 {
			recommendedPrice = entryLevels[0]
		}

		signals = append(signals, StrategySignal{
			AssetPair:        data.AssetPair,
			Direction:        "buy",
			Confidence:       totalWeightedScore,
			Reason:           consensusReason,
			GeneratedAt:      time.Now(),
			FearGreedIndex:   data.FearGreedIndex.Value,
			RecommendedPrice: recommendedPrice,
			CalculatedSize:   finalSize,
			StopLossPrice:    stopLoss,
			TakeProfitPrice:  takeProfit,
		})

		// SELL Signal (Exit Mechanism)
	} else if totalWeightedScore <= cfg.Consensus.ThresholdSell {
		s.logger.LogInfo("GenerateSignals [%s]: SELL signal triggered by consensus score.", data.AssetPair)
		signals = append(signals, StrategySignal{
			AssetPair:        data.AssetPair,
			Direction:        "sell",
			Confidence:       math.Abs(totalWeightedScore),
			Reason:           consensusReason,
			GeneratedAt:      time.Now(),
			FearGreedIndex:   data.FearGreedIndex.Value,
			RecommendedPrice: currentPrice,
			CalculatedSize:   data.PortfolioValue, // Implies sell all
		})

		// HOLD Signal
	} else {
		s.logger.LogInfo("GenerateSignals [%s]: Consensus score %.2f is within hold thresholds (Buy >= %.2f, Sell <= %.2f). No signal generated.", data.AssetPair, totalWeightedScore, cfg.Consensus.ThresholdBuy, cfg.Consensus.ThresholdSell)
	}

	return signals, nil
}
