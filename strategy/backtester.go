package strategy

import (
	"Snowballin/utilities"
	"math"
)

// BacktestResult holds the performance metrics of a single backtest run.
type BacktestResult struct {
	Parameters    utilities.IndicatorsConfig
	TotalTrades   int
	WinningTrades int
	LosingTrades  int
	NetProfit     float64
	ProfitFactor  float64
	WinRate       float64
}

// RunBacktest simulates a trading strategy over historical data.
func RunBacktest(bars []utilities.OHLCVBar, indicatorParams utilities.IndicatorsConfig, tradingParams utilities.TradingConfig) BacktestResult {
	var netProfit float64
	var totalTrades, winningTrades int

	type simulatedPosition struct {
		IsActive           bool
		EntryPrice         float64
		AveragePrice       float64
		BaseOrderSize      float64
		TotalVolume        float64
		FilledSafetyOrders int
	}
	currentTrade := simulatedPosition{}

	requiredBars := indicatorParams.MACDSlowPeriod
	if indicatorParams.RSIPeriod > requiredBars {
		requiredBars = indicatorParams.RSIPeriod
	}
	if indicatorParams.OBVMAPeriod > requiredBars {
		requiredBars = indicatorParams.OBVMAPeriod
	}

	// Pre-calculate the full OBV series and close prices once for efficiency
	fullObvSeries := make([]float64, len(bars))
	closePrices := make([]float64, len(bars))
	for i := range bars {
		closePrices[i] = bars[i].Close
		if i > 0 {
			if bars[i].Close > bars[i-1].Close {
				fullObvSeries[i] = fullObvSeries[i-1] + bars[i].Volume
			} else if bars[i].Close < bars[i-1].Close {
				fullObvSeries[i] = fullObvSeries[i-1] - bars[i].Volume
			} else {
				fullObvSeries[i] = fullObvSeries[i-1]
			}
		}
	}

	for i := requiredBars; i < len(bars); i++ {
		currentBars := bars[:i+1]
		currentBar := currentBars[len(currentBars)-1]
		currentCloses := closePrices[:i+1]

		if !currentTrade.IsActive {
			rsi := CalculateRSI(currentBars, indicatorParams.RSIPeriod)
			// [FIX] Correctly call CalculateMACD with close prices and capture all return values
			_, _, macdHist := CalculateMACD(currentCloses, indicatorParams.MACDFastPeriod, indicatorParams.MACDSlowPeriod, indicatorParams.MACDSignalPeriod)

			currentOBV := fullObvSeries[i]
			obvSMA := CalculateSMA(fullObvSeries[:i+1], indicatorParams.OBVMAPeriod)

			if macdHist > 0 && rsi < 60 && currentOBV > obvSMA {
				currentTrade = simulatedPosition{
					IsActive:           true,
					EntryPrice:         currentBar.Close,
					AveragePrice:       currentBar.Close,
					BaseOrderSize:      1.0,
					TotalVolume:        1.0,
					FilledSafetyOrders: 0,
				}
			}
			continue
		}

		if currentTrade.IsActive {
			takeProfitPrice := currentTrade.AveragePrice * (1.0 + tradingParams.TakeProfitPercentage)
			if currentBar.High >= takeProfitPrice {
				profit := (takeProfitPrice - currentTrade.AveragePrice) * currentTrade.TotalVolume
				netProfit += profit
				totalTrades++
				winningTrades++
				currentTrade = simulatedPosition{}
				continue
			}

			if currentTrade.FilledSafetyOrders >= tradingParams.MaxSafetyOrders {
				continue
			}

			nextSONumber := currentTrade.FilledSafetyOrders + 1
			var totalDeviationPercentage float64
			currentStep := tradingParams.PriceDeviationToOpenSafetyOrders
			for j := 0; j < nextSONumber; j++ {
				totalDeviationPercentage += currentStep
				currentStep *= tradingParams.SafetyOrderStepScale
			}
			safetyOrderPrice := currentTrade.EntryPrice * (1.0 - (totalDeviationPercentage / 100.0))

			if currentBar.Low <= safetyOrderPrice {
				soVolume := currentTrade.BaseOrderSize * math.Pow(tradingParams.SafetyOrderVolumeScale, float64(nextSONumber))
				oldCost := currentTrade.AveragePrice * currentTrade.TotalVolume
				newCost := safetyOrderPrice * soVolume
				newTotalVolume := currentTrade.TotalVolume + soVolume
				currentTrade.AveragePrice = (oldCost + newCost) / newTotalVolume
				currentTrade.TotalVolume = newTotalVolume
				currentTrade.FilledSafetyOrders++
			}
		}
	}

	winRate := 0.0
	if totalTrades > 0 {
		winRate = float64(winningTrades) / float64(totalTrades)
	}

	return BacktestResult{
		Parameters:    indicatorParams,
		TotalTrades:   totalTrades,
		WinningTrades: winningTrades,
		LosingTrades:  totalTrades - winningTrades,
		NetProfit:     netProfit,
		WinRate:       winRate,
	}
}
