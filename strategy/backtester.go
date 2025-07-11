package strategy

import "Snowballin/utilities"

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

// RunBacktest simulates the DCA strategy over a historical dataset.
// NOTE: This is a simplified backtester for demonstration. A production version would be more complex.
func RunBacktest(bars []utilities.OHLCVBar, params utilities.IndicatorsConfig, tp float64) BacktestResult {
	var netProfit float64
	var wins, losses int
	inTrade := false
	entryPrice := 0.0

	for i := params.MACDSlowPeriod; i < len(bars); i++ {
		// Use a simple MACD crossover for entry/exit signals in this backtest
		currentBars := bars[:i+1]
		macdHist := CalculateMACD(currentBars, params.MACDFastPeriod, params.MACDSlowPeriod, params.MACDSignalPeriod)

		prevBars := bars[:i]
		prevMacdHist := CalculateMACD(prevBars, params.MACDFastPeriod, params.MACDSlowPeriod, params.MACDSignalPeriod)

		// Entry condition: MACD crosses above zero
		if !inTrade && macdHist > 0 && prevMacdHist <= 0 {
			inTrade = true
			entryPrice = currentBars[len(currentBars)-1].Close
		}

		// Exit condition: Take Profit is hit
		if inTrade && currentBars[len(currentBars)-1].High >= entryPrice*(1.0+tp) {
			profit := (entryPrice * (1.0 + tp)) - entryPrice
			netProfit += profit
			wins++
			inTrade = false
		} else if inTrade && macdHist < 0 && prevMacdHist >= 0 { // Exit condition: MACD crosses below zero (loss)
			profit := currentBars[len(currentBars)-1].Close - entryPrice
			netProfit += profit
			losses++
			inTrade = false
		}
	}

	totalTrades := wins + losses
	winRate := 0.0
	if totalTrades > 0 {
		winRate = float64(wins) / float64(totalTrades)
	}

	return BacktestResult{
		Parameters:    params,
		TotalTrades:   totalTrades,
		WinningTrades: wins,
		LosingTrades:  losses,
		NetProfit:     netProfit,
		WinRate:       winRate,
	}
}
