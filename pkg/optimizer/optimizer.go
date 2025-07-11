// In pkg/optimizer/optimizer.go
package optimizer

import (
	"Snowballin/dataprovider"
	"Snowballin/strategy"
	"Snowballin/utilities"
	"context"
	"encoding/json"
	"os"
	"time"
)

type Optimizer struct {
	logger *utilities.Logger
	cache  *dataprovider.SQLiteCache
	config *utilities.AppConfig
}

func NewOptimizer(logger *utilities.Logger, cache *dataprovider.SQLiteCache, config *utilities.AppConfig) *Optimizer {
	return &Optimizer{logger: logger, cache: cache, config: config}
}

// RunOptimizationCycle finds the best indicator parameters for a given asset.
func (o *Optimizer) RunOptimizationCycle(ctx context.Context, assetPair string) {
	o.logger.LogInfo("[Optimizer] Starting optimization cycle for %s...", assetPair)

	// 1. Fetch 180 days of historical data for the base timeframe
	sixMonthsAgo := time.Now().AddDate(0, -6, 0)
	// Assuming 1h timeframe for optimization
	bars, err := o.cache.GetBars("kraken", assetPair+"-60", sixMonthsAgo.UnixMilli(), time.Now().UnixMilli())
	if err != nil || len(bars) < 200 { // Need a good amount of data
		o.logger.LogError("[Optimizer] Could not fetch sufficient historical data for %s: %v", assetPair, err)
		return
	}

	var bestResult strategy.BacktestResult
	bestScore := -1.0

	// 2. Loop through a range of parameters
	for rsiPeriod := 10; rsiPeriod <= 20; rsiPeriod += 2 {
		for fast := 10; fast <= 15; fast++ {
			for slow := 20; slow <= 30; slow += 2 {
				params := utilities.IndicatorsConfig{
					RSIPeriod:        rsiPeriod,
					MACDFastPeriod:   fast,
					MACDSlowPeriod:   slow,
					MACDSignalPeriod: 9, // Can also be optimized
				}

				// 3. Run the backtest for each combination
				result := strategy.RunBacktest(bars, params, o.config.Trading.TakeProfitPercentage)

				// 4. Score the result (e.g., by net profit)
				if result.NetProfit > bestScore {
					bestScore = result.NetProfit
					bestResult = result
				}
			}
		}
	}

	if bestScore > 0 {
		o.logger.LogInfo("[Optimizer] Found new best parameters for %s: RSI(%d), MACD(%d, %d, %d). Net Profit: %.2f",
			assetPair, bestResult.Parameters.RSIPeriod, bestResult.Parameters.MACDFastPeriod, bestResult.Parameters.MACDSlowPeriod, bestResult.Parameters.MACDSignalPeriod, bestResult.NetProfit)

		// 5. Save the winning parameters to a file
		o.saveOptimizedParams(bestResult.Parameters)
	} else {
		o.logger.LogWarn("[Optimizer] Optimization cycle for %s did not yield a profitable result.", assetPair)
	}
}

func (o *Optimizer) saveOptimizedParams(params utilities.IndicatorsConfig) {
	file, err := json.MarshalIndent(params, "", "  ")
	if err != nil {
		o.logger.LogError("[Optimizer] Failed to marshal optimized params: %v", err)
		return
	}

	err = os.WriteFile("config/optimized_params.json", file, 0644)
	if err != nil {
		o.logger.LogError("[Optimizer] Failed to write optimized params file: %v", err)
	}
}

// StartScheduledOptimization launches a goroutine to run the optimizer periodically.
func (o *Optimizer) StartScheduledOptimization(ctx context.Context) {
	// Run once on startup
	go func() {
		for _, pair := range o.config.Trading.AssetPairs {
			o.RunOptimizationCycle(ctx, pair)
		}
	}()

	// Then run on a schedule (e.g., every 7 days)
	ticker := time.NewTicker(7 * 24 * time.Hour)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, pair := range o.config.Trading.AssetPairs {
					o.RunOptimizationCycle(ctx, pair)
				}
			}
		}
	}()
}
