// In pkg/optimizer/optimizer.go
package optimizer

import (
	"Snowballin/dataprovider"
	"Snowballin/strategy"
	"Snowballin/utilities"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type Optimizer struct {
	logger *utilities.Logger
	cache  *dataprovider.SQLiteCache
	config *utilities.AppConfig
	dp     dataprovider.DataProvider
}

func NewOptimizer(logger *utilities.Logger, cache *dataprovider.SQLiteCache, config *utilities.AppConfig, dp dataprovider.DataProvider) *Optimizer {
	return &Optimizer{
		logger: logger,
		cache:  cache,
		config: config,
		dp:     dp,
	}
}

// RunOptimizationCycle finds the best indicator parameters for a given asset.
func (o *Optimizer) RunOptimizationCycle(ctx context.Context, assetPair string) {
	o.logger.LogInfo("[Optimizer] Starting optimization cycle for %s...", assetPair)

	var bars []utilities.OHLCVBar
	var err error

	// --- MODIFIED: Logic to build the correct cache key ---
	// The optimizer now uses its data provider to get the correct ID, ensuring the key matches what was primed.
	baseAsset := strings.Split(assetPair, "/")[0]
	quoteAsset := strings.Split(assetPair, "/")[1]

	coinID, err := o.dp.GetCoinID(ctx, baseAsset)
	if err != nil {
		o.logger.LogError("[Optimizer] Could not get CoinGecko ID for %s: %v", baseAsset, err)
		return
	}

	// For optimization, we'll use the 1-hour timeframe ("1h" is the user-friendly format)
	interval := "1h"
	cacheKey := fmt.Sprintf("%s-%s-%s", coinID, strings.ToLower(quoteAsset), interval)
	providerName := "coingecko" // We know the data was primed from CoinGecko

	sixMonthsAgo := time.Now().AddDate(0, -6, 0)
	bars, err = o.cache.GetBars(providerName, cacheKey, sixMonthsAgo.UnixMilli(), time.Now().UnixMilli())

	if err != nil || len(bars) < 200 {
		o.logger.LogError("[Optimizer] Could not fetch sufficient historical data for key '%s' under provider '%s': %v", cacheKey, providerName, err)
		return
	}
	o.logger.LogInfo("[Optimizer] Found %d historical bars for key '%s' to run backtest.", len(bars), cacheKey)

	var bestResult strategy.BacktestResult
	bestScore := -1.0

	// 2. Loop through a range of parameters
	for rsiPeriod := 10; rsiPeriod <= 20; rsiPeriod += 2 {
		for fast := 10; fast <= 15; fast++ {
			for slow := 20; slow <= 30; slow += 2 {
				btCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
				defer cancel()

				if btCtx.Err() != nil {
					o.logger.LogWarn("[Optimizer] Optimization cycle for %s cancelled.", assetPair)
					return
				}

				params := utilities.IndicatorsConfig{
					RSIPeriod:        rsiPeriod,
					MACDFastPeriod:   fast,
					MACDSlowPeriod:   slow,
					MACDSignalPeriod: 9,
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
