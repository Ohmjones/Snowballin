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

	baseAsset := strings.Split(assetPair, "/")[0]
	quoteAsset := strings.Split(assetPair, "/")[1]
	interval := "1h"
	sixMonthsAgo := time.Now().AddDate(0, -6, 0)

	potentialProviders := []string{"coingecko", "coinmarketcap"}
	var lastErr error

	for _, providerName := range potentialProviders {
		id, err := o.dp.GetCoinID(ctx, baseAsset)
		if err != nil {
			o.logger.LogWarn("[Optimizer] Could not get asset ID for %s, skipping provider %s", baseAsset, providerName)
			lastErr = err
			continue
		}

		cacheKey := fmt.Sprintf("%s-%s-%s", id, strings.ToLower(quoteAsset), interval)
		bars, err = o.cache.GetBars(providerName, cacheKey, sixMonthsAgo.UnixMilli(), time.Now().UnixMilli())
		if err != nil {
			lastErr = err
		}

		if err == nil && len(bars) >= 40 {
			o.logger.LogInfo("[Optimizer] Found sufficient historical data for %s under provider '%s' with key '%s'", assetPair, providerName, cacheKey)
			lastErr = nil
			break
		}
	}

	if len(bars) < 40 {
		o.logger.LogError("[Optimizer] Could not fetch sufficient historical data for %s from any provider. Last error: %v", assetPair, lastErr)
		return
	}

	o.logger.LogInfo("[Optimizer] Found %d historical bars for %s. Beginning optimization...", len(bars), assetPair)

	var bestResult strategy.BacktestResult
	bestScore := -1e9

	for stochBuy := 20.0; stochBuy <= 30.0; stochBuy += 5.0 {
		for rsiPeriod := 12; rsiPeriod <= 16; rsiPeriod += 2 {
			for fast := 10; fast <= 15; fast++ {
				for slow := 20; slow <= 30; slow += 2 {
					btCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
					defer cancel()

					if btCtx.Err() != nil {
						o.logger.LogWarn("[Optimizer] Optimization cycle for %s cancelled.", assetPair)
						return
					}

					params := utilities.IndicatorsConfig{
						RSIPeriod:             rsiPeriod,
						StochRSIBuyThreshold:  stochBuy,
						StochRSISellThreshold: 80.0,
						StochRSIPeriod:        14,
						MACDFastPeriod:        fast,
						MACDSlowPeriod:        slow,
						MACDSignalPeriod:      9,
					}

					// --- MODIFIED: The call to RunBacktest now passes the trading config struct ---
					result := strategy.RunBacktest(bars, params, o.config.Trading)

					if result.NetProfit > bestScore {
						bestScore = result.NetProfit
						bestResult = result
					}
				}
			}
		}
	}

	if bestScore > -1e9 {
		o.logger.LogInfo("[Optimizer] Found new best parameters for %s -> Profit: %.2f | RSI(%d) StochBuy(%.1f) MACD(%d,%d,%d)",
			assetPair, bestResult.NetProfit,
			bestResult.Parameters.RSIPeriod, bestResult.Parameters.StochRSIBuyThreshold,
			bestResult.Parameters.MACDFastPeriod, bestResult.Parameters.MACDSlowPeriod, bestResult.Parameters.MACDSignalPeriod)

		finalParams := o.config.Indicators
		finalParams.RSIPeriod = bestResult.Parameters.RSIPeriod
		finalParams.StochRSIBuyThreshold = bestResult.Parameters.StochRSIBuyThreshold
		finalParams.MACDFastPeriod = bestResult.Parameters.MACDFastPeriod
		finalParams.MACDSlowPeriod = bestResult.Parameters.MACDSlowPeriod
		finalParams.MACDSignalPeriod = bestResult.Parameters.MACDSignalPeriod

		o.saveOptimizedParams(finalParams)

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
