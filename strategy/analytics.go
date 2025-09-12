package strategy

import (
	"Snowballin/dataprovider"
	"Snowballin/pkg/broker"
	"Snowballin/utilities"
	"fmt"
	"math"
	"sort"
	"strings"
)

type OrderBookAnalysis struct {
	DepthScore       float64            // The original -1.0 to 1.0 score indicating buy/sell pressure.
	SupportLevels    []SignificantLevel // A list of price levels with unusually high bid volume (buy walls).
	ResistanceLevels []SignificantLevel // A list of price levels with unusually high ask volume (sell walls).
}

type SignificantLevel struct {
	PriceLevel float64
	Volume     float64
}

// SentimentConsensus holds the result of a cross-provider analysis of an asset's social traction.
type SentimentConsensus struct {
	IsTrending     bool     // True if the asset is trending on a consensus basis.
	AttentionScore float64  // A weighted score from 0.0 to 1.0 indicating the level of consensus.
	TrendingOn     []string // A list of providers where the asset is trending.
}

// CalculateSentimentConsensus checks if a specific asset symbol is present in the trending lists
// from multiple data providers and calculates a weighted attention score.
func CalculateSentimentConsensus(
	assetSymbol string,
	cgTrending []dataprovider.TrendingCoin,
	cmcTrending []dataprovider.TrendingCoin,
	logger *utilities.Logger,
) SentimentConsensus {

	// Define the weights for each provider.
	weights := map[string]float64{
		"coingecko":     0.51,
		"coinmarketcap": 0.49,
	}

	var attentionScore float64
	var trendingOn []string
	upperAssetSymbol := strings.ToUpper(assetSymbol)

	// Check CoinGecko's list
	for _, coin := range cgTrending {
		if strings.ToUpper(coin.Coin.Symbol) == upperAssetSymbol {
			attentionScore += weights["coingecko"]
			trendingOn = append(trendingOn, "CoinGecko")
			logger.LogInfo("Sentiment Check [%s]: Asset is trending on CoinGecko.", assetSymbol)
			break // Stop after finding a match
		}
	}

	// Check CoinMarketCap's list
	for _, coin := range cmcTrending {
		if strings.ToUpper(coin.Coin.Symbol) == upperAssetSymbol {
			attentionScore += weights["coinmarketcap"]
			trendingOn = append(trendingOn, "CoinMarketCap")
			logger.LogInfo("Sentiment Check [%s]: Asset is trending on CoinMarketCap.", assetSymbol)
			break // Stop after finding a match
		}
	}

	// The threshold for consensus can be adjusted. Here, we consider it "trending"
	// if it appears on at least one major provider (score > 0.49).
	isTrending := attentionScore > 0.49

	return SentimentConsensus{
		IsTrending:     isTrending,
		AttentionScore: attentionScore,
		TrendingOn:     trendingOn,
	}
}

// CalculateMarketSentiment analyzes top gainers and losers to create a market sentiment score.
func CalculateMarketSentiment(gainers []dataprovider.MarketData, losers []dataprovider.MarketData, logger *utilities.Logger) MarketSentiment {
	if len(gainers) == 0 || len(losers) == 0 {
		return MarketSentiment{State: "Neutral", Score: 0, Message: "Insufficient gainer/loser data."}
	}

	// Define what constitutes a "significant" move.
	const significantMoveThreshold = 5.0 // 5%

	var significantGainers, significantLosers int

	for _, gainer := range gainers {
		if gainer.PriceChange24h > significantMoveThreshold {
			significantGainers++
		}
	}

	for _, loser := range losers {
		// PriceChange24h for losers will be negative.
		if loser.PriceChange24h < -significantMoveThreshold {
			significantLosers++
		}
	}

	// Avoid division by zero if there are no significant losers.
	if significantLosers == 0 {
		if significantGainers > 5 { // If we have many gainers and no losers, it's very bullish.
			return MarketSentiment{State: "Bullish", Score: 1.0, Message: fmt.Sprintf("Strongly Bullish: %d significant gainers vs. 0 losers.", significantGainers)}
		}
		return MarketSentiment{State: "Neutral", Score: 0.5, Message: "Neutral: Some gainers but no significant losers."}
	}

	ratio := float64(significantGainers) / float64(significantLosers)

	// Normalize the ratio into a state and score.
	if ratio > 2.0 {
		return MarketSentiment{State: "Bullish", Score: math.Min(1.0, ratio/4.0), Message: fmt.Sprintf("Bullish market mood (%.2f : 1 gainer/loser ratio).", ratio)}
	}
	if ratio < 0.75 {
		return MarketSentiment{State: "Bearish", Score: math.Max(-1.0, ratio-1.0), Message: fmt.Sprintf("Bearish market mood (%.2f : 1 gainer/loser ratio).", ratio)}
	}

	return MarketSentiment{State: "Neutral", Score: (ratio-0.75)/(2.0-0.75)*1.5 - 0.75, Message: fmt.Sprintf("Neutral market mood (%.2f : 1 gainer/loser ratio).", ratio)}
}

// ComputeEMASeries explicitly computes the Exponential Moving Average (EMA).
func ComputeEMASeries(data []float64, period int) ([]float64, []float64) {
	if period <= 0 || len(data) == 0 {
		return nil, nil
	}

	ema := make([]float64, len(data))
	multiplier := 2.0 / float64(period+1)

	ema[0] = data[0]
	for i := 1; i < len(data); i++ {
		ema[i] = (data[i]-ema[i-1])*multiplier + ema[i-1]
	}
	return ema, nil
}

func CalculateSMA(data []float64, period int) float64 {
	if len(data) < period {
		return 0.0
	}

	segment := data[len(data)-period:]
	sum := 0.0
	for _, v := range segment {
		sum += v
	}
	return sum / float64(period)
}

// CalculateATR explicitly calculates the Average True Range (ATR) over the last 'period' bars.
func CalculateATR(bars []utilities.OHLCVBar, period int) (float64, error) { // Changed signature
	n := len(bars)
	if period <= 0 || n < period+1 {
		// Return an error if conditions are not met
		return 0.0, fmt.Errorf("not enough bars (%d) for ATR calculation of period %d", n, period)
	}

	sum := 0.0
	for i := 1; i <= period; i++ {
		curr := bars[n-i]
		prev := bars[n-i-1]

		highLow := curr.High - curr.Low
		highClose := math.Abs(curr.High - prev.Close)
		lowClose := math.Abs(curr.Low - prev.Close)

		trueRange := math.Max(highLow, math.Max(highClose, lowClose))
		sum += trueRange
	}
	return sum / float64(period), nil // Return result and nil error
}

// This function combines the general depth pressure score with the identification of
// specific support and resistance levels (walls).
func PerformOrderBookAnalysis(orderBook broker.OrderBookData, depthPercent float64, windowSize int, spikeMultiplier float64) OrderBookAnalysis {
	// 1. Calculate the simple buy/sell pressure score.
	depthScore := AnalyzeOrderBookDepth(orderBook, depthPercent)

	// 2. Identify significant support and resistance levels.
	support, resistance := FindSignificantLevels(orderBook, windowSize, spikeMultiplier)

	// Add sorting: Support descending price (highest/closest first), Resistance ascending (lowest/closest first)
	sort.Slice(support, func(i, j int) bool {
		return support[i].PriceLevel < support[j].PriceLevel // Lowest first for deeper dips
	})
	sort.Slice(resistance, func(i, j int) bool {
		return resistance[i].PriceLevel < resistance[j].PriceLevel
	})

	// 3. Return the combined analysis.
	return OrderBookAnalysis{
		DepthScore:       depthScore,
		SupportLevels:    support,
		ResistanceLevels: resistance,
	}
}

// --- Finds significant volume clusters (buy/sell walls) in the order book. ---
// It identifies levels where the volume is significantly higher than its neighbors.
// `windowSize`: Number of neighboring levels to average for comparison.
// `spikeMultiplier`: How many times larger the volume must be than the average to be "significant".
func FindSignificantLevels(orderBook broker.OrderBookData, windowSize int, spikeMultiplier float64) (support []SignificantLevel, resistance []SignificantLevel) {
	if spikeMultiplier <= 1.0 { // Multiplier must be greater than 1 to be useful.
		spikeMultiplier = 1.5
	}
	if windowSize <= 0 {
		windowSize = 10
	}

	// Find Support Levels (from Bids)
	if len(orderBook.Bids) > windowSize {
		for i := 0; i < len(orderBook.Bids)-windowSize; i++ {
			currentBid := orderBook.Bids[i]
			var sumOfNeighbors float64
			// Look at the next N bids deeper in the book.
			for j := 1; j <= windowSize; j++ {
				sumOfNeighbors += orderBook.Bids[i+j].Volume
			}
			avgVolumeOfNeighbors := sumOfNeighbors / float64(windowSize)

			// If the current level's volume is a multiple of the average, it's a wall.
			if currentBid.Volume >= avgVolumeOfNeighbors*spikeMultiplier {
				support = append(support, SignificantLevel{
					PriceLevel: currentBid.Price,
					Volume:     currentBid.Volume,
				})
			}
		}
	}

	// Find Resistance Levels (from Asks)
	if len(orderBook.Asks) > windowSize {
		for i := 0; i < len(orderBook.Asks)-windowSize; i++ {
			currentAsk := orderBook.Asks[i]
			var sumOfNeighbors float64
			// Look at the next N asks deeper in the book.
			for j := 1; j <= windowSize; j++ {
				sumOfNeighbors += orderBook.Asks[i+j].Volume
			}
			avgVolumeOfNeighbors := sumOfNeighbors / float64(windowSize)

			// If the current level's volume is a multiple of the average, it's a wall.
			if currentAsk.Volume >= avgVolumeOfNeighbors*spikeMultiplier {
				resistance = append(resistance, SignificantLevel{
					PriceLevel: currentAsk.Price,
					Volume:     currentAsk.Volume,
				})
			}
		}
	}
	return support, resistance
}

// AnalyzeOrderBookDepth calculates the ratio of bid volume to ask volume within a
// certain percentage of the mid-price, returning a score from -1.0 to 1.0.
// This is a measure of immediate buy/sell pressure.
func AnalyzeOrderBookDepth(orderBook broker.OrderBookData, depthPercent float64) float64 {
	if len(orderBook.Bids) == 0 || len(orderBook.Asks) == 0 {
		return 0 // Not enough data to analyze
	}

	bestBid := orderBook.Bids[0].Price
	bestAsk := orderBook.Asks[0].Price
	midPrice := (bestBid + bestAsk) / 2.0

	priceRange := midPrice * (depthPercent / 100.0)
	minPrice := midPrice - priceRange
	maxPrice := midPrice + priceRange

	var bidVolume float64
	for _, bid := range orderBook.Bids {
		if bid.Price >= minPrice {
			bidVolume += bid.Volume
		} else {
			break // Bids are sorted high to low
		}
	}

	var askVolume float64
	for _, ask := range orderBook.Asks {
		if ask.Price <= maxPrice {
			askVolume += ask.Volume
		} else {
			break // Asks are sorted low to high
		}
	}

	if askVolume == 0 {
		if bidVolume > 0 {
			return 1.0 // Infinite bid support, strongly bullish
		}
		return 0.0 // No volume on either side
	}

	ratio := bidVolume / askVolume

	// Normalize the ratio to a score between -1.0 and 1.0.
	// A ratio > 2.0 is strongly bullish, < 0.5 is strongly bearish.
	if ratio >= 2.0 {
		return 1.0
	}
	if ratio <= 0.5 {
		return -1.0
	}

	// Linearly scale the ratio between 0.5 and 2.0 to the range [-1.0, 1.0]
	// Formula: ((value - old_min) / (old_max - old_min)) * (new_max - new_min) + new_min
	return ((ratio-0.5)/(2.0-0.5))*(1.0-(-1.0)) + (-1.0)
}

// VolatilityAdjustedOrderPrice explicitly adjusts an order price based on volatility (ATR).
func VolatilityAdjustedOrderPrice(lastPrice, atr, multiplier float64, buy bool) float64 {
	if buy {
		return lastPrice - atr*multiplier
	}
	return lastPrice + atr*multiplier
}

// CheckMultiIndicatorConfirmation provides a final, detailed check after the main consensus passes.
func CheckMultiIndicatorConfirmation(rsi, stochRSI, macdHist, obv float64, volumeSpike, liquidityHunt bool, bars []utilities.OHLCVBar, cfg utilities.IndicatorsConfig) (bool, string) {
	// Condition 1: A liquidity hunt event is always a strong signal.
	if liquidityHunt && volumeSpike {
		return true, "Liquidity event with volume spike"
	}

	// --- MODIFIED LOGIC ---
	// Condition 2: A classic dip-buying signal. We're looking for oversold conditions
	// on multiple momentum oscillators, which is a high-confidence signal that a
	// bounce is imminent. We REMOVED the `macdHist > 0` check as it was
	// contradicting our main "Momentum Shift" entry logic.
	isRsiOversold := rsi < 40
	isStochRsiOversold := stochRSI < cfg.StochRSIBuyThreshold // e.g., < 20

	isObvBullish := isOBVBullish(obv, bars)
	if isRsiOversold && isStochRsiOversold && isObvBullish {
		return true, fmt.Sprintf("Bullish Confirmation: RSI (%.2f), StochRSI (%.2f), and OBV (%.2f) are aligned.", rsi, stochRSI, obv)
	}
	// --- END MODIFIED LOGIC ---

	// This is a bearish (sell) condition and is not relevant for a buy confirmation.
	// It can be kept for future use or removed if this function is only for buys.
	if rsi > 70 && stochRSI > cfg.StochRSISellThreshold && macdHist < 0 && isOBVBearish(obv, bars) {
		// This part of the logic would not be hit for a buy signal anyway.
		return false, "Bearish conditions met, not bullish."
	}

	// This is the debug print that helped us find the issue. It can be removed or kept.
	fmt.Printf("FinalConf: RSI=%.2f, StochRSI=%.2f, MACDHist=%.4f, LiquidityHunt=%v, VolSpike=%v\n", rsi, stochRSI, macdHist, liquidityHunt, volumeSpike)

	return false, "No strong multi-indicator confirmation"
}

func isOBVBullish(currentOBV float64, bars []utilities.OHLCVBar) bool {
	if len(bars) < 2 {
		return false
	}
	historicalOBV := CalculateOBV(bars[:len(bars)-1])
	return currentOBV > historicalOBV
}

func isOBVBearish(currentOBV float64, bars []utilities.OHLCVBar) bool {
	if len(bars) < 2 {
		return false
	}
	historicalOBV := CalculateOBV(bars[:len(bars)-1])
	return currentOBV < historicalOBV
}

// AdjustPositionSize explicitly adjusts position sizing based on portfolio drawdown risk and volatility.
func AdjustPositionSize(portfolioValue, atr, maxDrawdown float64) float64 {
	riskAmount := portfolioValue * maxDrawdown
	if atr == 0 {
		return 0
	}
	return riskAmount / atr
}

// HandlePartialFills explicitly handles partial fills by calculating a new limit price for unfilled quantity.
func HandlePartialFills(requested, filled, lastPrice, atr, multiplier float64) float64 {
	unfilled := requested - filled
	if unfilled <= 0 {
		return 0
	}
	return VolatilityAdjustedOrderPrice(lastPrice, atr, multiplier, true)
}

// VPVREntry explicitly defines a price bucket and its aggregated volume.
type VPVREntry struct {
	PriceLevel float64
	Volume     float64
}

// CalculateVPVR explicitly calculates the Volume Profile Visible Range (VPVR) by
// binning each bar's volume into the bucket corresponding to its close price,
// then returning entries sorted by descending volume.
func CalculateVPVR(bars []utilities.OHLCVBar, bins int) []VPVREntry {
	entries := []VPVREntry{}
	if bins <= 0 || len(bars) == 0 {
		return entries
	}
	// Determine price range and total volume
	minPrice, maxPrice := bars[0].Low, bars[0].High
	totalVol := 0.0
	for _, bar := range bars {
		totalVol += bar.Volume
		if bar.Low < minPrice {
			minPrice = bar.Low
		}
		if bar.High > maxPrice {
			maxPrice = bar.High
		}
	}

	priceRange := maxPrice - minPrice
	if priceRange == 0 {
		// all prices equal, single bucket
		entries = append(entries, VPVREntry{PriceLevel: minPrice, Volume: totalVol})
		return entries
	}

	binSize := priceRange / float64(bins)
	// accumulate volume per bucket based on close price
	volBuckets := make([]float64, bins)
	for _, bar := range bars {
		idx := int((bar.Close - minPrice) / binSize)
		if idx < 0 {
			idx = 0
		} else if idx >= bins {
			idx = bins - 1
		}
		volBuckets[idx] += bar.Volume
	}

	// build entries at bucket centers
	for i, v := range volBuckets {
		center := minPrice + binSize*float64(i) + binSize/2.0
		entries = append(entries, VPVREntry{PriceLevel: center, Volume: v})
	}

	// sort by volume descending
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Volume > entries[j].Volume
	})
	return entries
}

// FindBestLimitPrice analyzes the order book to find a strategic price for a limit buy order.
// It looks for a "buy wall" (a bid with high volume) within a certain percentage
// of the initial targetPrice to improve the probability of a successful fill and bounce.
func FindBestLimitPrice(orderBook broker.OrderBookData, targetPrice float64, searchDepthPercent float64) float64 {
	if len(orderBook.Bids) == 0 {
		return targetPrice // Fallback if order book is empty
	}

	// Define the search range around the target price
	searchCutoff := targetPrice * (1.0 - (searchDepthPercent / 100.0))

	bestPrice := targetPrice
	maxVolume := 0.0

	// Iterate through the bids to find the one with the highest volume within our search range
	for _, bid := range orderBook.Bids {
		// Stop searching if the bid price is too far below our target
		if bid.Price < searchCutoff {
			break
		}

		// If this bid's volume is the largest we've seen so far, it's our new best price
		if bid.Volume > maxVolume {
			maxVolume = bid.Volume
			bestPrice = bid.Price
		}
	}

	// Return the price with the highest volume found, or the original target if no better price was identified.
	return bestPrice
}

// FindSupportNearPrice analyzes the order book to find a strategic price for a limit buy order
// within a specific window around a target price.
// `targetPrice`: The ideal price to snipe, calculated from ATR.
// `searchWindowPercent`: How far around the targetPrice to look for a volume wall (e.g., 0.5%).
func FindSupportNearPrice(orderBook broker.OrderBookData, targetPrice float64, searchWindowPercent float64) (float64, bool) {
	if len(orderBook.Bids) == 0 {
		return 0, false // Cannot find a price if the book is empty.
	}

	// Define the absolute search window around the target price.
	window := targetPrice * (searchWindowPercent / 100.0)
	upperBound := targetPrice + window
	lowerBound := targetPrice - window

	var bestPrice float64
	maxVolume := 0.0
	foundSupport := false

	// Iterate through bids to find the one with the highest volume within our search window.
	for _, bid := range orderBook.Bids {
		// If we've gone past our lower bound, we can stop.
		if bid.Price < lowerBound {
			break
		}

		// Check if the current bid is within our search window.
		if bid.Price <= upperBound && bid.Price >= lowerBound {
			if bid.Volume > maxVolume {
				maxVolume = bid.Volume
				bestPrice = bid.Price
				foundSupport = true
			}
		}
	}

	return bestPrice, foundSupport
}

// This function will process the data fetched from the new dataprovider method
func CalculateAggregatedLiquidity(tickers []dataprovider.CrossExchangeTicker, primaryExchangeName string) AggregatedLiquidityMetrics {
	var totalVolume, weightedPriceSum float64
	var primaryExchangeVolume float64

	if len(tickers) == 0 {
		return AggregatedLiquidityMetrics{}
	}

	// Sort tickers by volume to easily find the top ones
	sort.Slice(tickers, func(i, j int) bool {
		return tickers[i].Volume24h > tickers[j].Volume24h
	})

	for _, ticker := range tickers {
		// Only include tickers with a positive price and volume for accurate calculations
		if ticker.Price > 0 && ticker.Volume24h > 0 {
			totalVolume += ticker.Volume24h
			weightedPriceSum += ticker.Price * ticker.Volume24h
			if strings.EqualFold(ticker.ExchangeName, primaryExchangeName) {
				primaryExchangeVolume = ticker.Volume24h
			}
		}
	}

	vwap := 0.0
	if totalVolume > 0 {
		vwap = weightedPriceSum / totalVolume
	}

	primaryShare := 0.0
	if totalVolume > 0 {
		primaryShare = primaryExchangeVolume / totalVolume
	}

	var topExchanges []string
	for i := 0; i < 5 && i < len(tickers); i++ {
		topExchanges = append(topExchanges, tickers[i].ExchangeName)
	}

	return AggregatedLiquidityMetrics{
		VolumeWeightedAvgPrice: vwap,
		TopExchangesByVolume:   topExchanges,
		KrakenVolumeShare:      primaryShare,
		TotalMarketVolume24h:   totalVolume,
	}
}
