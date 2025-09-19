package mapper

import (
	"Snowballin/dataprovider"
	"Snowballin/pkg/broker/kraken"
	"Snowballin/utilities"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// AssetMapper is responsible for resolving and caching asset identities across all providers.
type AssetMapper struct {
	cfg           *utilities.AppConfig
	db            *dataprovider.SQLiteCache
	logger        *utilities.Logger
	kraken        *kraken.Client
	coingecko     dataprovider.DataProvider
	coinmarketcap dataprovider.DataProvider
	identityCache sync.Map // In-memory cache: map[string]*dataprovider.AssetIdentity
}

// NewAssetMapper creates a new instance of the AssetMapper.
func NewAssetMapper(db *dataprovider.SQLiteCache, logger *utilities.Logger, kr *kraken.Client, cg, cmc dataprovider.DataProvider, cfg *utilities.AppConfig) *AssetMapper {
	return &AssetMapper{
		cfg:           cfg,
		db:            db,
		logger:        logger,
		kraken:        kr,
		coingecko:     cg,
		coinmarketcap: cmc,
		identityCache: sync.Map{},
	}
}

// GetIdentity is the main entry point. It finds an asset's identity from cache, DB, or by discovery.
func (m *AssetMapper) GetIdentity(ctx context.Context, commonSymbol string) (*dataprovider.AssetIdentity, error) {
	upperSymbol := strings.ToUpper(commonSymbol)

	if identity, ok := m.identityCache.Load(upperSymbol); ok {
		return identity.(*dataprovider.AssetIdentity), nil
	}

	identity, err := m.db.GetAssetIdentity(upperSymbol)
	if err != nil {
		return nil, fmt.Errorf("error checking database for asset identity %s: %w", upperSymbol, err)
	}
	if identity != nil {
		m.identityCache.Store(upperSymbol, identity)
		return identity, nil
	}

	m.logger.LogInfo("Asset '%s' not found in cache or DB. Starting discovery process...", upperSymbol)
	return m.discoverAndMapAsset(ctx, upperSymbol)
}

// discoverAndMapAsset orchestrates the multi-provider identification process.
func (m *AssetMapper) discoverAndMapAsset(ctx context.Context, commonSymbol string) (*dataprovider.AssetIdentity, error) {
	// --- RESILIENCY CHECK: Ensure essential providers are configured ---
	if m.kraken == nil {
		return nil, errors.New("cannot discover new asset identity: Kraken client is not configured")
	}

	// Step 1: Get the Kraken altname for the BASE asset (e.g., "BTC" -> "XBT").
	baseAltname, err := m.kraken.GetKrakenAssetAltName(ctx, commonSymbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get Kraken altname for base asset %s: %w", commonSymbol, err)
	}

	// Step 2: Determine the preferred QUOTE asset from config and get its altname.
	var quoteCommonSymbol string
	if strings.EqualFold(m.cfg.Kraken.QuoteCurrency, "usd") {
		quoteCommonSymbol = "USD"
	} else if strings.EqualFold(m.cfg.Kraken.QuoteCurrency, "usdt") {
		quoteCommonSymbol = "USDT"
	} else {
		return nil, fmt.Errorf("mapper: unsupported quote_currency '%s' in config", m.cfg.Kraken.QuoteCurrency)
	}

	quoteAltname, err := m.kraken.GetKrakenAssetAltName(ctx, quoteCommonSymbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get Kraken altname for quote asset %s: %w", quoteCommonSymbol, err)
	}
	m.logger.LogDebug("mapper: Preferred quote for %s: %s (from config quote_currency: %s)", commonSymbol, quoteAltname, m.cfg.Kraken.QuoteCurrency)

	// Step 3: Construct the tradeable pair altname (e.g., "XBT" + "USD" -> "XBTUSD").
	krakenPairToQuery := baseAltname + quoteAltname

	// Step 4: Query Kraken with the correctly constructed pair name.
	krakenPairInfoMap, err := m.kraken.GetAssetPairsAPI(ctx, krakenPairToQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query pair %s: %w", krakenPairToQuery, err)
	}

	var targetKrakenPair *kraken.AssetPairInfo
	for _, pairInfo := range krakenPairInfoMap {
		// The API might return info for a slightly different pair name,
		// so we find the one matching our query.
		if pairInfo.Altname == krakenPairToQuery {
			p := pairInfo // Create a new variable for the pointer
			targetKrakenPair = &p
			break
		}
	}

	if targetKrakenPair == nil {
		return nil, fmt.Errorf("kraken did not return data for the expected pair %s", krakenPairToQuery)
	}

	// Step 5: Initialize the identity with Kraken data and prepare for enrichment.
	identity := &dataprovider.AssetIdentity{
		CommonSymbol:    commonSymbol,
		KrakenAsset:     baseAltname,
		KrakenWsName:    targetKrakenPair.WSName,
		LastUpdated:     time.Now(),
		CoinGeckoID:     "N/A", // Default value
		CoinMarketCapID: "N/A", // Default value
		IconPath:        "",    // Default value
	}

	// Step 6: Enrich with CoinGecko data.
	if m.coingecko == nil {
		m.logger.LogInfo("CoinGecko provider not configured. Skipping discovery for '%s'.", commonSymbol)
	} else {
		cgIDs, err := m.coingecko.GetCoinIDsBySymbol(ctx, commonSymbol)
		if err != nil {
			m.logger.LogWarn("mapper: failed to get CoinGecko IDs for '%s', proceeding with partial identity: %v", commonSymbol, err)
		} else {
			cgMarketData, err := m.coingecko.GetMarketData(ctx, cgIDs, "USD")
			if err != nil {
				m.logger.LogWarn("mapper: failed to get market data for CoinGecko IDs [%s], proceeding with partial identity: %v", strings.Join(cgIDs, ","), err)
			} else {
				var candidates []dataprovider.MarketData
				for _, cgCoin := range cgMarketData {
					if strings.EqualFold(cgCoin.Symbol, commonSymbol) && cgCoin.MarketCapRank > 0 && cgCoin.MarketCapRank <= m.cfg.Trading.DynamicAssetScanTopN {
						candidates = append(candidates, cgCoin)
					}
				}

				if len(candidates) == 0 {
					m.logger.LogWarn("mapper: could not find a confident match for '%s' on CoinGecko after cross-referencing.", commonSymbol)
				} else {
					sort.Slice(candidates, func(i, j int) bool {
						return candidates[i].MarketCapRank < candidates[j].MarketCapRank
					})
					matchedCgCoin := candidates[0]
					identity.CoinGeckoID = matchedCgCoin.ID

					// Step 7: Enrich with CoinMarketCap data (dependent on successful CG match).
					var cmcLogoURL string
					if m.coinmarketcap != nil {
						cmcID, logoURL, cmcErr := m.findPreciseCmcID(ctx, commonSymbol, matchedCgCoin.MarketCap)
						if cmcErr != nil {
							m.logger.LogWarn("mapper: could not resolve CoinMarketCap ID for '%s': %v", commonSymbol, cmcErr)
						} else {
							identity.CoinMarketCapID = cmcID
							cmcLogoURL = logoURL
						}
					} else {
						m.logger.LogInfo("mapper: CoinMarketCap provider not configured, skipping ID resolution for '%s'.", commonSymbol)
					}

					// Step 8: Download the icon.
					iconURL := matchedCgCoin.Image
					if iconURL == "" {
						iconURL = cmcLogoURL // Fallback to CMC logo if CG doesn't provide one.
					}
					iconPath, err := m.downloadIcon(ctx, matchedCgCoin.ID, iconURL)
					if err != nil {
						m.logger.LogWarn("mapper: failed to download icon for '%s': %v", commonSymbol, err)
					} else {
						identity.IconPath = iconPath
					}
				}
			}
		}
	}

	// Step 9: Log and save the final, enriched identity.
	m.logger.LogInfo("Successfully discovered identity for '%s' (CG: %s, CMC: %s)", commonSymbol, identity.CoinGeckoID, identity.CoinMarketCapID)

	if err := m.db.SaveAssetIdentity(identity); err != nil {
		m.logger.LogWarn("Could not save newly discovered asset identity for %s to DB: %v", commonSymbol, err)
	}
	m.identityCache.Store(strings.ToUpper(commonSymbol), identity)

	return identity, nil
}

// findPreciseCmcID gets the CMC ID by cross-referencing market cap data to avoid ambiguity.
func (m *AssetMapper) findPreciseCmcID(ctx context.Context, symbol string, cgMarketCap float64) (string, string, error) {
	// Step 1: Fetch all matching IDs for the symbol from CoinMarketCap.
	cmcIDs, err := m.coinmarketcap.GetCoinIDsBySymbol(ctx, symbol)
	if err != nil {
		return "", "", fmt.Errorf("failed to get CMC IDs for symbol %s: %w", symbol, err)
	}
	if len(cmcIDs) == 0 {
		return "", "", fmt.Errorf("no CMC IDs found for symbol %s", symbol)
	}

	// Step 2: Get market data for ALL matching IDs.
	cmcMarketData, err := m.coinmarketcap.GetMarketData(ctx, cmcIDs, "USD")
	if err != nil {
		return "", "", fmt.Errorf("failed to get CMC market data for IDs [%s]: %w", strings.Join(cmcIDs, ","), err)
	}
	if len(cmcMarketData) == 0 {
		return "", "", fmt.Errorf("no market data returned from CMC for IDs [%s]", strings.Join(cmcIDs, ","))
	}

	// Step 3: If there is only one result, assume it's correct. This is the most common case.
	if len(cmcMarketData) == 1 {
		bestMatch := cmcMarketData[0]
		m.logger.LogInfo("Precise match found for %s on CMC: ID %s (Single Result)", symbol, bestMatch.ID)

		// Optionally log a warning if market caps differ significantly, but still accept the match.
		if cgMarketCap > 0 && bestMatch.MarketCap > 0 {
			difference := math.Abs(bestMatch.MarketCap - cgMarketCap)
			relativeDiff := difference / cgMarketCap
			if relativeDiff > 0.15 { // 15% tolerance for warning
				m.logger.LogWarn("Market cap for %s differs between CoinGecko (%.0f) and CoinMarketCap (%.0f) by %.2f%%, but accepting single match.",
					symbol, cgMarketCap, bestMatch.MarketCap, relativeDiff*100)
			}
		}
		return bestMatch.ID, bestMatch.Image, nil
	}

	// Step 4: If multiple results, use market cap comparison as a tie-breaker.
	m.logger.LogWarn("Multiple CMC results for symbol '%s' (%d candidates). Comparing market caps for precision.", symbol, len(cmcMarketData))

	if cgMarketCap == 0 {
		// No CG data to compare against; select the candidate with the highest market cap.
		var maxCap float64 = 0
		var bestMatch *dataprovider.MarketData
		for i := range cmcMarketData {
			data := cmcMarketData[i]
			if data.MarketCap > maxCap {
				maxCap = data.MarketCap
				bestMatch = &data
			}
		}
		if bestMatch == nil {
			return "", "", fmt.Errorf("no market data with positive market cap available for %s", symbol)
		}
		m.logger.LogInfo("Precise match found for %s on CMC: ID %s (Highest Market Cap, no CG reference)", symbol, bestMatch.ID)
		return bestMatch.ID, bestMatch.Image, nil
	}

	// With CG reference: find the candidate with the closest market cap within the tolerance.
	const marketCapTolerance = 0.15 // 15% difference
	var bestMatch *dataprovider.MarketData
	minDifference := math.MaxFloat64

	for i := range cmcMarketData {
		data := cmcMarketData[i]
		if data.MarketCap == 0 {
			continue // Can't compare with a zero market cap
		}
		difference := math.Abs(data.MarketCap - cgMarketCap)
		relativeDiff := difference / cgMarketCap
		if relativeDiff < marketCapTolerance && relativeDiff < minDifference {
			minDifference = relativeDiff
			bestMatch = &data
			m.logger.LogDebug("CMC candidate for %s: ID %s, MarketCap %f, Relative Diff %f", symbol, data.ID, data.MarketCap, relativeDiff)
		}
	}

	if bestMatch == nil {
		return "", "", fmt.Errorf("no precise match found on CMC for %s after comparing market caps", symbol)
	}

	m.logger.LogInfo("Precise match found for %s on CMC: ID %s (Market Cap Match)", symbol, bestMatch.ID)
	return bestMatch.ID, bestMatch.Image, nil
}

// downloadIcon fetches an image from a URL and saves it locally.
func (m *AssetMapper) downloadIcon(ctx context.Context, coinID, imageURL string) (string, error) {
	if imageURL == "" {
		return "", fmt.Errorf("image URL is empty")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", imageURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("bad status fetching icon: %s", resp.Status)
	}
	iconDir := "web/static/icons"
	if err := os.MkdirAll(iconDir, os.ModePerm); err != nil {
		return "", err
	}
	fileExt := filepath.Ext(imageURL)
	if fileExt == "" {
		// Guess extension from content type
		contentType := resp.Header.Get("Content-Type")
		if strings.Contains(contentType, "webp") {
			fileExt = ".webp"
		} else {
			fileExt = ".png" // Default
		}
	}
	fileName := fmt.Sprintf("%s%s", coinID, fileExt)
	filePath := filepath.Join(iconDir, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return "", err
	}
	return filePath, nil
}
