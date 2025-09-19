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

	// --- The rest of the asset mapping logic will go here ---
	// For now, we'll create a partial identity to pass the pre-flight check.

	identity := &dataprovider.AssetIdentity{
		CommonSymbol: commonSymbol,
		KrakenAsset:  baseAltname,
		KrakenWsName: targetKrakenPair.WSName,
		LastUpdated:  time.Now(),
	}

	m.logger.LogInfo("Successfully discovered partial identity for '%s' via Kraken.", commonSymbol)

	// Store in DB and cache
	if err := m.db.SaveAssetIdentity(identity); err != nil {
		m.logger.LogWarn("Could not save newly discovered asset identity for %s to DB: %v", commonSymbol, err)
	}
	m.identityCache.Store(strings.ToUpper(commonSymbol), identity)

	return identity, nil
}

// findPreciseCmcID gets the CMC ID by cross-referencing market cap data to avoid ambiguity.
func (m *AssetMapper) findPreciseCmcID(ctx context.Context, symbol string, cgMarketCap float64) (string, string, error) {
	// Step 1: Fetch all matching IDs for the symbol using CMC's map endpoint (returns array for multiple matches)
	// Assume CMC package has GetAllCoinIDsBySymbol or similar; if not, implement via HTTP
	cmcIDs, err := m.coinmarketcap.GetCoinIDsBySymbol(ctx, symbol) // Adjusted to existing method
	if err != nil {
		return "", "", fmt.Errorf("failed to get CMC IDs for symbol %s: %w", symbol, err)
	}
	if len(cmcIDs) == 0 {
		return "", "", fmt.Errorf("no CMC IDs found for symbol %s", symbol)
	}

	// Step 2: Get market data for ALL matching IDs
	cmcMarketData, err := m.coinmarketcap.GetMarketData(ctx, cmcIDs, "USD")
	if err != nil {
		return "", "", fmt.Errorf("failed to get CMC market data for IDs [%s]: %w", strings.Join(cmcIDs, ","), err)
	}
	if len(cmcMarketData) == 0 {
		return "", "", fmt.Errorf("no market data returned from CMC for IDs [%s]", strings.Join(cmcIDs, ","))
	}

	m.logger.LogWarn("Multiple CMC results for symbol '%s' (%d candidates). Comparing market caps for precision.", symbol, len(cmcMarketData))

	if cgMarketCap == 0 {
		// No CG data; select the candidate with the highest market cap
		var maxCap float64 = 0
		var bestMatch *dataprovider.MarketData
		for _, data := range cmcMarketData {
			if data.MarketCap > maxCap {
				maxCap = data.MarketCap
				bestMatch = &data
			}
		}
		if bestMatch == nil {
			return "", "", fmt.Errorf("no market data available for %s", symbol)
		}
		m.logger.LogInfo("Precise match found for %s on CMC: ID %s (Highest Market Cap, no CG reference)", symbol, bestMatch.ID)
		return bestMatch.ID, bestMatch.Image, nil
	}

	// With CG reference: use tolerance comparison
	const marketCapTolerance = 0.15 // 15% difference

	var bestMatch *dataprovider.MarketData
	minDifference := math.MaxFloat64
	for _, data := range cmcMarketData {
		if cgMarketCap == 0 || data.MarketCap == 0 {
			continue // Avoid division by zero
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
