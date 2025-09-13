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
	// --- RESILIENCY CHECK: Ensure essential providers are configured ---
	if m.coingecko == nil {
		return nil, errors.New("cannot discover new asset identity: CoinGecko data provider is not configured")
	}
	if m.kraken == nil {
		return nil, errors.New("cannot discover new asset identity: Kraken client is not configured")
	}
	// --- END CHECK ---

	// Step 1: Get all asset pair details from Kraken (our source of truth)
	krakenPairs, err := m.kraken.GetAssetPairsAPI(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("mapper: failed to get asset pairs from Kraken: %w", err)
	}

	var targetKrakenPair *kraken.AssetPairInfo
	var krakenAssetName string
	for name, pair := range krakenPairs {
		// Use the client's mapping logic to get the common name for the base asset.
		commonBaseName, err := m.kraken.GetCommonAssetName(ctx, pair.Base)
		if err != nil {
			continue // Skip if the base asset cannot be mapped.
		}

		// Check if the common name matches the one we're looking for.
		if strings.EqualFold(commonBaseName, commonSymbol) && (strings.EqualFold(pair.Quote, "ZUSD") || strings.EqualFold(pair.Quote, "USDT")) {
			p := pair // Create a copy
			targetKrakenPair = &p
			krakenAssetName = name
			break
		}
	}

	if targetKrakenPair == nil {
		return nil, fmt.Errorf("mapper: could not find a matching USD/USDT asset pair for '%s' on Kraken", commonSymbol)
	}

	// Step 2: Get all potential CoinGecko IDs for the common symbol.
	cgIDs, err := m.coingecko.GetCoinIDsBySymbol(ctx, commonSymbol)
	if err != nil {
		return nil, fmt.Errorf("mapper: failed to get CoinGecko IDs for '%s': %w", commonSymbol, err)
	}

	// Step 3: Get market data for ALL potential CoinGecko IDs to filter by market cap and volume.
	cgMarketData, err := m.coingecko.GetMarketData(ctx, cgIDs, "USD")
	if err != nil {
		return nil, fmt.Errorf("mapper: failed to get market data for CoinGecko IDs [%s]: %w", strings.Join(cgIDs, ","), err)
	}

	// Step 4: Find the best match by cross-referencing with Kraken data and market cap rank.
	var matchedCgCoin *dataprovider.MarketData
	for _, cgCoin := range cgMarketData {
		// Check for a symbol match, a valid market cap rank, and that the rank is within our configured top N.
		if strings.EqualFold(cgCoin.Symbol, commonSymbol) && cgCoin.MarketCapRank > 0 && cgCoin.MarketCapRank <= m.cfg.Trading.DynamicAssetScanTopN {
			matchedCgCoin = &cgCoin
			break
		}
	}

	if matchedCgCoin == nil {
		return nil, fmt.Errorf("mapper: could not find a confident match for '%s' on CoinGecko after cross-referencing", commonSymbol)
	}

	// Step 3: Use the verified CoinGecko data to find the precise CoinMarketCap ID.
	var cmcID, cmcLogoURL string
	// --- RESILIENCY CHECK: Only search CMC if it's configured ---
	if m.coinmarketcap != nil {
		var cmcErr error
		cmcID, cmcLogoURL, cmcErr = m.findPreciseCmcID(ctx, commonSymbol, matchedCgCoin.MarketCap)
		if cmcErr != nil {
			m.logger.LogWarn("mapper: could not resolve CoinMarketCap ID for '%s': %v", commonSymbol, cmcErr)
			cmcID = "N/A"
		}
	} else {
		m.logger.LogInfo("mapper: CoinMarketCap provider not configured, skipping ID resolution for '%s'.", commonSymbol)
		cmcID = "N/A"
	}
	// --- END CHECK ---

	// Step 4: Download the icon
	iconURL := matchedCgCoin.Image
	if iconURL == "" {
		iconURL = cmcLogoURL
	}
	iconPath, err := m.downloadIcon(ctx, matchedCgCoin.ID, iconURL)
	if err != nil {
		m.logger.LogWarn("mapper: failed to download icon for '%s': %v", commonSymbol, err)
		iconPath = ""
	}

	// Step 5: Assemble the complete identity object
	newIdentity := &dataprovider.AssetIdentity{
		CommonSymbol:    commonSymbol,
		KrakenAsset:     krakenAssetName,
		KrakenWsName:    targetKrakenPair.WSName,
		CoinGeckoID:     matchedCgCoin.ID,
		CoinMarketCapID: cmcID,
		IconPath:        iconPath,
		LastUpdated:     time.Now(),
	}

	// Step 6: Save to the database
	if err := m.db.SaveAssetIdentity(newIdentity); err != nil {
		return nil, fmt.Errorf("mapper: failed to save asset identity for '%s': %w", commonSymbol, err)
	}

	m.identityCache.Store(commonSymbol, newIdentity)
	m.logger.LogInfo("Successfully discovered and mapped new asset: %s (CG: %s, CMC: %s)", commonSymbol, newIdentity.CoinGeckoID, newIdentity.CoinMarketCapID)
	return newIdentity, nil
}

// findPreciseCmcID gets the CMC ID by cross-referencing market cap data to avoid ambiguity.
func (m *AssetMapper) findPreciseCmcID(ctx context.Context, symbol string, cgMarketCap float64) (string, string, error) {
	// Step 1: Use the CMC client's own GetCoinID method to resolve the numerical ID.
	// This will correctly handle the symbol_overrides from the config.
	cmcID, err := m.coinmarketcap.GetCoinID(ctx, symbol)
	if err != nil {
		return "", "", fmt.Errorf("failed to get CMC numerical ID for symbol %s: %w", symbol, err)
	}

	// Step 2: Call GetMarketData with the correct numerical ID.
	cmcMarketData, err := m.coinmarketcap.GetMarketData(ctx, []string{cmcID}, "USD")
	if err != nil {
		return "", "", fmt.Errorf("failed to get CMC market data for ID %s: %w", cmcID, err)
	}
	if len(cmcMarketData) == 0 {
		return "", "", fmt.Errorf("no market data returned from CMC for ID %s", cmcID)
	}

	// The rest of the function remains the same, as the data returned is now correct.
	if len(cmcMarketData) == 1 {
		return cmcMarketData[0].ID, cmcMarketData[0].Image, nil
	}

	m.logger.LogWarn("Multiple CMC results for symbol '%s'. Comparing market caps for precision.", symbol)
	const marketCapTolerance = 0.15 // 15% difference

	for _, data := range cmcMarketData {
		if cgMarketCap == 0 || data.MarketCap == 0 {
			continue // Avoid division by zero if a provider has no market cap data
		}
		difference := math.Abs(data.MarketCap - cgMarketCap)
		if (difference / cgMarketCap) < marketCapTolerance {
			m.logger.LogInfo("Precise match found for %s on CMC: ID %s (Market Cap Match)", symbol, data.ID)
			return data.ID, data.Image, nil
		}
	}

	return "", "", fmt.Errorf("no precise match found on CMC for %s after comparing market caps", symbol)
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
