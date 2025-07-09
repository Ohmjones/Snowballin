package coinmarketcap

import (
	"Snowballin/utilities"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Cache manages historical market data with efficient storage and retrieval for CoinMarketCap.
type Cache struct {
	basePath string
	mu       sync.RWMutex
	logger   *utilities.Logger
}

// NewCache initializes and returns a new Cache instance for CoinMarketCap data.
func NewCache(basePath string, logger *utilities.Logger) (*Cache, error) {
	if basePath == "" {
		return nil, fmt.Errorf("coinmarketcap cache: base path cannot be empty")
	}
	if logger == nil {
		// This case should ideally be prevented by the caller (e.g., NewClient)
		// If a logger must be created here, use a default one.
		// However, for a library component, it's better to enforce injection.
		return nil, fmt.Errorf("coinmarketcap cache: logger cannot be nil")
	}
	if err := os.MkdirAll(basePath, os.ModePerm); err != nil {
		logger.LogError("CoinMarketCap Cache: Failed to create cache directory '%s': %v", basePath, err)
		return nil, fmt.Errorf("coinmarketcap cache: failed to create directory '%s': %w", basePath, err)
	}
	logger.LogInfo("CoinMarketCap Cache: Initialized OHLCV cache at: %s", basePath)
	return &Cache{basePath: basePath, logger: logger}, nil
}

// saveOHLCVNoLock saves OHLCV without acquiring the mutex. Assumes the caller is holding the lock.
func (c *Cache) saveOHLCVNoLock(assetID, vsCurrency, interval string, bars []utilities.OHLCVBar) error {
	path := c.cacheFilePath(assetID, vsCurrency, interval)
	data, err := json.Marshal(bars)
	if err != nil {
		c.logger.LogError("CoinMarketCap Cache: Failed to marshal OHLCV data for %s/%s (interval: %s): %v", assetID, vsCurrency, interval, err)
		return fmt.Errorf("failed to marshal cmc ohlcv data: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		c.logger.LogError("CoinMarketCap Cache: Failed to write OHLCV data to cache file %s: %v", path, err)
		return fmt.Errorf("failed to write cmc ohlcv data to cache file %s: %w", path, err)
	}
	c.logger.LogDebug("CoinMarketCap Cache: Successfully saved %d bars to %s", len(bars), path)
	return nil
}

// loadOHLCVNoLock loads OHLCV without acquiring the mutex. Assumes the caller is holding the lock. Returns nil, nil if cache miss.
func (c *Cache) loadOHLCVNoLock(assetID, vsCurrency, interval string) ([]utilities.OHLCVBar, error) {
	path := c.cacheFilePath(assetID, vsCurrency, interval)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			c.logger.LogDebug("CoinMarketCap Cache: Cache miss for %s/%s (interval: %s) at path %s", assetID, vsCurrency, interval, path)
			return nil, nil // Cache miss
		}
		c.logger.LogError("CoinMarketCap Cache: Failed to read OHLCV data from cache file %s: %v", path, err)
		return nil, fmt.Errorf("failed to read cmc ohlcv data from cache file %s: %w", path, err)
	}

	if len(data) == 0 { // Handle empty file case explicitly
		c.logger.LogWarn("CoinMarketCap Cache: Cache file %s is empty. Treating as cache miss.", path)
		return nil, nil // Treat empty file as a cache miss
	}

	var bars []utilities.OHLCVBar
	if err := json.Unmarshal(data, &bars); err != nil {
		c.logger.LogError("CoinMarketCap Cache: Failed to unmarshal OHLCV data from cache file %s: %v", path, err)
		return nil, fmt.Errorf("failed to unmarshal cmc ohlcv data from cache file %s: %w", path, err)
	}
	c.logger.LogDebug("CoinMarketCap Cache: Successfully loaded %d bars from %s", len(bars), path)
	return bars, nil
}

// pruneCacheNoLock prunes the cache without holding the mutex. Assumes the caller is holding the lock.
func (c *Cache) pruneCacheNoLock(retentionDays int) error {
	if retentionDays <= 0 {
		c.logger.LogInfo("CoinMarketCap Cache: Pruning skipped, retentionDays (%d) is not positive.", retentionDays)
		return nil
	}

	files, err := filepath.Glob(filepath.Join(c.basePath, "cmc_*.json"))
	if err != nil {
		c.logger.LogError("CoinMarketCap Cache: Failed to glob cache files for pruning: %v", err)
		return fmt.Errorf("failed to glob cmc cache files for pruning: %w", err)
	}

	cutoffTime := time.Now().AddDate(0, 0, -retentionDays)
	cutoffUnix := cutoffTime.Unix()

	c.logger.LogInfo("CoinMarketCap Cache: Pruning entries older than %s (Unix: %d) with %d retention days.", cutoffTime.Format(time.RFC3339), cutoffUnix, retentionDays)
	prunedCount := 0
	removedFileCount := 0

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			c.logger.LogWarn("CoinMarketCap Cache: Failed to read file %s for pruning, skipping: %v", file, err)
			continue
		}

		if len(data) == 0 {
			c.logger.LogInfo("CoinMarketCap Cache: File %s is empty, removing.", file)
			if err := os.Remove(file); err != nil {
				c.logger.LogWarn("CoinMarketCap Cache: Failed to remove empty cache file %s: %v", file, err)
			} else {
				removedFileCount++
			}
			continue
		}

		var bars []utilities.OHLCVBar
		if errUnmarshal := json.Unmarshal(data, &bars); errUnmarshal != nil {
			c.logger.LogWarn("CoinMarketCap Cache: Failed to unmarshal data in cache file %s, considering removal: %v", file, errUnmarshal)
			// Optionally remove malformed files
			// if err := os.Remove(file); err != nil {
			// 	c.logger.LogWarn("CoinMarketCap Cache: Failed to remove malformed cache file %s: %v", file, err)
			// }
			continue
		}
		if len(bars) == 0 { // Already unmarshaled, and it's an empty list of bars
			c.logger.LogInfo("CoinMarketCap Cache: File %s contains an empty list of bars, removing.", file)
			if err := os.Remove(file); err != nil {
				c.logger.LogWarn("CoinMarketCap Cache: Failed to remove empty list cache file %s: %v", file, err)
			} else {
				removedFileCount++
			}
			continue
		}

		var prunedBars []utilities.OHLCVBar
		for _, bar := range bars {
			if bar.Timestamp >= cutoffUnix {
				prunedBars = append(prunedBars, bar)
			}
		}

		if len(prunedBars) == 0 {
			c.logger.LogInfo("CoinMarketCap Cache: All bars in cache file %s are older than retention period, removing file.", file)
			if err := os.Remove(file); err != nil {
				c.logger.LogWarn("CoinMarketCap Cache: Failed to remove outdated cache file %s: %v", file, err)
			} else {
				removedFileCount++
			}
			continue
		}

		if len(prunedBars) < len(bars) {
			prunedData, errMarshal := json.Marshal(prunedBars)
			if errMarshal != nil {
				c.logger.LogError("CoinMarketCap Cache: Failed to marshal pruned OHLCV data for %s: %v", file, errMarshal)
				continue
			}
			if errWrite := os.WriteFile(file, prunedData, 0644); errWrite != nil {
				c.logger.LogError("CoinMarketCap Cache: Failed to write pruned OHLCV data to %s: %v", file, errWrite)
			} else {
				c.logger.LogDebug("CoinMarketCap Cache: Pruned cache file %s: %d bars removed, %d bars remaining.", file, len(bars)-len(prunedBars), len(prunedBars))
				prunedCount++
			}
		}
	}
	c.logger.LogInfo("CoinMarketCap Cache: Pruning completed. %d files affected (bars pruned), %d files removed.", prunedCount, removedFileCount)
	return nil
}

// cacheFilePath generates a file path for caching CoinMarketCap data.
// Uses Base to be more restrictive against path traversal.
func (c *Cache) cacheFilePath(assetID, vsCurrency, interval string) string {
	safeAssetID := filepath.Clean(filepath.Base(assetID))
	safeVsCurrency := filepath.Clean(filepath.Base(vsCurrency))
	safeInterval := filepath.Clean(filepath.Base(interval))

	filename := fmt.Sprintf("cmc_%s_%s_%s.json", safeAssetID, safeVsCurrency, safeInterval)
	return filepath.Join(c.basePath, filename)
}

// SaveOHLCV stores OHLCV data efficiently.
func (c *Cache) SaveOHLCV(assetID, vsCurrency, interval string, bars []utilities.OHLCVBar) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saveOHLCVNoLock(assetID, vsCurrency, interval, bars)
}

// LoadOHLCV retrieves cached OHLCV data. Returns nil, nil if cache miss.
func (c *Cache) LoadOHLCV(assetID, vsCurrency, interval string) ([]utilities.OHLCVBar, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.loadOHLCVNoLock(assetID, vsCurrency, interval)
}

// UpdateOHLCV merges new OHLCV data with cached data, ensuring no duplicates.
func (c *Cache) UpdateOHLCV(assetID, vsCurrency, interval string, newBars []utilities.OHLCVBar) error {
	if len(newBars) == 0 {
		c.logger.LogDebug("CoinMarketCap Cache Update: No new bars provided for %s/%s (interval: %s). Nothing to do.", assetID, vsCurrency, interval)
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	existingBars, err := c.loadOHLCVNoLock(assetID, vsCurrency, interval)
	if err != nil {
		c.logger.LogWarn("CoinMarketCap Cache Update: Error loading existing OHLCV for update (%s/%s, interval: %s): %v. Will attempt to save new bars as fresh.", assetID, vsCurrency, interval, err)
		existingBars = []utilities.OHLCVBar{} // Treat as empty if load failed but still try to save new bars
	}

	barMap := make(map[int64]utilities.OHLCVBar, len(existingBars)+len(newBars))
	for _, bar := range existingBars {
		if bar.Timestamp != 0 {
			barMap[bar.Timestamp] = bar
		}
	}

	addedCount := 0
	updatedCount := 0
	for _, bar := range newBars {
		if bar.Timestamp != 0 {
			if _, exists := barMap[bar.Timestamp]; !exists {
				addedCount++
			} else {
				updatedCount++
			}
			barMap[bar.Timestamp] = bar
		}
	}

	if len(barMap) == 0 {
		c.logger.LogWarn("CoinMarketCap Cache Update: Resulted in zero valid bars for %s/%s (interval: %s), not saving.", assetID, vsCurrency, interval)
		// Consider if an empty cache file should be removed if it exists
		// path := c.cacheFilePath(assetID, vsCurrency, interval)
		// if _, statErr := os.Stat(path); statErr == nil { os.Remove(path) }
		return nil
	}

	mergedBars := make([]utilities.OHLCVBar, 0, len(barMap))
	for _, bar := range barMap {
		mergedBars = append(mergedBars, bar)
	}

	utilities.SortBarsByTimestamp(mergedBars)

	saveErr := c.saveOHLCVNoLock(assetID, vsCurrency, interval, mergedBars)
	if saveErr == nil {
		c.logger.LogInfo("CoinMarketCap Cache Update: Updated for %s/%s (interval: %s): %d added, %d updated/overwritten, %d total bars.", assetID, vsCurrency, interval, addedCount, updatedCount, len(mergedBars))
	}
	// Error is already logged by saveOHLCVNoLock
	return saveErr
}

// PruneCache is the public method that wraps pruneCacheNoLock with a mutex.
func (c *Cache) PruneCache(retentionDays int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger.LogInfo("CoinMarketCap Cache: Starting cache pruning process with retention of %d days.", retentionDays)
	return c.pruneCacheNoLock(retentionDays)
}
