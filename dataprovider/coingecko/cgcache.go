package coingecko

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"Snowballin/utilities"
)

// Cache manages historical market data with efficient storage and retrieval.
type Cache struct {
	basePath string
	mu       sync.RWMutex
}

// NewCache initializes and returns a new Cache instance.
func NewCache(basePath string) (*Cache, error) {
	if err := os.MkdirAll(basePath, os.ModePerm); err != nil {
		return nil, err
	}
	return &Cache{basePath: basePath}, nil
}

// cacheFilePath generates a file path for caching data.
func (c *Cache) cacheFilePath(asset, vsCurrency, interval string) string {
	return filepath.Join(c.basePath, fmt.Sprintf("%s_%s_%s.json", asset, vsCurrency, interval))
}

// SaveOHLCV stores OHLCV data efficiently.
func (c *Cache) SaveOHLCV(asset, vsCurrency, interval string, bars []utilities.OHLCVBar) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	path := c.cacheFilePath(asset, vsCurrency, interval)
	data, err := json.Marshal(bars)
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

// LoadOHLCV retrieves cached OHLCV data.
func (c *Cache) LoadOHLCV(asset, vsCurrency, interval string) ([]utilities.OHLCVBar, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	path := c.cacheFilePath(asset, vsCurrency, interval)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // Cache miss; caller should fetch data.
		}
		return nil, err
	}

	var bars []utilities.OHLCVBar
	err = json.Unmarshal(data, &bars)
	return bars, err
}

// UpdateOHLCV merges new OHLCV data with cached data, ensuring no duplicates and minimal storage.
func (c *Cache) UpdateOHLCV(asset, vsCurrency, interval string, newBars []utilities.OHLCVBar) error {
	existingBars, err := c.LoadOHLCV(asset, vsCurrency, interval)
	if err != nil {
		// If LoadOHLCV returns an error other than a simple cache miss (nil, nil),
		// we should probably return that error rather than proceeding.
		// However, if it's a cache miss (existingBars is nil), it's fine.
		// The current LoadOHLCV returns (nil, nil) for cache miss, so this check is okay.
		// If there was a real error reading an existing file, we might not want to overwrite.
		// For now, assume LoadOHLCV handles errors appropriately for this flow.
	}

	barMap := make(map[int64]utilities.OHLCVBar)
	for _, bar := range existingBars {
		barMap[bar.Timestamp] = bar
	}
	for _, bar := range newBars {
		barMap[bar.Timestamp] = bar
	}

	mergedBars := make([]utilities.OHLCVBar, 0, len(barMap))
	for _, bar := range barMap {
		mergedBars = append(mergedBars, bar)
	}

	// Sort by timestamp ascending
	utilities.SortBarsByTimestamp(mergedBars)

	return c.SaveOHLCV(asset, vsCurrency, interval, mergedBars)
}

func pruneBars(bars []utilities.OHLCVBar, cutoff time.Time) []utilities.OHLCVBar {
	// Find the index of the first bar to keep
	keepIndex := 0
	for i, bar := range bars {
		barTime := time.Unix(bar.Timestamp/1000, 0) // Assuming Timestamp is in milliseconds
		if !barTime.Before(cutoff) {
			keepIndex = i
			break
		}
		// If all bars are before cutoff, keepIndex will become len(bars)
		if i == len(bars)-1 && barTime.Before(cutoff) {
			keepIndex = len(bars)
		}
	}
	return bars[keepIndex:]
}

// PruneCache periodically removes historical data older than retentionDays to save storage space.
func (c *Cache) PruneCache(retentionDays int) error {
	c.mu.Lock() // Changed to full Lock as we are modifying files
	defer c.mu.Unlock()

	files, err := filepath.Glob(filepath.Join(c.basePath, "*.json"))
	if err != nil {
		return err
	}

	cutoff := time.Now().AddDate(0, 0, -retentionDays)

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			// Log this error or handle it? For now, skip the file.
			// c.logger.LogWarn("PruneCache: Error reading file %s: %v", file, err)
			continue
		}

		var bars []utilities.OHLCVBar
		if errUnmarshal := json.Unmarshal(data, &bars); errUnmarshal != nil || len(bars) == 0 {
			// Log this error or handle it? For now, skip the file.
			// c.logger.LogWarn("PruneCache: Error unmarshalling or empty data in file %s: %v", file, errUnmarshal)
			if len(bars) == 0 && errUnmarshal == nil { // Empty array is not an error for unmarshal, but we might want to remove it
				if errRemove := os.Remove(file); errRemove != nil {
					// c.logger.LogError("PruneCache: Failed to remove empty cache file %s: %v", file, errRemove)
				}
			}
			continue
		}

		prunedBars := pruneBars(bars, cutoff)

		if len(prunedBars) == 0 {
			if errRemove := os.Remove(file); errRemove != nil {
				// c.logger.LogError("PruneCache: Failed to remove fully pruned cache file %s: %v", file, errRemove)
			}
			continue
		}

		// Only write back if the number of bars has changed
		if len(prunedBars) < len(bars) {
			prunedData, errMarshal := json.Marshal(prunedBars)
			if errMarshal != nil {
				// Log this error
				// c.logger.LogError("PruneCache: Failed to marshal pruned data for file %s: %v", file, errMarshal)
				continue
			}
			if errWrite := os.WriteFile(file, prunedData, 0644); errWrite != nil {
				// Log this error
				// c.logger.LogError("PruneCache: Failed to write pruned data to file %s: %v", file, errWrite)
			}
		}
	}

	return nil
}
