// File: dataprovider/db.go
package dataprovider

import (
	"Snowballin/utilities"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"path/filepath"
	"time"
)

type SQLiteCache struct {
	db *sql.DB
}

func NewSQLiteCache(dbPath utilities.DatabaseConfig) (*SQLiteCache, error) {
	dir := filepath.Dir(dbPath.DBPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create database directory '%s': %w", dir, err)
	}
	db, err := sql.Open("sqlite3", dbPath.DBPath)
	if err != nil {
		return nil, err
	}
	return &SQLiteCache{db: db}, nil
}

func (s *SQLiteCache) InitSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS ohlcv_bars (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		provider TEXT NOT NULL,
		coin_id TEXT NOT NULL,
		timestamp INTEGER NOT NULL,
		open REAL NOT NULL,
		high REAL NOT NULL,
		low REAL NOT NULL,
		close REAL NOT NULL,
		volume REAL NOT NULL,
		UNIQUE(provider, coin_id, timestamp)
	);
	CREATE INDEX IF NOT EXISTS idx_provider_coin_timestamp ON ohlcv_bars (provider, coin_id, timestamp);

	CREATE TABLE IF NOT EXISTS open_positions (
		asset_pair TEXT PRIMARY KEY,
		entry_timestamp INTEGER NOT NULL,
		average_price REAL NOT NULL,
		total_volume REAL NOT NULL,
		base_order_price REAL NOT NULL,
		filled_safety_orders INTEGER NOT NULL,
		is_dca_active INTEGER NOT NULL,
		base_order_size REAL NOT NULL,
		current_take_profit REAL NOT NULL,
		broker_order_id TEXT NOT NULL,
		is_trailing_active INTEGER NOT NULL,
		peak_price_since_tp REAL NOT NULL
	);

	CREATE TABLE IF NOT EXISTS pending_orders (
		broker_order_id TEXT PRIMARY KEY,
		asset_pair TEXT NOT NULL
	);
	`
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("failed to execute database schema: %w", err)
	}
	log.Println("Database schema initialized successfully.")
	return nil
}

func (s *SQLiteCache) SaveBar(provider, coinID string, bar utilities.OHLCVBar) error {
	_, err := s.db.Exec(`INSERT OR REPLACE INTO ohlcv_bars (provider, coin_id, timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		provider, coinID, bar.Timestamp, bar.Open, bar.High, bar.Low, bar.Close, bar.Volume)
	return err
}

func (s *SQLiteCache) GetBars(provider, coinID string, start, end int64) ([]utilities.OHLCVBar, error) {
	rows, err := s.db.Query(`SELECT timestamp, open, high, low, close, volume FROM ohlcv_bars WHERE provider=? AND coin_id=? AND timestamp BETWEEN ? AND ? ORDER BY timestamp ASC`,
		provider, coinID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var bars []utilities.OHLCVBar
	for rows.Next() {
		var bar utilities.OHLCVBar
		if err := rows.Scan(&bar.Timestamp, &bar.Open, &bar.High, &bar.Low, &bar.Close, &bar.Volume); err != nil {
			return nil, err
		}
		bars = append(bars, bar)
	}
	return bars, nil
}

func (s *SQLiteCache) LoadPositions() (map[string]*utilities.Position, error) {
	query := `SELECT asset_pair, entry_timestamp, average_price, total_volume, base_order_price, filled_safety_orders, is_dca_active, base_order_size, current_take_profit, broker_order_id, is_trailing_active, peak_price_since_tp FROM open_positions`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query open positions: %w", err)
	}
	defer rows.Close()

	positions := make(map[string]*utilities.Position)
	for rows.Next() {
		var pos utilities.Position
		var ts int64
		var isDcaActiveInt int
		var isTrailingActiveInt int

		err := rows.Scan(
			&pos.AssetPair,
			&ts,
			&pos.AveragePrice,
			&pos.TotalVolume,
			&pos.BaseOrderPrice,
			&pos.FilledSafetyOrders,
			&isDcaActiveInt,
			&pos.BaseOrderSize,
			&pos.CurrentTakeProfit,
			&pos.BrokerOrderID,
			&isTrailingActiveInt,
			&pos.PeakPriceSinceTP,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan position row: %w", err)
		}
		pos.EntryTimestamp = time.Unix(ts, 0)
		pos.IsDcaActive = isDcaActiveInt == 1
		pos.IsTrailingActive = isTrailingActiveInt == 1
		positions[pos.AssetPair] = &pos
	}
	return positions, nil
}

// --- MODIFIED: The entire SavePosition function is corrected ---
func (s *SQLiteCache) SavePosition(pos *utilities.Position) error {
	query := `INSERT OR REPLACE INTO open_positions (asset_pair, entry_timestamp, average_price, total_volume, base_order_price, filled_safety_orders, is_dca_active, base_order_size, current_take_profit, broker_order_id, is_trailing_active, peak_price_since_tp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	isDcaActiveInt := 0
	if pos.IsDcaActive {
		isDcaActiveInt = 1
	}
	isTrailingActiveInt := 0
	if pos.IsTrailingActive {
		isTrailingActiveInt = 1
	}

	_, err := s.db.Exec(query,
		pos.AssetPair,
		pos.EntryTimestamp.Unix(),
		pos.AveragePrice,
		pos.TotalVolume,
		pos.BaseOrderPrice,
		pos.FilledSafetyOrders,
		isDcaActiveInt,
		pos.BaseOrderSize,
		pos.CurrentTakeProfit,
		pos.BrokerOrderID,
		isTrailingActiveInt,
		pos.PeakPriceSinceTP,
	) // Corrected: removed misplaced '}' and added ')'
	return err
}

func (s *SQLiteCache) DeletePosition(assetPair string) error {
	_, err := s.db.Exec(`DELETE FROM open_positions WHERE asset_pair = ?`, assetPair)
	return err
}

func (s *SQLiteCache) LoadPendingOrders() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT broker_order_id, asset_pair FROM pending_orders`)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending orders: %w", err)
	}
	defer rows.Close()

	orders := make(map[string]string)
	for rows.Next() {
		var orderID, assetPair string
		if err := rows.Scan(&orderID, &assetPair); err != nil {
			return nil, fmt.Errorf("failed to scan pending order row: %w", err)
		}
		orders[orderID] = assetPair
	}
	return orders, nil
}

func (s *SQLiteCache) SavePendingOrder(orderID, assetPair string) error {
	_, err := s.db.Exec(`INSERT OR REPLACE INTO pending_orders (broker_order_id, asset_pair) VALUES (?, ?)`, orderID, assetPair)
	return err
}

func (s *SQLiteCache) DeletePendingOrder(orderID string) error {
	_, err := s.db.Exec(`DELETE FROM pending_orders WHERE broker_order_id = ?`, orderID)
	return err
}

func (s *SQLiteCache) CleanupOldBars(provider string, olderThan time.Time) error {
	cutoff := olderThan.UnixMilli()
	_, err := s.db.Exec(`DELETE FROM ohlcv_bars WHERE provider=? AND timestamp < ?`, provider, cutoff)
	return err
}

func (s *SQLiteCache) Close() error {
	return s.db.Close()
}

func (s *SQLiteCache) StartScheduledCleanup(interval time.Duration, provider string) {
	go func() {
		time.Sleep(2 * time.Minute)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cutoff := time.Now().AddDate(0, 0, -180)
				if err := s.CleanupOldBars(provider, cutoff); err != nil {
					log.Printf("Scheduled cleanup error for %s: %v", provider, err)
				} else {
					log.Printf("Scheduled cleanup successful for %s", provider)
				}
			}
		}
	}()
}
