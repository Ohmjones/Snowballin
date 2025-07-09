// File: dataprovider/db.go
package dataprovider

import (
	"Snowballin/utilities"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"time"
)

type SQLiteCache struct {
	db *sql.DB
}

func NewSQLiteCache(dbPath utilities.DatabaseConfig) (*SQLiteCache, error) {
	db, err := sql.Open("sqlite3", dbPath.DBPath)
	if err != nil {
		return nil, err
	}
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
		entry_price REAL NOT NULL,
		volume REAL NOT NULL,
		entry_timestamp INTEGER NOT NULL,
		current_stop_loss REAL NOT NULL,
		current_take_profit REAL NOT NULL,
		broker_order_id TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS pending_orders (
		broker_order_id TEXT PRIMARY KEY,
		asset_pair TEXT NOT NULL
	);
	`
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return &SQLiteCache{db: db}, nil
}

// --- OHLCV Bar Caching ---
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

// --- State Persistence Functions ---

func (s *SQLiteCache) LoadPositions() (map[string]*utilities.Position, error) {
	rows, err := s.db.Query(`SELECT asset_pair, entry_price, volume, entry_timestamp, current_stop_loss, current_take_profit, broker_order_id FROM open_positions`)
	if err != nil {
		return nil, fmt.Errorf("failed to query open positions: %w", err)
	}
	defer rows.Close()

	positions := make(map[string]*utilities.Position)
	for rows.Next() {
		var pos utilities.Position
		var ts int64
		if err := rows.Scan(&pos.AssetPair, &pos.EntryPrice, &pos.Volume, &ts, &pos.CurrentStopLoss, &pos.CurrentTakeProfit, &pos.BrokerOrderID); err != nil {
			return nil, fmt.Errorf("failed to scan position row: %w", err)
		}
		pos.EntryTimestamp = time.Unix(ts, 0)
		positions[pos.AssetPair] = &pos
	}
	return positions, nil
}

func (s *SQLiteCache) SavePosition(pos *utilities.Position) error {
	_, err := s.db.Exec(`INSERT OR REPLACE INTO open_positions (asset_pair, entry_price, volume, entry_timestamp, current_stop_loss, current_take_profit, broker_order_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		pos.AssetPair, pos.EntryPrice, pos.Volume, pos.EntryTimestamp.Unix(), pos.CurrentStopLoss, pos.CurrentTakeProfit, pos.BrokerOrderID)
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

// --- Cleanup ---
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
		for {
			cutoff := time.Now().AddDate(0, 0, -14)
			if err := s.CleanupOldBars(provider, cutoff); err != nil {
				log.Printf("Scheduled cleanup error for %s: %v", provider, err)
			} else {
				log.Printf("Scheduled cleanup successful for %s", provider)
			}
			time.Sleep(interval)
		}
	}()
}
