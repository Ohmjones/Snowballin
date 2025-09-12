package dataprovider

import (
	"Snowballin/pkg/broker"
	"Snowballin/utilities"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
	"os"
)

// SQLiteCache now includes the application's logger for consistent logging.
type SQLiteCache struct {
	db     *sql.DB
	logger *utilities.Logger
	mu     sync.RWMutex // Upgraded for concurrent reads
}

// NewSQLiteCache now accepts a logger and uses it for all output.
func NewSQLiteCache(cfg utilities.DatabaseConfig, logger *utilities.Logger) (*SQLiteCache, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	dir := filepath.Dir(cfg.DBPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		logger.LogError("Failed to create database directory '%s': %v", dir, err)
		return nil, fmt.Errorf("failed to create database directory '%s': %w", dir, err)
	}
	logger.LogDebug("Database directory '%s' exists or was created.", dir)

	absPath, _ := filepath.Abs(cfg.DBPath)
	logger.LogInfo("Initializing SQLite database at: %s", absPath)

	// Open the database connection. The file will be created if it doesn't exist.
	db, err := sql.Open("sqlite3", cfg.DBPath+"?_journal_mode=WAL")
	if err != nil {
		logger.LogError("Failed to open sqlite database at %s: %v", absPath, err)
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Ping the database to verify the connection.
	if err := db.Ping(); err != nil {
		logger.LogError("Failed to connect to the database: %v", err)
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Set busy_timeout to prevent indefinite hangs on locks
	if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		logger.LogError("Failed to set SQLite busy_timeout: %v", err)
		return nil, fmt.Errorf("failed to set busy_timeout: %w", err)
	}

	return &SQLiteCache{db: db, logger: logger}, nil
}

// InitSchema creates the database tables if they do not already exist.
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

	CREATE TABLE IF NOT EXISTS cached_trades (
        asset_pair TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        data BLOB NOT NULL, -- JSON array of Trade structs
        PRIMARY KEY (asset_pair)
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

	CREATE TABLE IF NOT EXISTS cached_fees (
		pair TEXT PRIMARY KEY,
		maker_fee REAL NOT NULL,
		taker_fee REAL NOT NULL,
		timestamp INTEGER NOT NULL
	);
	
	CREATE TABLE IF NOT EXISTS cached_tickers (
		pair TEXT PRIMARY KEY,
		data BLOB NOT NULL, -- JSON blob of TickerInfo
		timestamp INTEGER NOT NULL
	);

	CREATE TABLE IF NOT EXISTS asset_identities (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		common_symbol TEXT NOT NULL UNIQUE,
		kraken_asset TEXT NOT NULL,
		kraken_ws_name TEXT NOT NULL,
		coingecko_id TEXT NOT NULL,
		coinmarketcap_id TEXT NOT NULL,
		icon_path TEXT,
		last_updated INTEGER NOT NULL
	);`
	if _, err := s.db.Exec(schema); err != nil {
		s.logger.LogError("Failed to execute database schema: %v", err)
		return fmt.Errorf("failed to execute database schema: %w", err)
	}
	s.logger.LogInfo("Database schema initialized successfully.")
	return nil
}

// GetAssetIdentity retrieves a cached asset identity from the database by its common symbol.
func (s *SQLiteCache) GetAssetIdentity(commonSymbol string) (*AssetIdentity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `SELECT id, common_symbol, kraken_asset, kraken_ws_name, coingecko_id, coinmarketcap_id, icon_path, last_updated 
               FROM asset_identities WHERE common_symbol = ?`

	row := s.db.QueryRow(query, commonSymbol)

	var identity AssetIdentity
	var lastUpdated int64
	err := row.Scan(
		&identity.ID,
		&identity.CommonSymbol,
		&identity.KrakenAsset,
		&identity.KrakenWsName,
		&identity.CoinGeckoID,
		&identity.CoinMarketCapID,
		&identity.IconPath,
		&lastUpdated,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not found is not an error, it's a cache miss
		}
		return nil, fmt.Errorf("error scanning asset identity for %s: %w", commonSymbol, err)
	}

	identity.LastUpdated = time.Unix(lastUpdated, 0)
	return &identity, nil
}

// SaveAssetIdentity saves or updates an asset identity in the database.
func (s *SQLiteCache) SaveAssetIdentity(identity *AssetIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `INSERT OR REPLACE INTO asset_identities 
              (common_symbol, kraken_asset, kraken_ws_name, coingecko_id, coinmarketcap_id, icon_path, last_updated) 
              VALUES (?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.Exec(query,
		identity.CommonSymbol,
		identity.KrakenAsset,
		identity.KrakenWsName,
		identity.CoinGeckoID,
		identity.CoinMarketCapID,
		identity.IconPath,
		identity.LastUpdated.Unix(),
	)

	if err != nil {
		s.logger.LogError("Failed to save asset identity for %s: %v", identity.CommonSymbol, err)
		return err
	}
	return nil
}

func (s *SQLiteCache) SaveBar(provider, coinID string, bar utilities.OHLCVBar) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`INSERT OR REPLACE INTO ohlcv_bars (provider, coin_id, timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		provider, coinID, bar.Timestamp, bar.Open, bar.High, bar.Low, bar.Close, bar.Volume)
	return err
}

func (s *SQLiteCache) GetBars(provider, coinID string, start, end int64) ([]utilities.OHLCVBar, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	s.mu.RLock()
	defer s.mu.RUnlock()
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

func (s *SQLiteCache) SavePosition(pos *utilities.Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	)
	if err != nil {
		s.logger.LogError("Failed to save position for %s: %v", pos.AssetPair, err)
	}
	return err
}

func (s *SQLiteCache) DeletePosition(assetPair string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM open_positions WHERE asset_pair = ?`, assetPair)
	return err
}

func (s *SQLiteCache) LoadPendingOrders() (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`INSERT OR REPLACE INTO pending_orders (broker_order_id, asset_pair) VALUES (?, ?)`, orderID, assetPair)
	return err
}

func (s *SQLiteCache) DeletePendingOrder(orderID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM pending_orders WHERE broker_order_id = ?`, orderID)
	return err
}

func (s *SQLiteCache) CleanupOldBars(provider string, olderThan time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := olderThan.UnixMilli()
	_, err := s.db.Exec(`DELETE FROM ohlcv_bars WHERE provider=? AND timestamp < ?`, provider, cutoff)
	return err
}

func (s *SQLiteCache) Close() error {
	return s.db.Close()
}

func (s *SQLiteCache) StartScheduledCleanup(interval time.Duration, provider string) {
	go func() {
		// Add a small initial delay to avoid running immediately at startup
		time.Sleep(2 * time.Minute)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			// In a real application, a context would be used for graceful shutdown
			<-ticker.C
			cutoff := time.Now().AddDate(0, 0, -180) // Cleanup data older than 180 days
			s.logger.LogInfo("Running scheduled DB cleanup for provider: %s", provider)
			if err := s.CleanupOldBars(provider, cutoff); err != nil {
				s.logger.LogError("Scheduled cleanup error for %s: %v", provider, err)
			} else {
				s.logger.LogDebug("Scheduled cleanup successful for %s", provider)
			}
		}
	}()
}
func (s *SQLiteCache) GetCachedFees(pair string) (maker, taker float64, fresh bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var ts int64
	err = s.db.QueryRow(`SELECT maker_fee, taker_fee, timestamp FROM cached_fees WHERE pair = ?`, pair).Scan(&maker, &taker, &ts)
	if err == sql.ErrNoRows {
		return 0, 0, false, nil
	} else if err != nil {
		return 0, 0, false, err
	}
	fresh = time.Since(time.Unix(ts, 0)) < 24*time.Hour
	return maker, taker, fresh, nil
}

func (s *SQLiteCache) SaveFees(pair string, maker, taker float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`INSERT OR REPLACE INTO cached_fees (pair, maker_fee, taker_fee, timestamp) VALUES (?, ?, ?, ?)`,
		pair, maker, taker, time.Now().Unix())
	return err
}

func (s *SQLiteCache) GetCachedTicker(pair string) (broker.TickerData, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var data []byte
	var ts int64
	err := s.db.QueryRow(`SELECT data, timestamp FROM cached_tickers WHERE pair = ?`, pair).Scan(&data, &ts)
	if err == sql.ErrNoRows {
		return broker.TickerData{}, false, nil
	} else if err != nil {
		return broker.TickerData{}, false, err
	}
	var ticker broker.TickerData
	if jsonErr := json.Unmarshal(data, &ticker); jsonErr != nil {
		return broker.TickerData{}, false, jsonErr
	}
	fresh := time.Since(time.Unix(ts, 0)) < time.Minute
	return ticker, fresh, nil
}
func (s *SQLiteCache) GetCachedTrades(assetPair string) ([]broker.Trade, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var data []byte
	var ts int64
	err := s.db.QueryRow(`SELECT data, timestamp FROM cached_trades WHERE asset_pair = ?`, assetPair).Scan(&data, &ts)
	if err == sql.ErrNoRows {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	var trades []broker.Trade
	if jsonErr := json.Unmarshal(data, &trades); jsonErr != nil {
		return nil, false, jsonErr
	}
	fresh := time.Since(time.Unix(ts, 0)) < 24*time.Hour // Adjust TTL as needed (e.g., 1h for active trading)
	return trades, fresh, nil
}
func (s *SQLiteCache) SaveTicker(pair string, ticker broker.TickerData) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(ticker)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT OR REPLACE INTO cached_tickers (pair, data, timestamp) VALUES (?, ?, ?)`,
		pair, data, time.Now().Unix())
	return err
}
func (s *SQLiteCache) SaveTrades(assetPair string, trades []broker.Trade) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(trades)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT OR REPLACE INTO cached_trades (asset_pair, data, timestamp) VALUES (?, ?, ?)`,
		assetPair, data, time.Now().Unix())
	return err
}
