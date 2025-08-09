package web

import (
	"Snowballin/utilities"
	"context"
	"sync"
)

// DashboardData is shown on the main dashboard.
type DashboardData struct {
	ActivePositions   map[string]utilities.Position
	PortfolioValue    float64
	TotalUnrealizedPL float64
	QuoteCurrency     string
	Version           string
}

// AssetDetailData is the raw data for a single asset.
type AssetDetailData struct {
	AssetPair       string
	CurrentPrice    float64
	Position        *utilities.Position
	IndicatorValues map[string]string
	Consensus       map[string]string
	Analysis        []string
}

// -------- Page view models (template bindings) --------

type DashboardPageData struct {
	Dashboard  DashboardData
	AssetPairs []string
}

type SettingsPageData struct {
	Config  utilities.AppConfig
	Saved   bool
	Message string
}

type AssetDetailPageData struct {
	AssetPair       string
	AssetIconURL    string
	Position        *utilities.Position
	IndicatorValues map[string]string
	Consensus       map[string]string
	Analysis        []string
}

// -------- Controller interface the main app must satisfy --------

type AppController interface {
	// Live data for dashboard
	GetDashboardData(ctx context.Context) DashboardData

	// Settings/config
	GetConfig() utilities.AppConfig
	UpdateAndSaveConfig(newConfig utilities.AppConfig) error

	// Asset detail: fetch per-asset info (pair like "BTC/USD")
	GetAssetDetailData(assetPair string) (AssetDetailData, error)

	// Logging and concurrency
	Logger() *utilities.Logger
	Mutex() *sync.RWMutex
}
