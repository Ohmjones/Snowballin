package web

import (
	"Snowballin/utilities"
	"sync"
)

// DashboardData is updated to include overall Profit/Loss.
type DashboardData struct {
	ActivePositions   map[string]utilities.Position
	PortfolioValue    float64
	TotalUnrealizedPL float64 // New: Overall profit/loss of all open positions
	QuoteCurrency     string
	Version           string
}

// AssetDetailData holds all information for the new asset-specific page.
type AssetDetailData struct {
	AssetPair       string
	Position        *utilities.Position // Pointer, as a position might not exist
	CurrentPrice    float64
	IndicatorValues map[string]string
	Analysis        []string // The human-readable analysis points
}

// AppController defines the interface the web package needs to interact with
// the main application's state. We add a new method to get data for one asset.
type AppController interface {
	GetDashboardData() DashboardData
	GetConfig() utilities.AppConfig
	UpdateAndSaveConfig(newConfig utilities.AppConfig) error
	GetAssetDetailData(assetPair string) (AssetDetailData, error) // New method
	Logger() *utilities.Logger
	Mutex() *sync.RWMutex
}
