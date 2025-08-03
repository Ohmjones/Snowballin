package web

import (
	"Snowballin/utilities"
	"sync"
)

// DashboardData is a specific struct to pass data to the dashboard template.
type DashboardData struct {
	ActivePositions map[string]utilities.Position
	PortfolioValue  float64
	QuoteCurrency   string
	Version         string
}

// AppController defines the interface the web package needs to interact with
// the main application's state, breaking the import cycle. Any type that
// implements these methods can be controlled by the web UI.
type AppController interface {
	GetDashboardData() DashboardData
	GetConfig() utilities.AppConfig
	UpdateAndSaveConfig(newConfig utilities.AppConfig) error
	Logger() *utilities.Logger
	Mutex() *sync.RWMutex
}
