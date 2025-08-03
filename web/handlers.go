package web

import (
	"Snowballin/utilities"
	"fmt"
	"html/template"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// renderTemplate is updated with a new function to make URLs safe.
func renderTemplate(w http.ResponseWriter, r *http.Request, controller AppController, tmplName string, pageData interface{}) {
	lp := filepath.Join("web", "templates", "layout.html")
	fp := filepath.Join("web", "templates", tmplName)

	funcMap := template.FuncMap{
		"join": strings.Join,
		"timeSince": func(t time.Time) string {
			if t.IsZero() {
				return "N/A"
			}
			return time.Since(t).Round(time.Second).String()
		},
		// New function to make asset pairs safe for URLs (e.g., "BTC/USD" -> "BTC-USD")
		"safeURL": func(s string) string {
			return strings.Replace(s, "/", "-", -1)
		},
	}

	layoutData := struct {
		Template string
		Version  string
		PageData interface{}
	}{
		Template: tmplName,
		Version:  controller.GetConfig().Version,
		PageData: pageData,
	}

	tmpl, err := template.New(filepath.Base(lp)).Funcs(funcMap).ParseFiles(lp, fp)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error parsing template: %v", err), http.StatusInternalServerError)
		return
	}

	if err := tmpl.Execute(w, layoutData); err != nil {
		http.Error(w, fmt.Sprintf("Error executing template: %v", err), http.StatusInternalServerError)
	}
}

// dashboardHandler is updated to calculate and pass P/L data.
func dashboardHandler(controller AppController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dashboardData := controller.GetDashboardData()

		// This is a simplified P/L calculation. For a more performant version on many assets,
		// you would fetch all ticker prices at once.
		if len(dashboardData.ActivePositions) > 0 {
			var totalPL float64
			for pair, pos := range dashboardData.ActivePositions {
				detailData, err := controller.GetAssetDetailData(pair)
				if err == nil && detailData.Position != nil {
					// The P/L calculation is now done in GetAssetDetailData, so we just use it.
					pos.UnrealizedPL = detailData.Position.UnrealizedPL
					pos.UnrealizedPLPercent = detailData.Position.UnrealizedPLPercent
					dashboardData.ActivePositions[pair] = pos
					totalPL += pos.UnrealizedPL
				}
			}
			dashboardData.TotalUnrealizedPL = totalPL
		}

		renderTemplate(w, r, controller, "dashboard.html", dashboardData)
	}
}

// assetDetailHandler is the new handler for the asset-specific page.
func assetDetailHandler(controller AppController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract asset pair from URL, e.g., "/asset/BTC-USD"
		urlPath := strings.TrimPrefix(r.URL.Path, "/asset/")
		assetPair := strings.Replace(urlPath, "-", "/", 1) // "BTC-USD" -> "BTC/USD"

		assetData, err := controller.GetAssetDetailData(assetPair)
		if err != nil {
			http.Error(w, "Failed to get asset data: "+err.Error(), http.StatusInternalServerError)
			return
		}

		renderTemplate(w, r, controller, "asset_detail.html", assetData)
	}
}

// settingsHandler routes GET and POST requests for the settings page.
func settingsHandler(controller AppController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			settingsPostHandler(w, r, controller)
		} else {
			settingsGetHandler(w, r, controller)
		}
	}
}

// settingsGetHandler remains the same.
func settingsGetHandler(w http.ResponseWriter, r *http.Request, controller AppController) {
	currentConfig := controller.GetConfig()
	pageData := struct {
		Config  utilities.AppConfig
		Message string
	}{
		Config:  currentConfig,
		Message: r.URL.Query().Get("message"),
	}
	renderTemplate(w, r, controller, "settings.html", pageData)
}

// settingsPostHandler remains the same.
func settingsPostHandler(w http.ResponseWriter, r *http.Request, controller AppController) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}
	newConfig := controller.GetConfig()
	rawPairs := r.FormValue("trading.asset_pairs")
	newConfig.Trading.AssetPairs = []string{}
	for _, p := range strings.Split(rawPairs, ",") {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			newConfig.Trading.AssetPairs = append(newConfig.Trading.AssetPairs, trimmed)
		}
	}
	newConfig.Trading.PortfolioRiskPerTrade, _ = strconv.ParseFloat(r.FormValue("trading.portfolio_risk_per_trade"), 64)
	newConfig.Trading.BaseOrderSize, _ = strconv.ParseFloat(r.FormValue("trading.base_order_size"), 64)
	newConfig.Trading.TakeProfitPercentage, _ = strconv.ParseFloat(r.FormValue("trading.take_profit_percentage"), 64)
	newConfig.Trading.TakeProfitPartialSellPercent, _ = strconv.ParseFloat(r.FormValue("trading.take_profit_partial_sell_percent"), 64)
	newConfig.Trading.MaxSafetyOrders, _ = strconv.Atoi(r.FormValue("trading.max_safety_orders"))
	newConfig.Trading.PriceDeviationToOpenSafetyOrders, _ = strconv.ParseFloat(r.FormValue("trading.price_deviation_to_open_safety_orders"), 64)
	newConfig.Trading.SafetyOrderStepScale, _ = strconv.ParseFloat(r.FormValue("trading.safety_order_step_scale"), 64)
	newConfig.Trading.SafetyOrderVolumeScale, _ = strconv.ParseFloat(r.FormValue("trading.safety_order_volume_scale"), 64)
	newConfig.Trading.TrailingStopEnabled = r.FormValue("trading.trailing_stop_enabled") == "on"
	newConfig.Trading.TrailingStopDeviation, _ = strconv.ParseFloat(r.FormValue("trading.trailing_stop_deviation"), 64)
	newConfig.Indicators.RSIPeriod, _ = strconv.Atoi(r.FormValue("indicators.rsi_period"))
	newConfig.Indicators.ATRPeriod, _ = strconv.Atoi(r.FormValue("indicators.atr_period"))
	newConfig.Indicators.MACDFastPeriod, _ = strconv.Atoi(r.FormValue("indicators.macd_fast_period"))
	newConfig.Indicators.MACDSlowPeriod, _ = strconv.Atoi(r.FormValue("indicators.macd_slow_period"))
	newConfig.Indicators.MACDSignalPeriod, _ = strconv.Atoi(r.FormValue("indicators.macd_signal_period"))
	newConfig.Indicators.StochRSIBuyThreshold, _ = strconv.ParseFloat(r.FormValue("indicators.stoch_rsi_buy_threshold"), 64)
	newConfig.CircuitBreaker.Enabled = r.FormValue("circuitbreaker.enabled") == "on"
	newConfig.CircuitBreaker.DrawdownThresholdPercent, _ = strconv.ParseFloat(r.FormValue("circuitbreaker.drawdown_threshold_percent"), 64)
	newConfig.Logging.Level = r.FormValue("logging.level")
	if err := controller.UpdateAndSaveConfig(newConfig); err != nil {
		http.Error(w, "Failed to update configuration: "+err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/settings?message=Success! Settings have been saved and applied.", http.StatusFound)
}
