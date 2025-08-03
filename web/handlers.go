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

// renderTemplate now accepts the controller to fetch common layout data.
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
	}

	// This is the new, consistent data structure for the layout.
	// It always has a Version and the specific data for the page being rendered.
	layoutData := struct {
		Template string
		Version  string
		PageData interface{}
	}{
		Template: tmplName,
		Version:  controller.GetConfig().Version, // Always get the version
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

// dashboardHandler now passes the controller to renderTemplate.
func dashboardHandler(controller AppController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dashboardData := controller.GetDashboardData()
		renderTemplate(w, r, controller, "dashboard.html", dashboardData)
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

// settingsGetHandler now passes the controller to renderTemplate.
func settingsGetHandler(w http.ResponseWriter, r *http.Request, controller AppController) {
	currentConfig := controller.GetConfig()

	// This is now the page-specific data.
	pageData := struct {
		Config  utilities.AppConfig
		Message string
	}{
		Config:  currentConfig,
		Message: r.URL.Query().Get("message"),
	}
	renderTemplate(w, r, controller, "settings.html", pageData)
}

// settingsPostHandler does not need changes to its core logic.
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
	newConfig.Trading.TrailingStopEnabled = r.FormValue("trading.trailing_stop_enabled") == "on"
	newConfig.Trading.BaseOrderSize, _ = strconv.ParseFloat(r.FormValue("trading.base_order_size"), 64)
	newConfig.Trading.MaxSafetyOrders, _ = strconv.Atoi(r.FormValue("trading.max_safety_orders"))
	newConfig.Trading.TakeProfitPercentage, _ = strconv.ParseFloat(r.FormValue("trading.take_profit_percentage"), 64)
	newConfig.Trading.SafetyOrderStepScale, _ = strconv.ParseFloat(r.FormValue("trading.safety_order_step_scale"), 64)
	newConfig.Trading.SafetyOrderVolumeScale, _ = strconv.ParseFloat(r.FormValue("trading.safety_order_volume_scale"), 64)
	newConfig.Trading.PriceDeviationToOpenSafetyOrders, _ = strconv.ParseFloat(r.FormValue("trading.price_deviation_to_open_safety_orders"), 64)
	newConfig.Trading.ConsensusBuyMultiplier, _ = strconv.ParseFloat(r.FormValue("trading.consensus_buy_multiplier"), 64)
	newConfig.Indicators.RSIPeriod, _ = strconv.Atoi(r.FormValue("indicators.rsi_period"))
	newConfig.Indicators.ATRPeriod, _ = strconv.Atoi(r.FormValue("indicators.atr_period"))
	newConfig.Indicators.MACDFastPeriod, _ = strconv.Atoi(r.FormValue("indicators.macd_fast_period"))
	newConfig.Indicators.MACDSlowPeriod, _ = strconv.Atoi(r.FormValue("indicators.macd_slow_period"))
	newConfig.Indicators.MACDSignalPeriod, _ = strconv.Atoi(r.FormValue("indicators.macd_signal_period"))
	newConfig.CircuitBreaker.Enabled = r.FormValue("circuitbreaker.enabled") == "on"
	newConfig.CircuitBreaker.DrawdownThresholdPercent, _ = strconv.ParseFloat(r.FormValue("circuitbreaker.drawdown_threshold_percent"), 64)
	newConfig.Logging.Level = r.FormValue("logging.level")

	if err := controller.UpdateAndSaveConfig(newConfig); err != nil {
		http.Error(w, "Failed to update configuration: "+err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/settings?message=Success! Settings have been saved and applied.", http.StatusFound)
}
