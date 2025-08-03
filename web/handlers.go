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

// A helper to parse and execute templates
func renderTemplate(w http.ResponseWriter, r *http.Request, tmplName string, data interface{}) {
	lp := filepath.Join("web", "templates", "layout.html")
	fp := filepath.Join("web", "templates", tmplName)

	// Functions to be used in templates
	funcMap := template.FuncMap{
		"join": strings.Join,
		"timeSince": func(t time.Time) string {
			if t.IsZero() {
				return "N/A"
			}
			return time.Since(t).Round(time.Second).String()
		},
	}

	// Data to be passed to the layout template
	layoutData := struct {
		Template string      // Which sub-template is being rendered
		Data     interface{} // The specific data for that sub-template
	}{
		Template: tmplName,
		Data:     data,
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

// dashboardHandler prepares data for and renders the main dashboard.
func dashboardHandler(controller AppController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dashboardData := controller.GetDashboardData()
		renderTemplate(w, r, "dashboard.html", dashboardData)
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

// settingsGetHandler renders the settings form with the current config.
func settingsGetHandler(w http.ResponseWriter, r *http.Request, controller AppController) {
	currentConfig := controller.GetConfig()

	data := struct {
		Config  utilities.AppConfig
		Message string // For displaying success/error messages
	}{
		Config:  currentConfig,
		Message: r.URL.Query().Get("message"),
	}
	renderTemplate(w, r, "settings.html", data)
}

// settingsPostHandler parses the form, updates the config, and saves it.
func settingsPostHandler(w http.ResponseWriter, r *http.Request, controller AppController) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	// Get a copy of the current config to modify
	newConfig := controller.GetConfig()

	// --- Trading Settings ---
	// Handle asset_pairs as a special case for string slices
	rawPairs := r.FormValue("trading.asset_pairs")
	newConfig.Trading.AssetPairs = []string{} // Clear existing
	for _, p := range strings.Split(rawPairs, ",") {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			newConfig.Trading.AssetPairs = append(newConfig.Trading.AssetPairs, trimmed)
		}
	}

	newConfig.Trading.TrailingStopEnabled, _ = strconv.ParseBool(r.FormValue("trading.trailing_stop_enabled"))
	newConfig.Trading.BaseOrderSize, _ = strconv.ParseFloat(r.FormValue("trading.base_order_size"), 64)
	newConfig.Trading.MaxSafetyOrders, _ = strconv.Atoi(r.FormValue("trading.max_safety_orders"))
	newConfig.Trading.TakeProfitPercentage, _ = strconv.ParseFloat(r.FormValue("trading.take_profit_percentage"), 64)
	newConfig.Trading.SafetyOrderStepScale, _ = strconv.ParseFloat(r.FormValue("trading.safety_order_step_scale"), 64)
	newConfig.Trading.SafetyOrderVolumeScale, _ = strconv.ParseFloat(r.FormValue("trading.safety_order_volume_scale"), 64)
	newConfig.Trading.PriceDeviationToOpenSafetyOrders, _ = strconv.ParseFloat(r.FormValue("trading.price_deviation_to_open_safety_orders"), 64)
	newConfig.Trading.ConsensusBuyMultiplier, _ = strconv.ParseFloat(r.FormValue("trading.consensus_buy_multiplier"), 64)

	// --- Indicator Settings ---
	newConfig.Indicators.RSIPeriod, _ = strconv.Atoi(r.FormValue("indicators.rsi_period"))
	newConfig.Indicators.ATRPeriod, _ = strconv.Atoi(r.FormValue("indicators.atr_period"))
	newConfig.Indicators.MACDFastPeriod, _ = strconv.Atoi(r.FormValue("indicators.macd_fast_period"))
	newConfig.Indicators.MACDSlowPeriod, _ = strconv.Atoi(r.FormValue("indicators.macd_slow_period"))
	newConfig.Indicators.MACDSignalPeriod, _ = strconv.Atoi(r.FormValue("indicators.macd_signal_period"))

	// --- Circuit Breaker ---
	// Handle checkbox value (only sent if checked)
	if r.FormValue("circuitbreaker.enabled") == "on" {
		newConfig.CircuitBreaker.Enabled = true
	} else {
		newConfig.CircuitBreaker.Enabled = false
	}
	newConfig.CircuitBreaker.DrawdownThresholdPercent, _ = strconv.ParseFloat(r.FormValue("circuitbreaker.drawdown_threshold_percent"), 64)

	// --- Logging ---
	newConfig.Logging.Level = r.FormValue("logging.level")

	// Call the controller to perform the update and save logic
	if err := controller.UpdateAndSaveConfig(newConfig); err != nil {
		http.Error(w, "Failed to update configuration: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Redirect back to settings page with a success message
	http.Redirect(w, r, "/settings?message=Success! Settings have been saved and applied.", http.StatusFound)
}
