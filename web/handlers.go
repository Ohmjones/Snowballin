package web

import (
	"html/template"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
)

// renderTemplate loads base layout + a page template with func map.
func renderTemplate(w http.ResponseWriter, r *http.Request, controller AppController, tmplName string, pageData interface{}) {
	lp := filepath.Join("web", "templates", "layout.html")
	fp := filepath.Join("web", "templates", tmplName)

	funcMap := template.FuncMap{
		"safeURL": func(s string) template.URL { return template.URL(s) },
		"replace": strings.Replace,
		"join":    strings.Join,
	}

	tmpl, err := template.New(filepath.Base(lp)).Funcs(funcMap).ParseFiles(lp, fp)
	if err != nil {
		controller.Logger().LogError("template parse error: %v", err)
		http.Error(w, "template error", http.StatusInternalServerError)
		return
	}
	if err := tmpl.ExecuteTemplate(w, filepath.Base(lp), pageData); err != nil {
		controller.Logger().LogError("template exec error: %v", err)
		http.Error(w, "template error", http.StatusInternalServerError)
		return
	}
}

// GET /
func dashboardHandler(controller AppController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		dd := controller.GetDashboardData(r.Context())
		cfg := controller.GetConfig()
		page := DashboardPageData{Dashboard: dd, AssetPairs: cfg.Trading.AssetPairs}
		renderTemplate(w, r, controller, "dashboard.html", page)
	}
}

// GET/POST /settings
func settingsHandler(controller AppController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			page := SettingsPageData{Config: controller.GetConfig()}
			renderTemplate(w, r, controller, "settings.html", page)

		case http.MethodPost:
			cfg := controller.GetConfig()

			// Trading – Take Profit controls (float64)
			if v := r.FormValue("trading.take_profit_percentage"); v != "" {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					cfg.Trading.TakeProfitPercentage = f
				}
			}
			if v := r.FormValue("trading.take_profit_partial_sell_percent"); v != "" {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					cfg.Trading.TakeProfitPartialSellPercent = f
				}
			}
			if v := r.FormValue("trading.trailing_stop_deviation"); v != "" {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					cfg.Trading.TrailingStopDeviation = f
				}
			}

			// Indicators – RSI / StochRSI
			if v := r.FormValue("indicators.rsi_period"); v != "" {
				if i, err := strconv.Atoi(v); err == nil {
					cfg.Indicators.RSIPeriod = i
				}
			}
			// RSI Buy Threshold maps to Trading.PredictiveRsiThreshold (float64)
			if v := r.FormValue("indicators.rsi_buy_threshold"); v != "" {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					cfg.Trading.PredictiveRsiThreshold = f
				}
			}
			if v := r.FormValue("indicators.stoch_rsi_buy_threshold"); v != "" {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					cfg.Indicators.StochRSIBuyThreshold = f
				}
			}
			if v := r.FormValue("indicators.stoch_rsi_sell_threshold"); v != "" {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					cfg.Indicators.StochRSISellThreshold = f
				}
			}

			// Consensus timeframes – base + up to 2 extras
			if v := strings.TrimSpace(r.FormValue("consensus.base_timeframe")); v != "" {
				cfg.Consensus.MultiTimeframe.BaseTimeframe = v
			}
			tfs := []string{}
			if v := strings.TrimSpace(r.FormValue("consensus.additional_timeframe_1")); v != "" {
				tfs = append(tfs, v)
			}
			if v := strings.TrimSpace(r.FormValue("consensus.additional_timeframe_2")); v != "" {
				tfs = append(tfs, v)
			}
			cfg.Consensus.MultiTimeframe.AdditionalTimeframes = tfs

			// Persist
			if err := controller.UpdateAndSaveConfig(cfg); err != nil {
				controller.Logger().LogError("saving config: %v", err)
				http.Error(w, "failed saving config", http.StatusInternalServerError)
				return
			}

			page := SettingsPageData{
				Config:  cfg,
				Saved:   true,
				Message: "Settings saved. Changes apply next cycle.",
			}
			renderTemplate(w, r, controller, "settings.html", page)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// GET /asset/{PAIR-DASH} e.g., /asset/BTC-USD -> "BTC/USD"
func assetDetailHandler(controller AppController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tail := strings.TrimPrefix(r.URL.Path, "/asset/")
		if tail == "" || strings.Contains(tail, "/") {
			http.NotFound(w, r)
			return
		}
		pair := strings.ReplaceAll(tail, "-", "/")

		data, err := controller.GetAssetDetailData(pair)
		if err != nil {
			controller.Logger().LogError("asset detail: %v", err)
			http.NotFound(w, r)
			return
		}

		// Build a local icon path (no controller method needed)
		symbol := strings.ToLower(strings.Split(pair, "/")[0])
		iconURL := "/static/icons/" + symbol + ".webp"

		page := AssetDetailPageData{
			AssetPair:       pair,
			AssetIconURL:    iconURL,
			Position:        data.Position,
			IndicatorValues: data.IndicatorValues,
			Consensus:       data.Consensus,
			Analysis:        data.Analysis,
		}
		renderTemplate(w, r, controller, "asset_detail.html", page)
	}
}
