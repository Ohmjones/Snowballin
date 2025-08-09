package web

import (
	"context"
	"net/http"
	"path/filepath"
)

// StartServer configures routes and returns an *http.Server.
// (Matches your original structure: it does NOT start listening.)
func StartServer(controller AppController, addr string) *http.Server {
	staticDir, err := filepath.Abs("./web/static")
	if err != nil {
		controller.Logger().LogFatal("Could not determine absolute path for static: %v", err)
	}

	mux := http.NewServeMux()
	fs := http.FileServer(http.Dir(staticDir))
	mux.Handle("/static/", http.StripPrefix("/static/", fs))

	mux.HandleFunc("/", dashboardHandler(controller))
	mux.HandleFunc("/settings", settingsHandler(controller))
	mux.HandleFunc("/asset/", assetDetailHandler(controller))

	srv := &http.Server{Addr: addr, Handler: mux}
	controller.Logger().LogInfo("Web listening on %s", addr)
	return srv
}

// StartWebServer matches your app.go call signature and actually starts the listener.
// Hardcoded to :8080 as per your setup.
func StartWebServer(_ context.Context, controller AppController) *http.Server {
	srv := StartServer(controller, ":8080")
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			controller.Logger().LogError("Web server error: %v", err)
		}
	}()
	return srv
}
