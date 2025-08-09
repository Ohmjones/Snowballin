package web

import (
	"context"
	"net/http"
	"path/filepath"
)

// StartServer configures routes and serves the UI (explicit addr).
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

// Back-compat: your app calls StartWebServer(ctx, state).
// Since you hardcode the port, we just use :8080.
func StartWebServer(_ context.Context, controller AppController) *http.Server {
	return StartServer(controller, ":8080")
}

// Optional convenience if you ever want to pass a custom address.
func StartWebServerWithAddr(controller AppController, addr string) *http.Server {
	return StartServer(controller, addr)
}
