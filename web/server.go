package web

import (
	"context"
	"net/http"
	"path/filepath"
	"time"
)

// StartWebServer initializes and starts the web server in a new goroutine.
// It takes an AppController, which is an interface implemented by the main application.
func StartWebServer(ctx context.Context, controller AppController) {
	addr := ":8080"

	staticDir, err := filepath.Abs("./web/static")
	if err != nil {
		// CORRECT: Call the Logger() method, which returns the logger object.
		// Then, call the LogFatal() method on that object.
		controller.Logger().LogFatal("Could not determine absolute path for static directory: %v", err)
	}

	mux := http.NewServeMux()

	fs := http.FileServer(http.Dir(staticDir))
	mux.Handle("/static/", http.StripPrefix("/static/", fs))

	// Pass the controller to the handlers
	mux.HandleFunc("/", dashboardHandler(controller))
	mux.HandleFunc("/settings", settingsHandler(controller))

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start the server in a goroutine
	go func() {
		// CORRECT: controller.Logger().LogInfo(...)
		// INCORRECT: controller.Logger.LogInfo(...)
		controller.Logger().LogInfo("Starting web dashboard on http://localhost%s", addr)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			controller.Logger().LogFatal("Web server failed: %v", err)
		}
	}()

	// Listen for context cancellation to gracefully shut down the server
	go func() {
		<-ctx.Done()
		controller.Logger().LogInfo("Shutting down web server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			controller.Logger().LogError("Web server graceful shutdown failed: %v", err)
		}
	}()
}
