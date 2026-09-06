package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"nexuslb/config"
	"nexuslb/pkg/backend"
	"nexuslb/pkg/balancer"
	"nexuslb/pkg/dashboard"
	"nexuslb/pkg/health"
	"nexuslb/pkg/metrics"
	"nexuslb/pkg/proxy"
)

func main() {
	configPath := flag.String("config", "config.json", "Path to configuration file")
	flag.Parse()

	log.Println("==================================================")
	log.Println("    NexusLB - Reverse Proxy & Load Balancer       ")
	log.Println("==================================================")

	// 1. Load Configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// 2. Initialize Upstream Backends
	var backends []*backend.Backend
	for _, rawURL := range cfg.Backends {
		b, err := backend.NewBackend(rawURL)
		if err != nil {
			log.Fatalf("Invalid backend URL %s: %v", rawURL, err)
		}
		backends = append(backends, b)
		log.Printf("[INIT] Registered upstream backend: %s", rawURL)
	}

	// 3. Initialize Balancer Strategy
	rr := balancer.NewRoundRobin(backends)
	log.Printf("[INIT] Active routing algorithm: %s", rr.Name())

	// 4. Initialize Metrics Collector
	collector := metrics.NewCollector()

	// 5. Initialize & Start Active Health Checker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker := health.NewChecker(backends, cfg.HealthCheckInterval, cfg.HealthCheckPath)
	checker.Start(ctx)
	log.Printf("[INIT] Health checker active (probing every %v on %s)", cfg.HealthCheckInterval, cfg.HealthCheckPath)

	// 6. Start Reverse Proxy Server
	proxyHandler := proxy.NewProxyHandler(rr, collector)
	proxyServer := &http.Server{
		Addr:         cfg.ProxyPort,
		Handler:      proxyHandler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("[PROXY] Reverse proxy listening on http://localhost%s", cfg.ProxyPort)
		if err := proxyServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Proxy server failed: %v", err)
		}
	}()

	// 7. Start Observability Dashboard Server
	dashServer := dashboard.NewServer(cfg.DashboardPort, cfg.ProxyPort, collector, rr, checker)
	go func() {
		if err := dashServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Dashboard server failed: %v", err)
		}
	}()

	log.Printf("\nNexusLB is running!")
	log.Printf("Ingress Proxy:     http://localhost%s", cfg.ProxyPort)
	log.Printf("Admin Dashboard:   http://localhost%s\n", cfg.DashboardPort)

	// 8. Graceful Shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("\n[SHUTDOWN] Shutting down NexusLB gracefully...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := proxyServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Proxy shutdown error: %v", err)
	}
	if err := dashServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Dashboard shutdown error: %v", err)
	}

	fmt.Println("NexusLB stopped.")
}
