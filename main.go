package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"nexuslb/config"
	"nexuslb/pkg/backend"
	"nexuslb/pkg/balancer"
	"nexuslb/pkg/dashboard"
	"nexuslb/pkg/health"
	"nexuslb/pkg/metrics"
	"nexuslb/pkg/proxy"
	"nexuslb/pkg/ratelimit"
	"nexuslb/pkg/simulator"
)

func main() {
	configPath := flag.String("config", "config.json", "Path to configuration file")
	simulateFlag := flag.Bool("simulate", false, "Auto-start real-life mixed traffic and DDoS simulator on launch")
	simIntensityFlag := flag.String("sim-intensity", "", "Simulation traffic intensity: low, normal, or heavy")
	simChaosFlag := flag.String("sim-chaos", "", "Override reversible chaos drills (true/false)")
	simDDoSFlag := flag.String("sim-ddos", "", "Override periodic DDoS attack waves (true/false)")
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
		b, err := backend.NewBackendWithConfig(rawURL, cfg.CircuitBreakerMaxFailures, cfg.CircuitBreakerCooldown)
		if err != nil {
			log.Fatalf("Invalid backend URL %s: %v", rawURL, err)
		}
		backends = append(backends, b)
		log.Printf("[INIT] Registered upstream backend: %s (CB threshold=%d, cooldown=%v)", rawURL, cfg.CircuitBreakerMaxFailures, cfg.CircuitBreakerCooldown)
	}

	// 3. Initialize Balancer Strategy
	sm, err := balancer.NewStrategyManager(cfg.Strategy, backends)
	if err != nil {
		log.Fatalf("Failed to initialize load balancer strategy: %v", err)
	}
	log.Printf("[INIT] Active routing algorithm: %s", sm.Name())

	// 4. Initialize Metrics Collector
	collector := metrics.NewCollector()

	// 5. Initialize Edge Security & Rate Limiting Shield
	limiter := ratelimit.NewLimiter(cfg.RateLimitRate, cfg.RateLimitBurst, cfg.RateLimitEnabled)
	defer limiter.Close()
	log.Printf("[INIT] Edge Shield rate limiter active=%v (%.1f req/s, burst capacity %d)", cfg.RateLimitEnabled, cfg.RateLimitRate, cfg.RateLimitBurst)

	// 6. Initialize & Start Active Health Checker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker := health.NewChecker(backends, cfg.HealthCheckInterval, cfg.HealthCheckPath)
	checker.Start(ctx)
	log.Printf("[INIT] Health checker active (probing every %v on %s)", cfg.HealthCheckInterval, cfg.HealthCheckPath)

	// 7. Start Reverse Proxy Server
	proxyHandler := proxy.NewProxyHandlerWithOptions(sm, collector, limiter, proxy.ProxyOptions{
		MaxRetries:         cfg.MaxRetries,
		MaxRetryBodyBytes:  cfg.MaxRetryBodyBytes,
		RetryNonIdempotent: cfg.RetryNonIdempotent,
	})
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

	// 8. Initialize Real-Life Traffic Simulator
	simIntensity := simulator.Intensity(cfg.SimIntensity)
	if *simIntensityFlag != "" {
		if parsed, ok := simulator.ParseIntensity(*simIntensityFlag); ok {
			simIntensity = parsed
		} else {
			log.Fatalf("Invalid -sim-intensity %q (must be low, normal, or heavy)", *simIntensityFlag)
		}
	}
	simChaos := cfg.SimChaos
	if *simChaosFlag != "" {
		if parsed, err := strconv.ParseBool(*simChaosFlag); err == nil {
			simChaos = parsed
		}
	}
	simDDoS := cfg.SimDDoS
	if *simDDoSFlag != "" {
		if parsed, err := strconv.ParseBool(*simDDoSFlag); err == nil {
			simDDoS = parsed
		}
	}

	sim := simulator.New(simulator.SimConfig{
		Enabled:    cfg.SimEnabled || *simulateFlag,
		Intensity:  simIntensity,
		AllowChaos: simChaos,
		AllowDDoS:  simDDoS,
	}, simulator.ClusterHooks{
		ProxyURL:    "http://localhost" + cfg.ProxyPort,
		GetBackends: sm.GetBackends,
		CheckHealth: checker.CheckAll,
		StrategyKey: sm.CurrentKey,
	})

	// 9. Start Observability Dashboard Server
	dashServer := dashboard.NewServer(cfg.DashboardPort, cfg.ProxyPort, collector, sm, checker, limiter)
	dashServer.SetSimulator(sim)
	go func() {
		if err := dashServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Dashboard server failed: %v", err)
		}
	}()

	if cfg.SimEnabled || *simulateFlag {
		sim.Start()
		log.Printf("[INIT] Real-life traffic simulator active (intensity=%s, chaos=%v, ddos=%v)", simIntensity, simChaos, simDDoS)
	} else {
		log.Printf("[INIT] Real-life traffic simulator standby (start via Dashboard or -simulate flag)")
	}

	log.Printf("\nNexusLB is running!")
	log.Printf("Ingress Proxy:     http://localhost%s", cfg.ProxyPort)
	log.Printf("Admin Dashboard:   http://localhost%s\n", cfg.DashboardPort)

	// 10. Graceful Shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("\n[SHUTDOWN] Shutting down NexusLB gracefully...")
	sim.Stop()
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
