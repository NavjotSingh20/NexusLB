package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"nexuslb/pkg/backend"
	"nexuslb/pkg/balancer"
	"nexuslb/pkg/health"
	"nexuslb/pkg/metrics"
	"nexuslb/pkg/ratelimit"
	"nexuslb/web"
)

type Server struct {
	addr        string
	proxyAddr   string
	collector   *metrics.Collector
	balancer    balancer.Balancer
	strategyMgr *balancer.StrategyManager
	checker     *health.Checker
	limiter     *ratelimit.Limiter
	httpServer  *http.Server
}

func NewServer(addr, proxyAddr string, collector *metrics.Collector, b balancer.Balancer, checker *health.Checker, limiter *ratelimit.Limiter) *Server {
	var sm *balancer.StrategyManager
	if mgr, ok := b.(*balancer.StrategyManager); ok {
		sm = mgr
	}
	return &Server{
		addr:        addr,
		proxyAddr:   proxyAddr,
		collector:   collector,
		balancer:    b,
		strategyMgr: sm,
		checker:     checker,
		limiter:     limiter,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Serve static web assets
	fileServer := http.FileServer(http.FS(web.Assets))
	mux.Handle("/", fileServer)

	// API Endpoints
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/stream", s.handleStream)
	mux.HandleFunc("/api/test-request", s.handleTestRequest)
	mux.HandleFunc("/api/backend/toggle", s.handleToggleBackend)
	mux.HandleFunc("/api/backend/delay", s.handleBackendDelay)
	mux.HandleFunc("/api/backend/connections", s.handleBackendConnections)
	mux.HandleFunc("/api/backend/weight", s.handleBackendWeight)
	mux.HandleFunc("/api/backend/canary", s.handleBackendCanary)
	mux.HandleFunc("/api/strategy", s.handleStrategy)
	mux.HandleFunc("/api/shield", s.handleShield)
	mux.HandleFunc("/api/shield/toggle", s.handleToggleShield)
	mux.HandleFunc("/api/shield/config", s.handleConfigShield)
	mux.HandleFunc("/api/stats/reset", s.handleResetStats)

	s.httpServer = &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 0, // Zero timeout allows long-lived SSE connections
	}

	log.Printf("[DASHBOARD] Monitoring dashboard listening on http://localhost%s", s.addr)
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

func (s *Server) getStats() metrics.DashboardStats {
	stats := s.collector.Snapshot(s.balancer.Name(), s.balancer.GetBackends())
	if s.strategyMgr != nil {
		stats.StrategyKey = s.strategyMgr.CurrentKey()
		stats.AvailableStrategies = s.strategyMgr.AvailableStrategies()
	} else {
		stats.StrategyKey = "round_robin"
		stats.AvailableStrategies = []string{"round_robin", "least_connections", "ip_hash"}
	}
	if s.limiter != nil {
		stats.ShieldEnabled = s.limiter.IsEnabled()
		stats.ShieldRate = s.limiter.Rate()
		stats.ShieldBurst = s.limiter.Burst()
		stats.TotalThrottled = s.limiter.TotalBlocked()
	}
	return stats
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.getStats()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Send initial snapshot immediately
	stats := s.getStats()
	data, _ := json.Marshal(stats)
	fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			stats := s.getStats()
			data, err := json.Marshal(stats)
			if err != nil {
				continue
			}
			_, err = fmt.Fprintf(w, "data: %s\n\n", data)
			if err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) handleTestRequest(w http.ResponseWriter, r *http.Request) {
	countStr := r.URL.Query().Get("count")
	count := 1
	if countStr != "" {
		if val, err := strconv.Atoi(countStr); err == nil && val > 0 {
			count = val
		}
	}
	if count > 2000 {
		count = 2000
	}

	targetURL := fmt.Sprintf("http://localhost%s/", s.proxyAddr)
	delayParam := r.URL.Query().Get("delay")
	if delayParam != "" {
		targetURL = fmt.Sprintf("%s?delay=%s", targetURL, delayParam)
	}
	simIPs := r.URL.Query().Get("sim_ips") == "true" || r.URL.Query().Get("sim_ips") == "1"
	customClientIP := r.URL.Query().Get("client_ip")
	isCanary := r.URL.Query().Get("canary") == "true" || r.URL.Query().Get("canary") == "1"

	tr := &http.Transport{
		MaxIdleConns:        150,
		MaxIdleConnsPerHost: 150,
		IdleConnTimeout:     30 * time.Second,
	}
	timeout := 10 * time.Second
	if delayParam != "" {
		if d, err := strconv.Atoi(delayParam); err == nil && d > 0 {
			timeout = time.Duration(d)*time.Millisecond + 5*time.Second
		}
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   timeout,
	}

	// Dispatch requests concurrently up to 150 workers
	concurrency := count
	if concurrency > 150 {
		concurrency = 150
	}
	if concurrency < 1 {
		concurrency = 1
	}

	// Register queued burst requests
	s.collector.IncQueue(int64(count))

	jobs := make(chan int, count)
	for i := 0; i < count; i++ {
		jobs <- i
	}
	close(jobs)

	var successCount int64
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for reqIndex := range jobs {
				req, err := http.NewRequest("GET", targetURL, nil)
				if err == nil {
					if isCanary {
						req.Header.Set("X-Canary", "true")
					}
					if customClientIP != "" {
						req.Header.Set("X-Forwarded-For", customClientIP)
					} else if simIPs {
						// Distribute across 10 deterministic simulated client IPs for IP hash demonstration
						ipLastOctet := ((reqIndex + workerID) % 10) + 1
						req.Header.Set("X-Forwarded-For", fmt.Sprintf("192.168.1.%d", 100+ipLastOctet))
					}
					resp, err := client.Do(req)
					if err == nil {
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
						if resp.StatusCode < 500 {
							atomic.AddInt64(&successCount, 1)
						}
					}
				}
				s.collector.DecQueue()
			}
		}(w)
	}
	wg.Wait()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"requested": count,
		"success":   successCount,
	})
}

func (s *Server) handleStrategy(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method == http.MethodGet {
		currentKey := "round_robin"
		name := s.balancer.Name()
		var available []string
		if s.strategyMgr != nil {
			currentKey = s.strategyMgr.CurrentKey()
			name = s.strategyMgr.Name()
			available = s.strategyMgr.AvailableStrategies()
		} else {
			available = []string{"round_robin", "least_connections", "ip_hash"}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"strategy":  name,
			"key":       currentKey,
			"available": available,
		})
		return
	}

	if r.Method == http.MethodPost {
		if s.strategyMgr == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "dynamic strategy switching not supported by current balancer instance",
			})
			return
		}

		name := r.URL.Query().Get("name")
		if name == "" {
			var body struct {
				Name string `json:"name"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err == nil && body.Name != "" {
				name = body.Name
			}
		}

		if name == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "missing strategy name parameter",
			})
			return
		}

		if err := s.strategyMgr.SetStrategy(name); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": err.Error(),
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "ok",
			"strategy": s.strategyMgr.Name(),
			"key":      s.strategyMgr.CurrentKey(),
		})
		return
	}

	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func (s *Server) handleToggleBackend(w http.ResponseWriter, r *http.Request) {
	targetURL := r.URL.Query().Get("url")
	if targetURL == "" {
		http.Error(w, "missing url param", http.StatusBadRequest)
		return
	}

	stateParam := r.URL.Query().Get("state")
	chaosURL := fmt.Sprintf("%s/chaos/toggle", targetURL)
	if stateParam != "" {
		chaosURL = fmt.Sprintf("%s?state=%s", chaosURL, stateParam)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(chaosURL)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":   "failed to contact backend",
			"details": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	// Trigger immediate health checks to instantly sync load balancer state
	if s.checker != nil {
		s.checker.CheckAll()
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	io.Copy(w, resp.Body)
}

func (s *Server) handleBackendDelay(w http.ResponseWriter, r *http.Request) {
	targetURL := r.URL.Query().Get("url")
	if targetURL == "" {
		http.Error(w, "missing url param", http.StatusBadRequest)
		return
	}

	msParam := r.URL.Query().Get("ms")
	delayURL := fmt.Sprintf("%s/chaos/delay?ms=%s", targetURL, msParam)

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(delayURL)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":   "failed to contact backend",
			"details": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	io.Copy(w, resp.Body)
}

func (s *Server) handleBackendConnections(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	targetURL := r.URL.Query().Get("url")
	if targetURL == "" {
		http.Error(w, "missing url param", http.StatusBadRequest)
		return
	}

	countStr := r.URL.Query().Get("count")
	deltaStr := r.URL.Query().Get("delta")

	backends := s.balancer.GetBackends()
	var found *backend.Backend
	for _, b := range backends {
		if b.URL.String() == targetURL {
			found = b
			break
		}
	}

	if found == nil {
		http.Error(w, "backend not found", http.StatusNotFound)
		return
	}

	if deltaStr != "" {
		if delta, err := strconv.ParseInt(deltaStr, 10, 64); err == nil {
			curr := atomic.LoadInt64(&found.ActiveConnections)
			newCount := curr + delta
			found.SetActiveConnections(newCount)
		}
	} else if countStr != "" {
		if count, err := strconv.ParseInt(countStr, 10, 64); err == nil {
			found.SetActiveConnections(count)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":             "ok",
		"url":                found.URL.String(),
		"active_connections": atomic.LoadInt64(&found.ActiveConnections),
	})
}

func (s *Server) handleShield(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	if s.limiter == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"enabled": false})
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"enabled":       s.limiter.IsEnabled(),
		"rate":          s.limiter.Rate(),
		"burst":         s.limiter.Burst(),
		"total_blocked": s.limiter.TotalBlocked(),
		"total_allowed": s.limiter.TotalAllowed(),
		"active_ips":    s.limiter.ActiveIPCount(),
	})
}

func (s *Server) handleToggleShield(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if s.limiter == nil {
		http.Error(w, `{"error":"rate limiter not initialized"}`, http.StatusBadRequest)
		return
	}

	stateParam := r.URL.Query().Get("state")
	if stateParam == "on" || stateParam == "true" {
		s.limiter.SetEnabled(true)
	} else if stateParam == "off" || stateParam == "false" {
		s.limiter.SetEnabled(false)
	} else {
		s.limiter.SetEnabled(!s.limiter.IsEnabled())
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ok",
		"enabled": s.limiter.IsEnabled(),
		"rate":    s.limiter.Rate(),
		"burst":   s.limiter.Burst(),
	})
}

func (s *Server) handleConfigShield(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if s.limiter == nil {
		http.Error(w, `{"error":"rate limiter not initialized"}`, http.StatusBadRequest)
		return
	}

	rateStr := r.URL.Query().Get("rate")
	burstStr := r.URL.Query().Get("burst")

	rate := s.limiter.Rate()
	burst := s.limiter.Burst()

	if rVal, err := strconv.ParseFloat(rateStr, 64); err == nil && rVal > 0 {
		rate = rVal
	}
	if bVal, err := strconv.Atoi(burstStr); err == nil && bVal > 0 {
		burst = bVal
	}

	s.limiter.SetConfig(rate, burst)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"rate":   rate,
		"burst":  burst,
	})
}

func (s *Server) handleResetStats(w http.ResponseWriter, r *http.Request) {
	s.collector.Reset(s.balancer.GetBackends())
	if s.limiter != nil {
		s.limiter.Reset()
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": "Metrics reset successfully",
	})
}

func (s *Server) handleBackendWeight(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	targetURL := r.URL.Query().Get("url")
	weightStr := r.URL.Query().Get("weight")
	deltaStr := r.URL.Query().Get("delta")

	var targetBackend *backend.Backend
	for _, b := range s.balancer.GetBackends() {
		if b.URL.String() == targetURL {
			targetBackend = b
			break
		}
	}

	if targetBackend == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "backend not found"})
		return
	}

	if weightStr != "" {
		if wVal, err := strconv.ParseInt(weightStr, 10, 64); err == nil && wVal >= 1 {
			targetBackend.SetWeight(wVal)
		}
	} else if deltaStr != "" {
		if dVal, err := strconv.ParseInt(deltaStr, 10, 64); err == nil {
			newW := targetBackend.GetWeight() + dVal
			if newW < 1 {
				newW = 1
			}
			targetBackend.SetWeight(newW)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"url":    targetBackend.URL.String(),
		"weight": targetBackend.GetWeight(),
	})
}

func (s *Server) handleBackendCanary(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	targetURL := r.URL.Query().Get("url")
	enabledStr := r.URL.Query().Get("enabled")
	tag := r.URL.Query().Get("tag")

	var targetBackend *backend.Backend
	for _, b := range s.balancer.GetBackends() {
		if b.URL.String() == targetURL {
			targetBackend = b
			break
		}
	}

	if targetBackend == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "backend not found"})
		return
	}

	isCanary := enabledStr == "true" || enabledStr == "1"
	targetBackend.SetCanary(isCanary, tag)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "ok",
		"url":        targetBackend.URL.String(),
		"is_canary":  targetBackend.IsCanaryNode(),
		"canary_tag": targetBackend.GetCanaryTag(),
	})
}

