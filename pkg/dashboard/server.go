package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"nexuslb/pkg/balancer"
	"nexuslb/pkg/health"
	"nexuslb/pkg/metrics"
	"nexuslb/web"
)

type Server struct {
	addr       string
	proxyAddr  string
	collector  *metrics.Collector
	balancer   balancer.Balancer
	checker    *health.Checker
	httpServer *http.Server
}

func NewServer(addr, proxyAddr string, collector *metrics.Collector, b balancer.Balancer, checker *health.Checker) *Server {
	return &Server{
		addr:      addr,
		proxyAddr: proxyAddr,
		collector: collector,
		balancer:  b,
		checker:   checker,
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

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.collector.Snapshot(s.balancer.Name(), s.balancer.GetBackends())
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
	stats := s.collector.Snapshot(s.balancer.Name(), s.balancer.GetBackends())
	data, _ := json.Marshal(stats)
	fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			stats := s.collector.Snapshot(s.balancer.Name(), s.balancer.GetBackends())
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
	if count > 50 {
		count = 50
	}

	targetURL := fmt.Sprintf("http://localhost%s/", s.proxyAddr)
	client := &http.Client{Timeout: 3 * time.Second}

	successCount := 0
	for i := 0; i < count; i++ {
		resp, err := client.Get(targetURL)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 500 {
				successCount++
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"requested": count,
		"success":   successCount,
	})
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

func (s *Server) handleResetStats(w http.ResponseWriter, r *http.Request) {
	s.collector.Reset(s.balancer.GetBackends())
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": "Metrics reset successfully",
	})
}

