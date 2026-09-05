package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type ServerInstance struct {
	id      int
	port    int
	isDown  bool
	delayMs int
	mux     sync.RWMutex
}

func (s *ServerInstance) handleRoot(w http.ResponseWriter, r *http.Request) {
	s.mux.RLock()
	down := s.isDown
	delay := s.delayMs
	s.mux.RUnlock()

	if delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}

	if down {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "[Backend %d on :%d] SIMULATED INTERNAL ERROR\n", s.id, s.port)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Backend-Id", fmt.Sprintf("%d", s.id))
	w.Header().Set("X-Backend-Port", fmt.Sprintf("%d", s.port))
	fmt.Fprintf(w, "Response served from Backend Server %d (port :%d)\n", s.id, s.port)
}

func (s *ServerInstance) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mux.RLock()
	down := s.isDown
	s.mux.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if down {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "unhealthy",
			"server": s.id,
			"port":   s.port,
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"server": s.id,
		"port":   s.port,
	})
}

func (s *ServerInstance) handleChaos(w http.ResponseWriter, r *http.Request) {
	s.mux.Lock()
	s.isDown = !s.isDown
	currState := s.isDown
	s.mux.Unlock()

	w.Header().Set("Content-Type", "application/json")
	stateStr := "healthy"
	if currState {
		stateStr = "unhealthy"
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"server":  s.id,
		"port":    s.port,
		"is_down": currState,
		"state":   stateStr,
	})
	log.Printf("[SIMULATOR] Backend %d (:%d) toggled to %s", s.id, s.port, stateStr)
}

func startServer(id, port int) *http.Server {
	instance := &ServerInstance{
		id:   id,
		port: port,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", instance.handleRoot)
	mux.HandleFunc("/health", instance.handleHealth)
	mux.HandleFunc("/chaos/toggle", instance.handleChaos)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		log.Printf("[BACKEND %d] Upstream running on http://localhost:%d", id, port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[BACKEND %d] Error: %v", id, err)
		}
	}()

	return srv
}

func main() {
	singlePort := flag.Int("port", 0, "Run a single backend on specified port (e.g. 8001)")
	flag.Parse()

	var servers []*http.Server

	if *singlePort > 0 {
		servers = append(servers, startServer(1, *singlePort))
	} else {
		// Launch 3 mock upstream servers on ports 8001, 8002, 8003
		servers = append(servers, startServer(1, 8001))
		servers = append(servers, startServer(2, 8002))
		servers = append(servers, startServer(3, 8003))
	}

	log.Println("[MOCK CLUSTER] All simulated upstream servers are ready. Press Ctrl+C to stop.")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("[MOCK CLUSTER] Shutting down simulated backends...")
	for _, srv := range servers {
		srv.Close()
	}
}
