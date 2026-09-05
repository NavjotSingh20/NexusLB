package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"nexuslb/pkg/backend"
)

func TestHealthChecker(t *testing.T) {
	// Start mock healthy server
	healthySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer healthySrv.Close()

	// Start mock failing server
	failingSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failingSrv.Close()

	b1, _ := backend.NewBackend(healthySrv.URL)
	b2, _ := backend.NewBackend(failingSrv.URL)

	checker := NewChecker([]*backend.Backend{b1, b2}, 50*time.Millisecond, "/health")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker.CheckAll()

	if !b1.IsAlive() {
		t.Errorf("Expected b1 to be alive, got false")
	}
	if b2.IsAlive() {
		t.Errorf("Expected b2 to be dead, got true")
	}

	_ = ctx
}
