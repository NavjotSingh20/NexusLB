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

func TestHealthChecker_DoesNotConsumeCircuitBreakerHalfOpen(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	b, _ := backend.NewBackendWithConfig(srv.URL, 2, 30*time.Millisecond)
	b.CircuitBreaker.RecordFailure()
	b.CircuitBreaker.RecordFailure()
	if state := b.CircuitBreaker.State(); state != backend.StateOpen {
		t.Fatalf("expected circuit state OPEN after 2 failures, got %s", state)
	}

	// Wait past circuit breaker cooldown so the next Allow() transitions Open -> HalfOpen.
	time.Sleep(45 * time.Millisecond)

	checker := NewChecker([]*backend.Backend{b}, time.Second, "/health")
	checker.CheckAll()

	// Health checker must inspect b.IsUp() rather than b.IsAlive(), leaving the
	// half-open trial probe slot untouched for real incoming traffic.
	if state := b.CircuitBreaker.State(); state != backend.StateHalfOpen {
		t.Fatalf("expected circuit state HALF_OPEN after cooldown elapsed, got %s", state)
	}
	if !b.IsAlive() {
		t.Fatal("expected first real traffic check via IsAlive() to receive the half-open trial probe slot (health check consumed it)")
	}
	if b.IsAlive() {
		t.Fatal("expected second concurrent check while probe is in-flight to be rejected")
	}
}
