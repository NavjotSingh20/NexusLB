package backend

import (
	"sync"
	"testing"
	"time"
)

func TestBackend_Lifecycle(t *testing.T) {
	b, err := NewBackend("http://127.0.0.1:8001")
	if err != nil {
		t.Fatalf("unexpected error creating backend: %v", err)
	}

	if !b.IsAlive() {
		t.Errorf("expected new backend to be alive")
	}

	b.SetAlive(false)
	if b.IsAlive() {
		t.Errorf("expected backend to be offline")
	}

	b.SetAlive(true)
	if !b.IsAlive() {
		t.Errorf("expected backend to be online")
	}

	// Invalid URL test
	_, err = NewBackend(":%invalid-url")
	if err == nil {
		t.Errorf("expected error for malformed URL")
	}
}

func TestBackend_ConnectionTracking(t *testing.T) {
	b, _ := NewBackend("http://127.0.0.1:8001")

	b.IncConn()
	b.IncConn()
	if conns := b.GetStats().ActiveConnections; conns != 2 {
		t.Errorf("expected 2 active connections, got %d", conns)
	}

	b.DecConn()
	if conns := b.GetStats().ActiveConnections; conns != 1 {
		t.Errorf("expected 1 active connection, got %d", conns)
	}

	b.SetActiveConnections(15)
	if conns := b.GetStats().ActiveConnections; conns != 15 {
		t.Errorf("expected 15 active connections, got %d", conns)
	}

	// Negative protection
	b.SetActiveConnections(-5)
	if conns := b.GetStats().ActiveConnections; conns != 0 {
		t.Errorf("expected clamp to 0, got %d", conns)
	}
}

func TestBackend_WeightAndCanary(t *testing.T) {
	b, _ := NewBackend("http://127.0.0.1:8001")

	if w := b.GetWeight(); w != 1 {
		t.Errorf("expected default weight 1, got %d", w)
	}

	b.SetWeight(10)
	if w := b.GetWeight(); w != 10 {
		t.Errorf("expected weight 10, got %d", w)
	}

	b.SetWeight(0)
	if w := b.GetWeight(); w != 1 {
		t.Errorf("expected clamp to 1 for zero weight, got %d", w)
	}

	// Canary
	if b.IsCanaryNode() {
		t.Errorf("expected default isCanary to be false")
	}

	b.SetCanary(true, "v2.1-beta")
	if !b.IsCanaryNode() {
		t.Errorf("expected isCanary to be true")
	}
	if tag := b.GetCanaryTag(); tag != "v2.1-beta" {
		t.Errorf("expected canary tag 'v2.1-beta', got %s", tag)
	}

	b.SetCanary(true, "")
	if tag := b.GetCanaryTag(); tag != "v2.0-canary" {
		t.Errorf("expected default tag when empty, got %s", tag)
	}
}

func TestBackend_RecordRequestAndStats(t *testing.T) {
	b, _ := NewBackend("http://127.0.0.1:8001")

	b.RecordRequest(50*time.Millisecond, false)
	b.RecordRequest(120*time.Millisecond, true)

	stats := b.GetStats()
	if stats.TotalRequests != 2 {
		t.Errorf("expected 2 total requests, got %d", stats.TotalRequests)
	}
	if stats.TotalErrors != 1 {
		t.Errorf("expected 1 total error, got %d", stats.TotalErrors)
	}
	if stats.LastLatencyMs != 120 {
		t.Errorf("expected last latency 120ms, got %d", stats.LastLatencyMs)
	}
}

func TestBackend_ConcurrentAccess(t *testing.T) {
	b, _ := NewBackend("http://127.0.0.1:8001")

	var wg sync.WaitGroup
	workers := 50

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			b.IncConn()
			b.RecordRequest(time.Duration(id)*time.Millisecond, id%2 == 0)
			_ = b.IsAlive()
			_ = b.GetWeight()
			_ = b.GetStats()
			b.DecConn()
		}(i)
	}

	wg.Wait()

	stats := b.GetStats()
	if stats.ActiveConnections != 0 {
		t.Errorf("expected 0 balanced active connections, got %d", stats.ActiveConnections)
	}
	if stats.TotalRequests != int64(workers) {
		t.Errorf("expected %d total requests, got %d", workers, stats.TotalRequests)
	}
}

func TestCircuitBreaker_StateTransitions(t *testing.T) {
	// 3 consecutive failures to trip, 50ms cooldown
	cb := NewCircuitBreaker(3, 50*time.Millisecond)

	if cb.State() != StateClosed {
		t.Errorf("expected initial state StateClosed, got %s", cb.State())
	}
	if !cb.Allow() {
		t.Errorf("expected Allow() to be true when closed")
	}

	// 1 failure: stays closed
	cb.RecordFailure()
	if cb.State() != StateClosed || cb.Failures() != 1 {
		t.Errorf("expected 1 failure and StateClosed, got %d and %s", cb.Failures(), cb.State())
	}

	// 2 failures: stays closed
	cb.RecordFailure()
	if cb.State() != StateClosed || cb.Failures() != 2 {
		t.Errorf("expected 2 failures and StateClosed, got %d and %s", cb.Failures(), cb.State())
	}

	// 3rd failure: trips to StateOpen
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Errorf("expected StateOpen after 3 failures, got %s", cb.State())
	}
	if cb.Allow() {
		t.Errorf("expected Allow() to be false when open")
	}

	// Wait for cooldown to expire
	time.Sleep(60 * time.Millisecond)

	// State should transition to StateHalfOpen
	if cb.State() != StateHalfOpen {
		t.Errorf("expected StateHalfOpen after cooldown, got %s", cb.State())
	}

	// First probe request allowed in half-open
	if !cb.Allow() {
		t.Errorf("expected 1st probe to be allowed in StateHalfOpen")
	}
	// Second concurrent probe rejected
	if cb.Allow() {
		t.Errorf("expected 2nd probe to be rejected while probing in StateHalfOpen")
	}

	// Probe succeeds: transitions back to StateClosed
	cb.RecordSuccess()
	if cb.State() != StateClosed {
		t.Errorf("expected StateClosed after probe success, got %s", cb.State())
	}
	if cb.Failures() != 0 {
		t.Errorf("expected 0 failures after probe success, got %d", cb.Failures())
	}

	// Now test probe failure: trip to open again
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Errorf("expected StateOpen, got %s", cb.State())
	}
	time.Sleep(60 * time.Millisecond)
	_ = cb.Allow() // probe allowed
	cb.RecordFailure() // probe failed!
	if cb.State() != StateOpen {
		t.Errorf("expected StateOpen immediately after probe failure in half-open, got %s", cb.State())
	}

	// Reset
	cb.Reset()
	if cb.State() != StateClosed || cb.Failures() != 0 {
		t.Errorf("expected clean reset, got %s", cb.State())
	}
}

func TestBackend_CircuitBreakerIntegration(t *testing.T) {
	b, _ := NewBackend("http://127.0.0.1:8001")
	// Use 3 failures, 50ms cooldown for fast test
	b.CircuitBreaker = NewCircuitBreaker(3, 50*time.Millisecond)

	if !b.IsAlive() {
		t.Errorf("expected backend to be alive initially")
	}

	// Record 3 error requests
	b.RecordRequest(10*time.Millisecond, true)
	b.RecordRequest(10*time.Millisecond, true)
	b.RecordRequest(10*time.Millisecond, true)

	// Circuit breaker should now be OPEN, so IsAlive() returns false
	if b.IsAlive() {
		t.Errorf("expected b.IsAlive() to return false when circuit breaker is open")
	}
	stats := b.GetStats()
	if stats.CircuitState != "open" {
		t.Errorf("expected CircuitState 'open', got %s", stats.CircuitState)
	}
	if stats.Alive {
		t.Errorf("expected stats.Alive to be false when circuit breaker is open")
	}
}

