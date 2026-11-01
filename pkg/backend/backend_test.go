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

