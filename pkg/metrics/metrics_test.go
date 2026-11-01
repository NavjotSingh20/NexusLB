package metrics

import (
	"sync"
	"testing"
	"time"

	"nexuslb/pkg/backend"
)

func TestCollector_BasicRecording(t *testing.T) {
	c := NewCollector()

	c.Record("GET", "/test", "127.0.0.1", "http://localhost:8001", 200, 15*time.Millisecond)
	c.Record("POST", "/data", "127.0.0.1", "http://localhost:8002", 404, 25*time.Millisecond)
	c.Record("GET", "/error", "127.0.0.1", "http://localhost:8003", 500, 50*time.Millisecond)
	c.Record("GET", "/spam", "127.0.0.1", "Edge Shield", 429, 2*time.Millisecond)
	c.Record("GET", "/canary", "127.0.0.1", "http://localhost:8003", 200, 10*time.Millisecond, true)

	snap := c.Snapshot("Round Robin", nil)

	if snap.TotalRequests != 5 {
		t.Errorf("expected 5 total requests, got %d", snap.TotalRequests)
	}
	if snap.TotalSuccess != 2 {
		t.Errorf("expected 2 success, got %d", snap.TotalSuccess)
	}
	if snap.TotalClientErrors != 2 { // 404 and 429
		t.Errorf("expected 2 client errors, got %d", snap.TotalClientErrors)
	}
	if snap.TotalServerErrors != 1 {
		t.Errorf("expected 1 server error, got %d", snap.TotalServerErrors)
	}
	if snap.TotalThrottled != 1 {
		t.Errorf("expected 1 throttled request, got %d", snap.TotalThrottled)
	}
	if snap.TotalCanaryRequests != 1 {
		t.Errorf("expected 1 canary request, got %d", snap.TotalCanaryRequests)
	}
	if snap.Strategy != "Round Robin" {
		t.Errorf("expected strategy 'Round Robin', got %s", snap.Strategy)
	}
}

func TestCollector_LogRingBuffer(t *testing.T) {
	c := NewCollector()

	// Push 70 logs (max is 50)
	for i := 1; i <= 70; i++ {
		c.Record("GET", "/path", "127.0.0.1", "http://localhost:8001", 200, time.Duration(i)*time.Millisecond)
	}

	snap := c.Snapshot("Round Robin", nil)
	if len(snap.RecentLogs) != 50 {
		t.Errorf("expected 50 recent logs, got %d", len(snap.RecentLogs))
	}

	// Newest log should appear first in snapshot
	if snap.RecentLogs[0].LatencyMs != 70 {
		t.Errorf("expected newest log latency 70ms first, got %d", snap.RecentLogs[0].LatencyMs)
	}
	if snap.RecentLogs[49].LatencyMs != 21 {
		t.Errorf("expected oldest log latency 21ms at end, got %d", snap.RecentLogs[49].LatencyMs)
	}
}

func TestCollector_QueueManagement(t *testing.T) {
	c := NewCollector()

	c.IncQueue(5)
	if q := c.GetQueue(); q != 5 {
		t.Errorf("expected queue 5, got %d", q)
	}

	c.DecQueue()
	if q := c.GetQueue(); q != 4 {
		t.Errorf("expected queue 4, got %d", q)
	}

	// Negative protection
	c.DecQueue()
	c.DecQueue()
	c.DecQueue()
	c.DecQueue()
	c.DecQueue() // Should be -1, but GetQueue clamps to 0
	if q := c.GetQueue(); q != 0 {
		t.Errorf("expected clamped queue 0, got %d", q)
	}
}

func TestCollector_Reset(t *testing.T) {
	c := NewCollector()
	b, _ := backend.NewBackend("http://127.0.0.1:8001")
	b.RecordRequest(50*time.Millisecond, false)

	c.Record("GET", "/test", "127.0.0.1", "http://localhost:8001", 200, 50*time.Millisecond)
	c.Reset([]*backend.Backend{b})

	snap := c.Snapshot("Round Robin", []*backend.Backend{b})
	if snap.TotalRequests != 0 {
		t.Errorf("expected 0 total requests after reset, got %d", snap.TotalRequests)
	}
	if len(snap.RecentLogs) != 0 {
		t.Errorf("expected empty logs after reset, got %d", len(snap.RecentLogs))
	}
	if snap.Backends[0].TotalRequests != 0 {
		t.Errorf("expected backend total requests reset to 0, got %d", snap.Backends[0].TotalRequests)
	}
}

func TestCollector_Concurrency(t *testing.T) {
	c := NewCollector()
	var wg sync.WaitGroup
	workers := 50

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c.Record("GET", "/test", "127.0.0.1", "http://localhost:8001", 200, time.Duration(id)*time.Millisecond)
			c.IncQueue(1)
			_ = c.CalculateRPS()
			_ = c.Snapshot("Round Robin", nil)
			c.DecQueue()
		}(i)
	}

	wg.Wait()

	snap := c.Snapshot("Round Robin", nil)
	if snap.TotalRequests != int64(workers) {
		t.Errorf("expected %d total requests, got %d", workers, snap.TotalRequests)
	}
}

