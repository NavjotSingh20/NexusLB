package balancer

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"nexuslb/pkg/backend"
)

func TestAdaptiveBalancer_AllFourTransitionsAndPriority(t *testing.T) {
	b1, _ := backend.NewBackend("http://localhost:8001")
	b2, _ := backend.NewBackend("http://localhost:8002")
	b3, _ := backend.NewBackend("http://localhost:8003")
	backends := []*backend.Backend{b1, b2, b3}

	rr := NewRoundRobin(backends)
	wrr := NewWeightedRoundRobin(backends)
	lc := NewLeastConnections(backends)
	ih := NewIPHash(backends)

	now := time.Date(2026, 11, 11, 23, 50, 0, 0, time.UTC)
	ab := NewAdaptiveBalancer(backends, rr, wrr, lc, ih)
	ab.nowFn = func() time.Time { return now }

	// 1. Initial balanced baseline -> Round Robin
	snap := ab.Evaluate()
	if snap.ActiveKey != StrategyRoundRobin {
		t.Fatalf("expected initial Round Robin, got %s (%s)", snap.ActiveKey, snap.Reason)
	}
	if snap.RuleMatched != "uniform_baseline" {
		t.Errorf("expected uniform_baseline rule, got %s", snap.RuleMatched)
	}

	// 2. Diverse clients keep Round Robin
	for i := 0; i < 10; i++ {
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set("X-Forwarded-For", fmt.Sprintf("192.168.1.%d", 10+i))
		if _, err := ab.NextBackend(req); err != nil {
			t.Fatalf("NextBackend failed: %v", err)
		}
	}
	now = now.Add(5 * time.Second)
	snap = ab.Evaluate()
	if snap.ActiveKey != StrategyRoundRobin {
		t.Errorf("expected Round Robin on diverse IPs, got %s (repeat=%.2f)", snap.ActiveKey, snap.Signals.RepeatIPRatio)
	}

	// 3. Repeat client IP concentration -> IP Hash
	ab.Reset()
	for i := 0; i < 8; i++ {
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set("X-Forwarded-For", "10.0.0.42")
		if _, err := ab.NextBackend(req); err != nil {
			t.Fatalf("NextBackend failed: %v", err)
		}
	}
	snap = ab.Evaluate()
	if snap.ActiveKey != StrategyIPHash {
		t.Fatalf("expected IP Hash on repeat IPs, got %s (repeat=%.2f, reason=%s)", snap.ActiveKey, snap.Signals.RepeatIPRatio, snap.Reason)
	}
	if snap.RuleMatched != "client_affinity" {
		t.Errorf("expected client_affinity rule, got %s", snap.RuleMatched)
	}

	// 4. Heterogeneous weights override IP Hash -> Weighted Round Robin
	b1.SetWeight(5)
	b2.SetWeight(1)
	b3.SetWeight(1)
	snap = ab.Evaluate()
	if snap.ActiveKey != StrategyWeightedRoundRobin {
		t.Fatalf("expected Weighted Round Robin on heterogeneous weights, got %s (%s)", snap.ActiveKey, snap.Reason)
	}
	if snap.RuleMatched != "capacity_weights" || snap.Signals.WeightSummary != "5:1:1" {
		t.Errorf("unexpected WRR metadata: rule=%s weights=%s", snap.RuleMatched, snap.Signals.WeightSummary)
	}

	// 5a. Socket spread overrides everything -> Least Connections
	b2.SetActiveConnections(4)
	snap = ab.Evaluate()
	if snap.ActiveKey != StrategyLeastConnections || snap.RuleMatched != "connection_skew" {
		t.Fatalf("expected Least Connections via connection_skew, got %s / %s", snap.ActiveKey, snap.RuleMatched)
	}

	// Clear socket spread & weights, expire cooldown -> settles back to IP Hash (repeat IPs still in ring)
	b2.SetActiveConnections(0)
	b1.SetWeight(1)
	now = now.Add(5 * time.Second)
	snap = ab.Evaluate()
	if snap.ActiveKey != StrategyIPHash {
		t.Fatalf("expected recovery to IP Hash after cooldown, got %s (%s)", snap.ActiveKey, snap.Reason)
	}

	// Test 5b: latency skew overrides IP Hash immediately -> Least Connections
	b1.RecordRequest(2*time.Millisecond, false)
	b2.RecordRequest(2*time.Millisecond, false)
	b3.RecordRequest(250*time.Millisecond, false)
	snap = ab.Evaluate()
	if snap.ActiveKey != StrategyLeastConnections || snap.RuleMatched != "latency_skew" {
		t.Fatalf("expected Least Connections via latency_skew, got %s / %s (%s)", snap.ActiveKey, snap.RuleMatched, snap.Reason)
	}

	if snap.SwitchCount != 5 || len(snap.History) != 5 {
		t.Errorf("expected 5 transitions in history, count=%d len=%d", snap.SwitchCount, len(snap.History))
	}
}

func TestAdaptiveBalancer_CooldownPreventsFlapping(t *testing.T) {
	b1, _ := backend.NewBackend("http://localhost:8001")
	b2, _ := backend.NewBackend("http://localhost:8002")
	backends := []*backend.Backend{b1, b2}

	ab := NewAdaptiveBalancer(
		backends,
		NewRoundRobin(backends),
		NewWeightedRoundRobin(backends),
		NewLeastConnections(backends),
		NewIPHash(backends),
	)
	now := time.Date(2026, 11, 11, 23, 55, 0, 0, time.UTC)
	ab.nowFn = func() time.Time { return now }

	// Immediate saturation escalation -> Least Connections
	b1.SetActiveConnections(3)
	snap := ab.Evaluate()
	if snap.ActiveKey != StrategyLeastConnections {
		t.Fatalf("expected immediate Least Connections escalation, got %s", snap.ActiveKey)
	}

	// Recover sockets 1s later (< 4s cooldown) -> holds Least Connections cleanly
	b1.SetActiveConnections(0)
	now = now.Add(1 * time.Second)
	snap = ab.Evaluate()
	if snap.ActiveKey != StrategyLeastConnections {
		t.Errorf("expected cooldown to hold Least Connections at 1s, got %s", snap.ActiveKey)
	}
	if snap.CooldownRemainingMs <= 0 || snap.CandidateKey != StrategyRoundRobin {
		t.Errorf("expected cooldown remaining and candidate round_robin, got rem=%d candidate=%s", snap.CooldownRemainingMs, snap.CandidateKey)
	}

	// Advance past cooldown -> settles back to Round Robin
	now = now.Add(4 * time.Second)
	snap = ab.Evaluate()
	if snap.ActiveKey != StrategyRoundRobin {
		t.Errorf("expected fallback to Round Robin after cooldown, got %s", snap.ActiveKey)
	}
}

func TestAdaptiveBalancer_ConcurrentAccess(t *testing.T) {
	b1, _ := backend.NewBackend("http://localhost:8001")
	b2, _ := backend.NewBackend("http://localhost:8002")
	b3, _ := backend.NewBackend("http://localhost:8003")
	backends := []*backend.Backend{b1, b2, b3}

	ab := NewAdaptiveBalancer(
		backends,
		NewRoundRobin(backends),
		NewWeightedRoundRobin(backends),
		NewLeastConnections(backends),
		NewIPHash(backends),
	)

	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req, _ := http.NewRequest("GET", "/", nil)
			req.Header.Set("X-Forwarded-For", fmt.Sprintf("10.1.0.%d", id))
			for i := 0; i < 40; i++ {
				if _, err := ab.NextBackend(req); err != nil {
					t.Errorf("concurrent NextBackend failed: %v", err)
				}
				if i%10 == 0 {
					_ = ab.Snapshot()
				}
			}
		}(g)
	}
	wg.Wait()
}

