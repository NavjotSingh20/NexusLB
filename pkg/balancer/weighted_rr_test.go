package balancer

import (
	"net/http"
	"testing"

	"nexuslb/pkg/backend"
)

func TestWeightedRoundRobin_SmoothInterleaving(t *testing.T) {
	b1, _ := backend.NewBackend("http://127.0.0.1:8001")
	b2, _ := backend.NewBackend("http://127.0.0.1:8002")
	b3, _ := backend.NewBackend("http://127.0.0.1:8003")

	b1.SetWeight(5)
	b2.SetWeight(1)
	b3.SetWeight(1)

	wrr := NewWeightedRoundRobin([]*backend.Backend{b1, b2, b3})

	req, _ := http.NewRequest("GET", "http://localhost:8080/", nil)

	// In Smooth Weighted Round-Robin (SWRR), 7 requests with weights {5, 1, 1}
	// must interleave smoothly as: B1, B1, B2, B1, B3, B1, B1
	expected := []string{
		"http://127.0.0.1:8001",
		"http://127.0.0.1:8001",
		"http://127.0.0.1:8002",
		"http://127.0.0.1:8001",
		"http://127.0.0.1:8003",
		"http://127.0.0.1:8001",
		"http://127.0.0.1:8001",
	}

	for i, exp := range expected {
		selected, err := wrr.NextBackend(req)
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i+1, err)
		}
		if selected.URL.String() != exp {
			t.Errorf("request %d: expected %s, got %s", i+1, exp, selected.URL.String())
		}
	}

	// Over 70 requests, verify exact proportions: 50, 10, 10
	counts := make(map[string]int)
	for i := 0; i < 70; i++ {
		selected, err := wrr.NextBackend(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		counts[selected.URL.String()]++
	}

	if counts["http://127.0.0.1:8001"] != 50 {
		t.Errorf("expected 50 for b1, got %d", counts["http://127.0.0.1:8001"])
	}
	if counts["http://127.0.0.1:8002"] != 10 {
		t.Errorf("expected 10 for b2, got %d", counts["http://127.0.0.1:8002"])
	}
	if counts["http://127.0.0.1:8003"] != 10 {
		t.Errorf("expected 10 for b3, got %d", counts["http://127.0.0.1:8003"])
	}
}

func TestWeightedRoundRobin_SkipsUnhealthy(t *testing.T) {
	b1, _ := backend.NewBackend("http://127.0.0.1:8001")
	b2, _ := backend.NewBackend("http://127.0.0.1:8002")
	b3, _ := backend.NewBackend("http://127.0.0.1:8003")

	b1.SetWeight(10)
	b2.SetWeight(2)
	b3.SetWeight(1)

	// Simulate b1 outage
	b1.SetAlive(false)

	wrr := NewWeightedRoundRobin([]*backend.Backend{b1, b2, b3})
	req, _ := http.NewRequest("GET", "http://localhost:8080/", nil)

	// Traffic should only distribute between b2 and b3 (weights 2 and 1)
	counts := make(map[string]int)
	for i := 0; i < 30; i++ {
		selected, err := wrr.NextBackend(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		counts[selected.URL.String()]++
	}

	if counts["http://127.0.0.1:8001"] != 0 {
		t.Errorf("expected 0 requests to offline b1, got %d", counts["http://127.0.0.1:8001"])
	}
	if counts["http://127.0.0.1:8002"] != 20 {
		t.Errorf("expected 20 requests to b2, got %d", counts["http://127.0.0.1:8002"])
	}
	if counts["http://127.0.0.1:8003"] != 10 {
		t.Errorf("expected 10 requests to b3, got %d", counts["http://127.0.0.1:8003"])
	}
}

func TestWeightedRoundRobin_AllUnhealthy(t *testing.T) {
	b1, _ := backend.NewBackend("http://127.0.0.1:8001")
	b1.SetAlive(false)

	wrr := NewWeightedRoundRobin([]*backend.Backend{b1})
	req, _ := http.NewRequest("GET", "http://localhost:8080/", nil)

	_, err := wrr.NextBackend(req)
	if err != ErrNoHealthyBackends {
		t.Fatalf("expected ErrNoHealthyBackends, got %v", err)
	}
}
