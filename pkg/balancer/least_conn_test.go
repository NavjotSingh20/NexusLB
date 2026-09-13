package balancer

import (
	"net/http"
	"testing"

	"nexuslb/pkg/backend"
)

func TestLeastConnections_Selection(t *testing.T) {
	b1, _ := backend.NewBackend("http://localhost:8001")
	b2, _ := backend.NewBackend("http://localhost:8002")
	b3, _ := backend.NewBackend("http://localhost:8003")

	// Set connection counts: b1=5, b2=2, b3=8
	b1.ActiveConnections = 5
	b2.ActiveConnections = 2
	b3.ActiveConnections = 8

	lc := NewLeastConnections([]*backend.Backend{b1, b2, b3})
	req, _ := http.NewRequest("GET", "/", nil)

	// b2 has the least connections (2), so it must be selected
	chosen, err := lc.NextBackend(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if chosen.URL.String() != "http://localhost:8002" {
		t.Errorf("Expected b2 (lowest conns 2), got %s", chosen.URL.String())
	}

	// Now b2 gets busy (conns=10), b1 now has the least (5)
	b2.ActiveConnections = 10
	chosen2, err := lc.NextBackend(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if chosen2.URL.String() != "http://localhost:8001" {
		t.Errorf("Expected b1 (lowest conns 5), got %s", chosen2.URL.String())
	}
}

func TestLeastConnections_TieBreaking(t *testing.T) {
	b1, _ := backend.NewBackend("http://localhost:8001")
	b2, _ := backend.NewBackend("http://localhost:8002")
	b3, _ := backend.NewBackend("http://localhost:8003")

	// All backends have equal connections (0)
	lc := NewLeastConnections([]*backend.Backend{b1, b2, b3})
	req, _ := http.NewRequest("GET", "/", nil)

	// In a tie, circular offset should distribute sequentially across all nodes
	seen := make(map[string]int)
	for i := 0; i < 6; i++ {
		b, err := lc.NextBackend(req)
		if err != nil {
			t.Fatalf("Unexpected error at iteration %d: %v", i, err)
		}
		seen[b.URL.String()]++
	}

	// Each backend must have been selected exactly 2 times
	for _, b := range []*backend.Backend{b1, b2, b3} {
		if seen[b.URL.String()] != 2 {
			t.Errorf("Backend %s expected 2 selections, got %d", b.URL.String(), seen[b.URL.String()])
		}
	}
}

func TestLeastConnections_SkipsUnhealthy(t *testing.T) {
	b1, _ := backend.NewBackend("http://localhost:8001")
	b2, _ := backend.NewBackend("http://localhost:8002")

	// b1 has fewer connections, but is DEAD
	b1.ActiveConnections = 0
	b1.SetAlive(false)

	// b2 has more connections, but is ALIVE
	b2.ActiveConnections = 5
	b2.SetAlive(true)

	lc := NewLeastConnections([]*backend.Backend{b1, b2})
	req, _ := http.NewRequest("GET", "/", nil)

	chosen, err := lc.NextBackend(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if chosen.URL.String() != "http://localhost:8002" {
		t.Errorf("Expected b2 to be chosen because b1 is dead, got %s", chosen.URL.String())
	}

	// When all are dead, return ErrNoHealthyBackends
	b2.SetAlive(false)
	_, err = lc.NextBackend(req)
	if err != ErrNoHealthyBackends {
		t.Errorf("Expected ErrNoHealthyBackends, got %v", err)
	}
}
