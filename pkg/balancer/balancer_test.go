package balancer

import (
	"net/http"
	"testing"

	"nexuslb/pkg/backend"
)

func TestRoundRobin(t *testing.T) {
	b1, _ := backend.NewBackend("http://localhost:8001")
	b2, _ := backend.NewBackend("http://localhost:8002")
	b3, _ := backend.NewBackend("http://localhost:8003")

	rr := NewRoundRobin([]*backend.Backend{b1, b2, b3})
	req, _ := http.NewRequest("GET", "/", nil)

	// 1. Verify sequential round robin cycling
	expectedSequence := []string{
		"http://localhost:8001",
		"http://localhost:8002",
		"http://localhost:8003",
		"http://localhost:8001",
	}

	for i, expected := range expectedSequence {
		b, err := rr.NextBackend(req)
		if err != nil {
			t.Fatalf("Step %d: unexpected error: %v", i, err)
		}
		if b.URL.String() != expected {
			t.Errorf("Step %d: expected %s, got %s", i, expected, b.URL.String())
		}
	}

	// 2. Mark b2 as unhealthy, ensure it's skipped
	b2.SetAlive(false)

	for i := 0; i < 4; i++ {
		b, err := rr.NextBackend(req)
		if err != nil {
			t.Fatalf("Step %d after failure: unexpected error: %v", i, err)
		}
		if b.URL.String() == "http://localhost:8002" {
			t.Errorf("Step %d: unhealthy backend was selected!", i)
		}
	}

	// 3. Mark all as unhealthy, ensure ErrNoHealthyBackends is returned
	b1.SetAlive(false)
	b3.SetAlive(false)

	_, err := rr.NextBackend(req)
	if err != ErrNoHealthyBackends {
		t.Errorf("Expected ErrNoHealthyBackends when all backends dead, got %v", err)
	}
}
