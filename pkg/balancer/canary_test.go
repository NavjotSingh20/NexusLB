package balancer

import (
	"net/http"
	"testing"

	"nexuslb/pkg/backend"
)

func TestCanaryRouting_HeaderAndCookie(t *testing.T) {
	b1, _ := backend.NewBackend("http://127.0.0.1:8001") // Stable
	b2, _ := backend.NewBackend("http://127.0.0.1:8002") // Stable
	b3, _ := backend.NewBackend("http://127.0.0.1:8003") // Canary

	b3.SetCanary(true, "v2.0-canary")

	sm, err := NewStrategyManager(StrategyRoundRobin, []*backend.Backend{b1, b2, b3})
	if err != nil {
		t.Fatalf("failed to create StrategyManager: %v", err)
	}

	// 1. Standard Request without canary indicator -> standard Round Robin between all nodes
	reqStandard, _ := http.NewRequest("GET", "http://localhost:8080/", nil)
	nodeStandard, err := sm.NextBackend(reqStandard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodeStandard == nil {
		t.Fatalf("expected backend, got nil")
	}

	// 2. Request with X-Canary header -> ALWAYS routes to b3 (Canary)
	reqHeader, _ := http.NewRequest("GET", "http://localhost:8080/", nil)
	reqHeader.Header.Set("X-Canary", "true")

	for i := 0; i < 10; i++ {
		selected, err := sm.NextBackend(reqHeader)
		if err != nil {
			t.Fatalf("unexpected error on canary request: %v", err)
		}
		if selected.URL.String() != "http://127.0.0.1:8003" {
			t.Errorf("expected canary backend 8003, got %s", selected.URL.String())
		}
		if !selected.IsCanaryNode() {
			t.Errorf("expected node to be marked as canary")
		}
	}

	// 3. Request with query param ?canary=true -> routes to b3
	reqQuery, _ := http.NewRequest("GET", "http://localhost:8080/?canary=true", nil)
	selectedQuery, err := sm.NextBackend(reqQuery)
	if err != nil || selectedQuery.URL.String() != "http://127.0.0.1:8003" {
		t.Errorf("expected query param canary to hit 8003, got %v", selectedQuery)
	}

	// 4. Request with cookie canary=true -> routes to b3
	reqCookie, _ := http.NewRequest("GET", "http://localhost:8080/", nil)
	reqCookie.AddCookie(&http.Cookie{Name: "canary", Value: "true"})
	selectedCookie, err := sm.NextBackend(reqCookie)
	if err != nil || selectedCookie.URL.String() != "http://127.0.0.1:8003" {
		t.Errorf("expected cookie canary to hit 8003, got %v", selectedCookie)
	}
}

func TestCanaryRouting_FallbackWhenCanaryOffline(t *testing.T) {
	b1, _ := backend.NewBackend("http://127.0.0.1:8001") // Stable
	b2, _ := backend.NewBackend("http://127.0.0.1:8002") // Stable
	b3, _ := backend.NewBackend("http://127.0.0.1:8003") // Canary

	b3.SetCanary(true, "v2.0-canary")
	// Simulate Canary server outage
	b3.SetAlive(false)

	sm, _ := NewStrategyManager(StrategyRoundRobin, []*backend.Backend{b1, b2, b3})

	reqCanary, _ := http.NewRequest("GET", "http://localhost:8080/", nil)
	reqCanary.Header.Set("X-Canary", "true")

	// Even though client requested canary, b3 is dead so system MUST fallback to healthy nodes (zero dropped requests)
	for i := 0; i < 10; i++ {
		selected, err := sm.NextBackend(reqCanary)
		if err != nil {
			t.Fatalf("unexpected error on fallback: %v", err)
		}
		if selected.URL.String() == "http://127.0.0.1:8003" {
			t.Fatalf("should not route to dead canary node")
		}
	}
}
