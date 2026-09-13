package balancer

import (
	"net/http"
	"testing"

	"nexuslb/pkg/backend"
)

func TestIPHash_StickySession(t *testing.T) {
	b1, _ := backend.NewBackend("http://localhost:8001")
	b2, _ := backend.NewBackend("http://localhost:8002")
	b3, _ := backend.NewBackend("http://localhost:8003")

	ih := NewIPHash([]*backend.Backend{b1, b2, b3})

	// Create request with specific client IP
	req, _ := http.NewRequest("GET", "/", nil)
	req.Header.Set("X-Forwarded-For", "198.51.100.45")

	// Get first assigned backend
	firstTarget, err := ih.NextBackend(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 50 subsequent requests from the same IP MUST resolve to the exact same backend
	for i := 0; i < 50; i++ {
		target, err := ih.NextBackend(req)
		if err != nil {
			t.Fatalf("Iteration %d: error: %v", i, err)
		}
		if target.URL.String() != firstTarget.URL.String() {
			t.Fatalf("Session affinity broken! Expected %s, got %s on iteration %d",
				firstTarget.URL.String(), target.URL.String(), i)
		}
	}
}

func TestIPHash_MultipleIPDistribution(t *testing.T) {
	b1, _ := backend.NewBackend("http://localhost:8001")
	b2, _ := backend.NewBackend("http://localhost:8002")
	b3, _ := backend.NewBackend("http://localhost:8003")

	ih := NewIPHash([]*backend.Backend{b1, b2, b3})

	clientIPs := []string{
		"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5",
		"192.168.1.1", "192.168.1.2", "192.168.1.3", "172.16.0.1",
		"203.0.113.5", "203.0.113.6", "203.0.113.7", "198.51.100.1",
	}

	distribution := make(map[string]int)
	for _, ip := range clientIPs {
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set("X-Forwarded-For", ip)

		b, err := ih.NextBackend(req)
		if err != nil {
			t.Fatalf("Failed for IP %s: %v", ip, err)
		}
		distribution[b.URL.String()]++
	}

	// Make sure requests did not all pile into one server
	if len(distribution) < 2 {
		t.Errorf("Expected distribution across multiple backends, but only got %d active target(s)", len(distribution))
	}
}

func TestIPHash_FailoverAndRecovery(t *testing.T) {
	b1, _ := backend.NewBackend("http://localhost:8001")
	b2, _ := backend.NewBackend("http://localhost:8002")
	b3, _ := backend.NewBackend("http://localhost:8003")

	ih := NewIPHash([]*backend.Backend{b1, b2, b3})

	req, _ := http.NewRequest("GET", "/", nil)
	req.Header.Set("X-Forwarded-For", "192.0.2.77")

	initial, err := ih.NextBackend(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Kill the backend assigned to this client
	initial.SetAlive(false)

	// Next request must fail over to a healthy backend
	fallback, err := ih.NextBackend(req)
	if err != nil {
		t.Fatalf("Failover failed: %v", err)
	}
	if fallback.URL.String() == initial.URL.String() {
		t.Errorf("Failed over to the dead backend: %s", fallback.URL.String())
	}

	// Restore original backend
	initial.SetAlive(true)

	// Client should be seamlessly reunited with their original backend
	reunited, err := ih.NextBackend(req)
	if err != nil {
		t.Fatalf("Recovery lookup failed: %v", err)
	}
	if reunited.URL.String() != initial.URL.String() {
		t.Errorf("Expected reconnection to %s, got %s", initial.URL.String(), reunited.URL.String())
	}
}

func TestExtractClientIP(t *testing.T) {
	// 1. X-Forwarded-For with multiple proxies
	req1, _ := http.NewRequest("GET", "/", nil)
	req1.Header.Set("X-Forwarded-For", "203.0.113.195, 70.41.3.18, 150.172.238.178")
	if ip := ExtractClientIP(req1); ip != "203.0.113.195" {
		t.Errorf("Expected 203.0.113.195, got %s", ip)
	}

	// 2. X-Real-IP
	req2, _ := http.NewRequest("GET", "/", nil)
	req2.Header.Set("X-Real-IP", "198.51.100.7")
	if ip := ExtractClientIP(req2); ip != "198.51.100.7" {
		t.Errorf("Expected 198.51.100.7, got %s", ip)
	}

	// 3. RemoteAddr host:port
	req3, _ := http.NewRequest("GET", "/", nil)
	req3.RemoteAddr = "192.0.2.1:54321"
	if ip := ExtractClientIP(req3); ip != "192.0.2.1" {
		t.Errorf("Expected 192.0.2.1, got %s", ip)
	}
}
