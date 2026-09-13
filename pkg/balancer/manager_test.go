package balancer

import (
	"net/http"
	"sync"
	"testing"

	"nexuslb/pkg/backend"
)

func TestStrategyManager_HotSwapping(t *testing.T) {
	b1, _ := backend.NewBackend("http://localhost:8001")
	b2, _ := backend.NewBackend("http://localhost:8002")

	sm, err := NewStrategyManager("round_robin", []*backend.Backend{b1, b2})
	if err != nil {
		t.Fatalf("Failed to create StrategyManager: %v", err)
	}

	if sm.Name() != "Round Robin" {
		t.Errorf("Expected Round Robin, got %s", sm.Name())
	}

	// Hot-swap to Least Connections
	if err := sm.SetStrategy("least_connections"); err != nil {
		t.Fatalf("Failed to switch to least_connections: %v", err)
	}
	if sm.Name() != "Least Connections" {
		t.Errorf("Expected Least Connections, got %s", sm.Name())
	}
	if sm.CurrentKey() != StrategyLeastConnections {
		t.Errorf("Expected key %s, got %s", StrategyLeastConnections, sm.CurrentKey())
	}

	// Hot-swap to IP Hash
	if err := sm.SetStrategy("ip_hash"); err != nil {
		t.Fatalf("Failed to switch to ip_hash: %v", err)
	}
	if sm.Name() != "IP Hash" {
		t.Errorf("Expected IP Hash, got %s", sm.Name())
	}

	// Reject invalid strategy
	if err := sm.SetStrategy("random_invalid"); err == nil {
		t.Errorf("Expected error when setting invalid strategy, got nil")
	}
}

func TestStrategyManager_ConcurrentTrafficAndSwapping(t *testing.T) {
	b1, _ := backend.NewBackend("http://localhost:8001")
	b2, _ := backend.NewBackend("http://localhost:8002")
	b3, _ := backend.NewBackend("http://localhost:8003")

	sm, _ := NewStrategyManager("round_robin", []*backend.Backend{b1, b2, b3})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 5 worker goroutines pounding the balancer
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			req, _ := http.NewRequest("GET", "/", nil)
			req.Header.Set("X-Forwarded-For", "192.168.1.10")

			for {
				select {
				case <-stop:
					return
				default:
					_, err := sm.NextBackend(req)
					if err != nil {
						t.Errorf("Worker %d: error selecting backend: %v", workerID, err)
					}
				}
			}
		}(i)
	}

	// Concurrently swap strategies back and forth
	strategies := []string{"round_robin", "least_connections", "ip_hash"}
	for i := 0; i < 100; i++ {
		strat := strategies[i%len(strategies)]
		if err := sm.SetStrategy(strat); err != nil {
			t.Errorf("Concurrent SetStrategy error: %v", err)
		}
	}

	close(stop)
	wg.Wait()
}
