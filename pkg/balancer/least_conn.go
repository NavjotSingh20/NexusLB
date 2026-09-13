package balancer

import (
	"net/http"
	"sync"
	"sync/atomic"

	"nexuslb/pkg/backend"
)

type LeastConnections struct {
	backends []*backend.Backend
	offset   uint64
	mux      sync.RWMutex
}

func NewLeastConnections(backends []*backend.Backend) *LeastConnections {
	return &LeastConnections{
		backends: backends,
	}
}

func (lc *LeastConnections) NextBackend(r *http.Request) (*backend.Backend, error) {
	lc.mux.RLock()
	defer lc.mux.RUnlock()

	n := len(lc.backends)
	if n == 0 {
		return nil, ErrNoHealthyBackends
	}

	// Circular offset for fair tie-breaking when multiple backends have equal connections
	startIdx := int(atomic.AddUint64(&lc.offset, 1) - 1) % n

	var chosen *backend.Backend
	minConns := int64(-1)

	for i := 0; i < n; i++ {
		idx := (startIdx + i) % n
		candidate := lc.backends[idx]

		if !candidate.IsAlive() {
			continue
		}

		conns := atomic.LoadInt64(&candidate.ActiveConnections)
		if minConns == -1 || conns < minConns {
			minConns = conns
			chosen = candidate
		}
	}

	if chosen == nil {
		return nil, ErrNoHealthyBackends
	}

	return chosen, nil
}

func (lc *LeastConnections) GetBackends() []*backend.Backend {
	lc.mux.RLock()
	defer lc.mux.RUnlock()
	copied := make([]*backend.Backend, len(lc.backends))
	copy(copied, lc.backends)
	return copied
}

func (lc *LeastConnections) Name() string {
	return "Least Connections"
}
