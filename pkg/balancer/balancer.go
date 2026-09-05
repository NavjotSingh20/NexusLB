package balancer

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"

	"nexuslb/pkg/backend"
)

var ErrNoHealthyBackends = errors.New("no healthy backends available")

type Balancer interface {
	NextBackend(r *http.Request) (*backend.Backend, error)
	GetBackends() []*backend.Backend
	Name() string
}

type RoundRobin struct {
	backends []*backend.Backend
	current  uint64
	mux      sync.RWMutex
}

func NewRoundRobin(backends []*backend.Backend) *RoundRobin {
	return &RoundRobin{
		backends: backends,
	}
}

func (rr *RoundRobin) NextBackend(r *http.Request) (*backend.Backend, error) {
	rr.mux.RLock()
	defer rr.mux.RUnlock()

	n := len(rr.backends)
	if n == 0 {
		return nil, ErrNoHealthyBackends
	}

	startIdx := int(atomic.AddUint64(&rr.current, 1) - 1) % n

	// Check if the current candidate is alive, or cycle through the pool
	for i := 0; i < n; i++ {
		idx := (startIdx + i) % n
		candidate := rr.backends[idx]
		if candidate.IsAlive() {
			return candidate, nil
		}
	}

	return nil, ErrNoHealthyBackends
}

func (rr *RoundRobin) GetBackends() []*backend.Backend {
	rr.mux.RLock()
	defer rr.mux.RUnlock()
	copied := make([]*backend.Backend, len(rr.backends))
	copy(copied, rr.backends)
	return copied
}

func (rr *RoundRobin) Name() string {
	return "Round Robin"
}
