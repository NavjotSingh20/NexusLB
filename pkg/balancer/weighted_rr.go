package balancer

import (
	"net/http"
	"sync"

	"nexuslb/pkg/backend"
)

// WeightedRoundRobin implements the Smooth Weighted Round-Robin (SWRR) algorithm
// originally popularized by NGINX. For arbitrary node weights, SWRR smoothly
// interleaves selections without burst-clustering on high-weight nodes.
type WeightedRoundRobin struct {
	backends []*backend.Backend
	mux      sync.Mutex
}

func NewWeightedRoundRobin(backends []*backend.Backend) *WeightedRoundRobin {
	return &WeightedRoundRobin{
		backends: backends,
	}
}

func (wrr *WeightedRoundRobin) Name() string {
	return "Weighted Round Robin"
}

// NextBackend selects the next backend according to the Smooth Weighted Round-Robin algorithm:
//  1. For each healthy backend i: b.CurrentWeight += b.GetWeight()
//  2. Select backend m with maximum b.CurrentWeight
//  3. Update: m.CurrentWeight -= TotalWeight(all healthy backends)
//  4. Return m
func (wrr *WeightedRoundRobin) NextBackend(r *http.Request) (*backend.Backend, error) {
	wrr.mux.Lock()
	defer wrr.mux.Unlock()

	n := len(wrr.backends)
	if n == 0 {
		return nil, ErrNoHealthyBackends
	}

	var best *backend.Backend
	var totalWeight int64

	for _, b := range wrr.backends {
		if !b.IsAlive() {
			continue
		}

		effWeight := b.GetWeight()
		if effWeight <= 0 {
			effWeight = 1
		}

		totalWeight += effWeight
		b.CurrentWeight += effWeight

		if best == nil || b.CurrentWeight > best.CurrentWeight {
			best = b
		}
	}

	if best == nil {
		return nil, ErrNoHealthyBackends
	}

	best.CurrentWeight -= totalWeight
	return best, nil
}

func (wrr *WeightedRoundRobin) GetBackends() []*backend.Backend {
	wrr.mux.Lock()
	defer wrr.mux.Unlock()
	copied := make([]*backend.Backend, len(wrr.backends))
	copy(copied, wrr.backends)
	return copied
}
