package balancer

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"nexuslb/pkg/backend"
)

const (
	StrategyRoundRobin          = "round_robin"
	StrategyWeightedRoundRobin  = "weighted_round_robin"
	StrategyLeastConnections    = "least_connections"
	StrategyIPHash              = "ip_hash"
)

type StrategyManager struct {
	backends           []*backend.Backend
	roundRobin         *RoundRobin
	weightedRoundRobin *WeightedRoundRobin
	leastConnections   *LeastConnections
	ipHash             *IPHash

	canaryCounter uint64

	active    Balancer
	activeKey string
	mux       sync.RWMutex
}

func NewStrategyManager(initialStrategy string, backends []*backend.Backend) (*StrategyManager, error) {
	rr := NewRoundRobin(backends)
	wrr := NewWeightedRoundRobin(backends)
	lc := NewLeastConnections(backends)
	ih := NewIPHash(backends)

	sm := &StrategyManager{
		backends:           backends,
		roundRobin:         rr,
		weightedRoundRobin: wrr,
		leastConnections:   lc,
		ipHash:             ih,
	}

	if initialStrategy == "" {
		initialStrategy = StrategyRoundRobin
	}

	if err := sm.SetStrategy(initialStrategy); err != nil {
		return nil, err
	}

	return sm, nil
}

// NextBackend applies Layer 7 Canary Traffic Steering first.
// If the incoming request carries canary indicators (X-Canary header, query param, or cookie),
// it routes to an active canary node. Otherwise, it delegates to the active strategy.
func (sm *StrategyManager) NextBackend(r *http.Request) (*backend.Backend, error) {
	sm.mux.RLock()
	defer sm.mux.RUnlock()

	// 1. Layer 7 Canary Steering
	if IsCanaryRequest(r) {
		if canaryTarget := sm.selectHealthyCanary(); canaryTarget != nil {
			return canaryTarget, nil
		}
		// Graceful fallback to active strategy if canary node is offline or unassigned
	}

	// 2. Active load balancing strategy
	return sm.active.NextBackend(r)
}

func (sm *StrategyManager) selectHealthyCanary() *backend.Backend {
	var candidates []*backend.Backend
	for _, b := range sm.backends {
		if b.IsCanaryNode() && b.IsAlive() {
			candidates = append(candidates, b)
		}
	}

	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0]
	}

	idx := atomic.AddUint64(&sm.canaryCounter, 1) - 1
	return candidates[int(idx)%len(candidates)]
}

// IsCanaryRequest checks if an HTTP request explicitly targets the canary release
func IsCanaryRequest(r *http.Request) bool {
	if r == nil {
		return false
	}
	if r.Header.Get("X-Canary") != "" {
		return true
	}
	if r.URL != nil && r.URL.Query().Get("canary") == "true" {
		return true
	}
	if cookie, err := r.Cookie("canary"); err == nil && cookie.Value == "true" {
		return true
	}
	return false
}

func (sm *StrategyManager) GetBackends() []*backend.Backend {
	sm.mux.RLock()
	defer sm.mux.RUnlock()
	copied := make([]*backend.Backend, len(sm.backends))
	copy(copied, sm.backends)
	return copied
}

func (sm *StrategyManager) Name() string {
	sm.mux.RLock()
	defer sm.mux.RUnlock()
	return sm.active.Name()
}

func (sm *StrategyManager) CurrentKey() string {
	sm.mux.RLock()
	defer sm.mux.RUnlock()
	return sm.activeKey
}

func (sm *StrategyManager) SetStrategy(name string) error {
	sm.mux.Lock()
	defer sm.mux.Unlock()

	norm := normalizeStrategyName(name)
	switch norm {
	case StrategyRoundRobin:
		sm.active = sm.roundRobin
		sm.activeKey = StrategyRoundRobin
	case StrategyWeightedRoundRobin:
		sm.active = sm.weightedRoundRobin
		sm.activeKey = StrategyWeightedRoundRobin
	case StrategyLeastConnections:
		sm.active = sm.leastConnections
		sm.activeKey = StrategyLeastConnections
	case StrategyIPHash:
		sm.active = sm.ipHash
		sm.activeKey = StrategyIPHash
	default:
		return fmt.Errorf("unsupported balancing strategy '%s', available: round_robin, weighted_round_robin, least_connections, ip_hash", name)
	}

	return nil
}

func (sm *StrategyManager) AvailableStrategies() []string {
	return []string{StrategyRoundRobin, StrategyWeightedRoundRobin, StrategyLeastConnections, StrategyIPHash}
}

func normalizeStrategyName(s string) string {
	lower := strings.ToLower(strings.TrimSpace(s))
	lower = strings.ReplaceAll(lower, "-", "_")
	if lower == "weighted_round_robin" || lower == "weighted_rr" || lower == "wrr" || lower == "weighted" {
		return StrategyWeightedRoundRobin
	}
	if lower == "least_conn" || lower == "leastconn" {
		return StrategyLeastConnections
	}
	if lower == "roundrobin" || lower == "rr" {
		return StrategyRoundRobin
	}
	if lower == "iphash" {
		return StrategyIPHash
	}
	return lower
}
