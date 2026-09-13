package balancer

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"nexuslb/pkg/backend"
)

const (
	StrategyRoundRobin       = "round_robin"
	StrategyLeastConnections = "least_connections"
	StrategyIPHash           = "ip_hash"
)

type StrategyManager struct {
	backends         []*backend.Backend
	roundRobin       *RoundRobin
	leastConnections *LeastConnections
	ipHash           *IPHash

	active    Balancer
	activeKey string
	mux       sync.RWMutex
}

func NewStrategyManager(initialStrategy string, backends []*backend.Backend) (*StrategyManager, error) {
	rr := NewRoundRobin(backends)
	lc := NewLeastConnections(backends)
	ih := NewIPHash(backends)

	sm := &StrategyManager{
		backends:         backends,
		roundRobin:       rr,
		leastConnections: lc,
		ipHash:           ih,
	}

	if initialStrategy == "" {
		initialStrategy = StrategyRoundRobin
	}

	if err := sm.SetStrategy(initialStrategy); err != nil {
		return nil, err
	}

	return sm, nil
}

func (sm *StrategyManager) NextBackend(r *http.Request) (*backend.Backend, error) {
	sm.mux.RLock()
	active := sm.active
	sm.mux.RUnlock()

	return active.NextBackend(r)
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
	case StrategyLeastConnections:
		sm.active = sm.leastConnections
		sm.activeKey = StrategyLeastConnections
	case StrategyIPHash:
		sm.active = sm.ipHash
		sm.activeKey = StrategyIPHash
	default:
		return fmt.Errorf("unsupported balancing strategy '%s', available: round_robin, least_connections, ip_hash", name)
	}

	return nil
}

func (sm *StrategyManager) AvailableStrategies() []string {
	return []string{StrategyRoundRobin, StrategyLeastConnections, StrategyIPHash}
}

func normalizeStrategyName(s string) string {
	lower := strings.ToLower(strings.TrimSpace(s))
	lower = strings.ReplaceAll(lower, "-", "_")
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
