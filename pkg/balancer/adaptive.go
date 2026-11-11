package balancer

import (
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"nexuslb/pkg/backend"
)

const (
	adaptiveIPWindowSize    = 24
	adaptiveMinIPSamples    = 6
	adaptiveRepeatIPTrigger = 0.70
	adaptiveConnSpreadTrig  = int64(2)
	adaptiveLatencyMinMs    = int64(80)
	adaptiveLatencySkewTrig = 2.5
	adaptiveDefaultCooldown = 4 * time.Second
	adaptiveEvalInterval    = 250 * time.Millisecond
	adaptiveMaxHistory      = 12
)

// AdaptiveSignals captures the live cluster metrics inspected during evaluation.
type AdaptiveSignals struct {
	HealthyNodes     int     `json:"healthy_nodes"`
	TotalNodes       int     `json:"total_nodes"`
	ConnSpread       int64   `json:"conn_spread"`
	MaxConnections   int64   `json:"max_connections"`
	MinConnections   int64   `json:"min_connections"`
	LatencySkewRatio float64 `json:"latency_skew_ratio"`
	MaxLatencyMs     int64   `json:"max_latency_ms"`
	MinLatencyMs     int64   `json:"min_latency_ms"`
	WeightsUniform   bool    `json:"weights_uniform"`
	WeightSummary    string  `json:"weight_summary"`
	RepeatIPRatio    float64 `json:"repeat_ip_ratio"`
	UniqueRecentIPs  int     `json:"unique_recent_ips"`
	RecentSampleSize int     `json:"recent_sample_size"`
}

// SwitchEvent records an observed automated transition between two concrete strategies.
type SwitchEvent struct {
	Timestamp string `json:"timestamp"`
	FromKey   string `json:"from_key"`
	FromName  string `json:"from_name"`
	ToKey     string `json:"to_key"`
	ToName    string `json:"to_name"`
	Rule      string `json:"rule"`
	Reason    string `json:"reason"`
}

// AdaptiveSnapshot exposes explainability state for dashboards and telemetry APIs.
type AdaptiveSnapshot struct {
	ActiveKey           string          `json:"active_key"`
	ActiveName          string          `json:"active_name"`
	CandidateKey        string          `json:"candidate_key"`
	RuleMatched         string          `json:"rule_matched"`
	Reason              string          `json:"reason"`
	EvaluatedAt         string          `json:"evaluated_at"`
	LastSwitchAt        string          `json:"last_switch_at,omitempty"`
	SwitchCount         int64           `json:"switch_count"`
	CooldownRemainingMs int64           `json:"cooldown_remaining_ms"`
	Signals             AdaptiveSignals `json:"signals"`
	History             []SwitchEvent   `json:"history"`
}

// AdaptiveBalancer dynamically delegates between Round Robin, Weighted Round Robin,
// Least Connections, and IP Hash based on live cluster signals and anti-flap hysteresis.
type AdaptiveBalancer struct {
	backends []*backend.Backend
	delegates map[string]Balancer

	mux             sync.RWMutex
	activeKey       string
	candidateKey    string
	ruleMatched     string
	reason          string
	lastEvaluatedAt time.Time
	lastSwitchAt    time.Time
	lastSignals     AdaptiveSignals
	switchCount     int64
	history         []SwitchEvent

	ipMux   sync.Mutex
	ipRing  [adaptiveIPWindowSize]string
	ipHead  int
	ipCount int

	lastEvalNano int64
	cooldown     time.Duration
	nowFn        func() time.Time
}

// NewAdaptiveBalancer creates an adaptive meta-balancer delegating to all 4 core strategies.
func NewAdaptiveBalancer(
	backends []*backend.Backend,
	rr *RoundRobin,
	wrr *WeightedRoundRobin,
	lc *LeastConnections,
	ih *IPHash,
) *AdaptiveBalancer {
	delegates := map[string]Balancer{
		StrategyRoundRobin:         rr,
		StrategyWeightedRoundRobin: wrr,
		StrategyLeastConnections:   lc,
		StrategyIPHash:             ih,
	}
	ab := &AdaptiveBalancer{
		backends:  backends,
		delegates: delegates,
		activeKey: StrategyRoundRobin,
		cooldown:  adaptiveDefaultCooldown,
		nowFn:     time.Now,
	}
	ab.Evaluate()
	return ab
}

func (a *AdaptiveBalancer) NextBackend(r *http.Request) (*backend.Backend, error) {
	a.recordClientIP(r)

	now := a.nowFn()
	prevNano := atomic.LoadInt64(&a.lastEvalNano)
	if now.UnixNano()-prevNano >= int64(adaptiveEvalInterval) {
		a.Evaluate()
	}

	a.mux.RLock()
	delegate := a.delegates[a.activeKey]
	a.mux.RUnlock()

	if delegate == nil {
		return nil, ErrNoHealthyBackends
	}
	return delegate.NextBackend(r)
}

func (a *AdaptiveBalancer) GetBackends() []*backend.Backend {
	a.mux.RLock()
	defer a.mux.RUnlock()
	copied := make([]*backend.Backend, len(a.backends))
	copy(copied, a.backends)
	return copied
}

func (a *AdaptiveBalancer) UpdateBackends(backends []*backend.Backend) {
	a.mux.Lock()
	a.backends = backends
	a.mux.Unlock()
	a.Evaluate()
}

func (a *AdaptiveBalancer) Name() string {
	a.mux.RLock()
	defer a.mux.RUnlock()
	return fmt.Sprintf("Smart Auto (%s)", strategyDisplayName(a.activeKey))
}

// Reset clears sliding repeat-IP history and transition log without altering static backend configs.
func (a *AdaptiveBalancer) Reset() {
	a.mux.Lock()
	a.ipMux.Lock()
	a.ipRing = [adaptiveIPWindowSize]string{}
	a.ipHead = 0
	a.ipCount = 0
	a.ipMux.Unlock()

	a.activeKey = StrategyRoundRobin
	a.switchCount = 0
	a.history = nil
	a.lastSwitchAt = time.Time{}
	a.mux.Unlock()

	a.Evaluate()
}

// Evaluate recomputes cluster signals and applies priority rules with anti-flap cooldown.
func (a *AdaptiveBalancer) Evaluate() AdaptiveSnapshot {
	a.mux.Lock()
	defer a.mux.Unlock()

	now := a.nowFn()
	atomic.StoreInt64(&a.lastEvalNano, now.UnixNano())

	signals := a.collectSignalsLocked()
	candidateKey, rule, candidateReason := decideStrategy(signals)

	var cooldownRemMs int64
	holdDueToCooldown := false

	if candidateKey != a.activeKey && !a.lastSwitchAt.IsZero() {
		elapsed := now.Sub(a.lastSwitchAt)
		isEscalation := candidateKey == StrategyLeastConnections ||
			(candidateKey == StrategyWeightedRoundRobin && a.activeKey != StrategyLeastConnections)

		if !isEscalation && elapsed < a.cooldown {
			holdDueToCooldown = true
			cooldownRemMs = (a.cooldown - elapsed).Milliseconds()
		}
	}

	if candidateKey != a.activeKey && !holdDueToCooldown {
		prevKey := a.activeKey
		a.activeKey = candidateKey
		a.switchCount++
		a.lastSwitchAt = now

		ev := SwitchEvent{
			Timestamp: now.Format("15:04:05.000"),
			FromKey:   prevKey,
			FromName:  strategyDisplayName(prevKey),
			ToKey:     candidateKey,
			ToName:    strategyDisplayName(candidateKey),
			Rule:      rule,
			Reason:    candidateReason,
		}
		a.history = append([]SwitchEvent{ev}, a.history...)
		if len(a.history) > adaptiveMaxHistory {
			a.history = a.history[:adaptiveMaxHistory]
		}
	}

	a.candidateKey = candidateKey
	a.ruleMatched = rule
	a.lastSignals = signals
	a.lastEvaluatedAt = now

	if holdDueToCooldown {
		a.reason = fmt.Sprintf(
			"Holding %s during %.1fs stability cooldown; pending transition to %s (%s)",
			strategyDisplayName(a.activeKey),
			float64(cooldownRemMs)/1000.0,
			strategyDisplayName(candidateKey),
			candidateReason,
		)
	} else {
		a.reason = candidateReason
	}

	return a.buildSnapshotLocked(cooldownRemMs)
}

// Snapshot returns the last evaluated state after refreshing against live cluster metrics.
func (a *AdaptiveBalancer) Snapshot() AdaptiveSnapshot {
	return a.Evaluate()
}

func (a *AdaptiveBalancer) buildSnapshotLocked(cooldownRemMs int64) AdaptiveSnapshot {
	histCopy := make([]SwitchEvent, len(a.history))
	copy(histCopy, a.history)

	var lastSwitchStr string
	if !a.lastSwitchAt.IsZero() {
		lastSwitchStr = a.lastSwitchAt.Format("15:04:05")
	}

	return AdaptiveSnapshot{
		ActiveKey:           a.activeKey,
		ActiveName:          strategyDisplayName(a.activeKey),
		CandidateKey:        a.candidateKey,
		RuleMatched:         a.ruleMatched,
		Reason:              a.reason,
		EvaluatedAt:         a.lastEvaluatedAt.Format("15:04:05.000"),
		LastSwitchAt:        lastSwitchStr,
		SwitchCount:         a.switchCount,
		CooldownRemainingMs: cooldownRemMs,
		Signals:             a.lastSignals,
		History:             histCopy,
	}
}

// ObserveClientIP records a client IP into the recent ring buffer without evaluating immediately.
func (a *AdaptiveBalancer) ObserveClientIP(r *http.Request) {
	a.recordClientIP(r)
}

func (a *AdaptiveBalancer) recordClientIP(r *http.Request) {
	ip := ExtractClientIP(r)
	a.ipMux.Lock()
	a.ipRing[a.ipHead] = ip
	a.ipHead = (a.ipHead + 1) % adaptiveIPWindowSize
	if a.ipCount < adaptiveIPWindowSize {
		a.ipCount++
	}
	a.ipMux.Unlock()
}

func (a *AdaptiveBalancer) collectSignalsLocked() AdaptiveSignals {
	sig := AdaptiveSignals{
		TotalNodes:     len(a.backends),
		WeightsUniform: true,
	}

	var minConns, maxConns int64
	var minLat, maxLat int64
	var minW, maxW int64
	weightParts := make([]string, 0, len(a.backends))

	firstHealthy := true
	for _, b := range a.backends {
		w := b.GetWeight()
		weightParts = append(weightParts, fmt.Sprintf("%d", w))

		if !b.IsAlive() {
			continue
		}
		sig.HealthyNodes++

		conns := atomic.LoadInt64(&b.ActiveConnections)
		lat := atomic.LoadInt64(&b.LastLatencyMs)

		if firstHealthy {
			minConns, maxConns = conns, conns
			minLat, maxLat = lat, lat
			minW, maxW = w, w
			firstHealthy = false
		} else {
			if conns < minConns {
				minConns = conns
			}
			if conns > maxConns {
				maxConns = conns
			}
			if lat < minLat {
				minLat = lat
			}
			if lat > maxLat {
				maxLat = lat
			}
			if w < minW {
				minW = w
			}
			if w > maxW {
				maxW = w
			}
		}
	}

	sig.WeightSummary = strings.Join(weightParts, ":")
	if sig.WeightSummary == "" {
		sig.WeightSummary = "none"
	}

	if sig.HealthyNodes > 0 {
		sig.MinConnections = minConns
		sig.MaxConnections = maxConns
		sig.ConnSpread = maxConns - minConns
		sig.MinLatencyMs = minLat
		sig.MaxLatencyMs = maxLat
		if maxLat > 0 {
			sig.LatencySkewRatio = math.Round((float64(maxLat)/math.Max(1.0, float64(minLat)))*10) / 10
		} else {
			sig.LatencySkewRatio = 1.0
		}
		sig.WeightsUniform = minW == maxW
	} else {
		sig.LatencySkewRatio = 1.0
	}

	a.ipMux.Lock()
	sig.RecentSampleSize = a.ipCount
	if a.ipCount > 0 {
		seen := make(map[string]struct{}, a.ipCount)
		for i := 0; i < a.ipCount; i++ {
			seen[a.ipRing[i]] = struct{}{}
		}
		sig.UniqueRecentIPs = len(seen)
		sig.RepeatIPRatio = math.Round((1.0-float64(sig.UniqueRecentIPs)/float64(a.ipCount))*1000) / 1000
	}
	a.ipMux.Unlock()

	return sig
}

func decideStrategy(sig AdaptiveSignals) (strategyKey, rule, reason string) {
	if sig.HealthyNodes < 2 {
		return StrategyRoundRobin, "uniform_baseline", fmt.Sprintf(
			"Single/zero healthy upstream (%d/%d online); using direct Round Robin baseline.",
			sig.HealthyNodes, sig.TotalNodes,
		)
	}

	// Priority 1: Connection queue spread or latency tail skew -> Least Connections
	if sig.ConnSpread >= adaptiveConnSpreadTrig {
		return StrategyLeastConnections, "connection_skew", fmt.Sprintf(
			"Active socket spread reached %d (%d max vs %d min); steering traffic away from saturated node via Least Connections.",
			sig.ConnSpread, sig.MaxConnections, sig.MinConnections,
		)
	}
	if sig.MaxLatencyMs >= adaptiveLatencyMinMs && sig.LatencySkewRatio >= adaptiveLatencySkewTrig {
		return StrategyLeastConnections, "latency_skew", fmt.Sprintf(
			"Response latency skewed across healthy nodes (%d ms slow vs %d ms fast, %.1fx); using Least Connections.",
			sig.MaxLatencyMs, sig.MinLatencyMs, sig.LatencySkewRatio,
		)
	}

	// Priority 2: Heterogeneous backend weights -> Weighted Round Robin
	if !sig.WeightsUniform {
		return StrategyWeightedRoundRobin, "capacity_weights", fmt.Sprintf(
			"Healthy backends carry unequal weights (%s); enforcing proportional Weighted RR split.",
			sig.WeightSummary,
		)
	}

	// Priority 3: Repeat client IP affinity -> IP Hash
	if sig.RecentSampleSize >= adaptiveMinIPSamples && sig.RepeatIPRatio >= adaptiveRepeatIPTrigger {
		return StrategyIPHash, "client_affinity", fmt.Sprintf(
			"High repeat client rate (%.0f%% across last %d requests, %d unique IP%s); pinning sessions with IP Hash.",
			sig.RepeatIPRatio*100, sig.RecentSampleSize, sig.UniqueRecentIPs, pluralS(sig.UniqueRecentIPs),
		)
	}

	// Priority 4: Balanced cluster -> Round Robin
	return StrategyRoundRobin, "uniform_baseline", fmt.Sprintf(
		"Pool is balanced (spread %d sockets, max %d ms, weights %s, %d recent unique IPs); using O(1) Round Robin.",
		sig.ConnSpread, sig.MaxLatencyMs, sig.WeightSummary, sig.UniqueRecentIPs,
	)
}

func strategyDisplayName(key string) string {
	switch key {
	case StrategyRoundRobin:
		return "Round Robin"
	case StrategyWeightedRoundRobin:
		return "Weighted Round Robin"
	case StrategyLeastConnections:
		return "Least Connections"
	case StrategyIPHash:
		return "IP Hash"
	default:
		return key
	}
}

func pluralS(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
