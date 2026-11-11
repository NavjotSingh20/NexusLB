package metrics

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"nexuslb/pkg/backend"
)

const defaultLatencySampleWindow = 1024

type RequestLog struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Method    string `json:"method"`
	Path      string `json:"path"`
	Backend   string `json:"backend"`
	Status    int    `json:"status"`
	LatencyMs int64  `json:"latency_ms"`
	ClientIP  string `json:"client_ip"`
	IsCanary  bool   `json:"is_canary,omitempty"`
}

type DashboardStats struct {
	TotalRequests       int64                  `json:"total_requests"`
	TotalSuccess        int64                  `json:"total_success"`
	TotalClientErrors   int64                  `json:"total_client_errors"`
	TotalServerErrors   int64                  `json:"total_server_errors"`
	ActiveConnections   int64                  `json:"active_connections"`
	QueuedRequests      int64                  `json:"queued_requests"`
	RequestsPerSecond   float64                `json:"rps"`
	AverageLatencyMs    float64                `json:"avg_latency_ms"`
	P50LatencyMs        float64                `json:"p50_latency_ms"`
	P95LatencyMs        float64                `json:"p95_latency_ms"`
	P99LatencyMs        float64                `json:"p99_latency_ms"`
	Backends            []backend.BackendStats `json:"backends"`
	RecentLogs          []RequestLog           `json:"recent_logs"`
	Strategy            string                 `json:"strategy"`
	StrategyKey         string                 `json:"strategy_key,omitempty"`
	AvailableStrategies []string               `json:"available_strategies,omitempty"`
	Adaptive            interface{}            `json:"adaptive,omitempty"`
	Simulation          interface{}            `json:"simulation,omitempty"`
	TotalThrottled      int64                  `json:"total_throttled"`
	TotalCanaryRequests int64                  `json:"total_canary_requests"`
	ShieldEnabled       bool                   `json:"shield_enabled"`
	ShieldRate          float64                `json:"shield_rate"`
	ShieldBurst         int                    `json:"shield_burst"`
	UptimeSeconds       int64                  `json:"uptime_seconds"`
}

type Collector struct {
	totalRequests       int64
	totalSuccess        int64
	totalClientErrors   int64
	totalServerErrors   int64
	totalThrottled      int64
	totalCanaryRequests int64
	totalLatencyMs      int64
	queuedRequests      int64

	startTime         time.Time
	lastRPSCheckTime  time.Time
	lastRPSReqCount   int64
	currentRPS        float64

	recentLogs     []RequestLog
	maxLogs        int
	latencySamples []float64
	latencyHead    int

	mux sync.RWMutex
}

func NewCollector() *Collector {
	now := time.Now()
	return &Collector{
		startTime:        now,
		lastRPSCheckTime: now,
		maxLogs:          50,
		recentLogs:       make([]RequestLog, 0, 50),
		latencySamples:   make([]float64, 0, defaultLatencySampleWindow),
	}
}

func (c *Collector) Reset(backends []*backend.Backend) {
	c.mux.Lock()
	atomic.StoreInt64(&c.totalRequests, 0)
	atomic.StoreInt64(&c.totalSuccess, 0)
	atomic.StoreInt64(&c.totalClientErrors, 0)
	atomic.StoreInt64(&c.totalServerErrors, 0)
	atomic.StoreInt64(&c.totalThrottled, 0)
	atomic.StoreInt64(&c.totalCanaryRequests, 0)
	atomic.StoreInt64(&c.totalLatencyMs, 0)
	atomic.StoreInt64(&c.queuedRequests, 0)
	c.recentLogs = make([]RequestLog, 0, 50)
	c.latencySamples = make([]float64, 0, defaultLatencySampleWindow)
	c.latencyHead = 0
	c.lastRPSReqCount = 0
	c.currentRPS = 0
	c.mux.Unlock()

	for _, b := range backends {
		atomic.StoreInt64(&b.TotalRequests, 0)
		atomic.StoreInt64(&b.TotalErrors, 0)
		atomic.StoreInt64(&b.LastLatencyMs, 0)
	}
}

func (c *Collector) IncQueue(n int64) {
	atomic.AddInt64(&c.queuedRequests, n)
}

func (c *Collector) DecQueue() {
	atomic.AddInt64(&c.queuedRequests, -1)
}

func (c *Collector) GetQueue() int64 {
	q := atomic.LoadInt64(&c.queuedRequests)
	if q < 0 {
		return 0
	}
	return q
}

func (c *Collector) Record(method, path, clientIP, targetBackend string, status int, latency time.Duration, isCanary ...bool) {
	c.RecordWithID("", method, path, clientIP, targetBackend, status, latency, isCanary...)
}

// RecordWithID records request telemetry along with an end-to-end correlation request ID.
func (c *Collector) RecordWithID(id, method, path, clientIP, targetBackend string, status int, latency time.Duration, isCanary ...bool) {
	atomic.AddInt64(&c.totalRequests, 1)
	latencyMs := latency.Milliseconds()
	sampleMs := float64(latency) / float64(time.Millisecond)
	atomic.AddInt64(&c.totalLatencyMs, latencyMs)

	canary := len(isCanary) > 0 && isCanary[0]
	if canary {
		atomic.AddInt64(&c.totalCanaryRequests, 1)
	}

	if status == 429 {
		atomic.AddInt64(&c.totalThrottled, 1)
		atomic.AddInt64(&c.totalClientErrors, 1)
	} else if status >= 200 && status < 400 {
		atomic.AddInt64(&c.totalSuccess, 1)
	} else if status >= 400 && status < 500 {
		atomic.AddInt64(&c.totalClientErrors, 1)
	} else if status >= 500 {
		atomic.AddInt64(&c.totalServerErrors, 1)
	}

	logEntry := RequestLog{
		ID:        id,
		Timestamp: time.Now().Format("15:04:05.000"),
		Method:    method,
		Path:      path,
		Backend:   targetBackend,
		Status:    status,
		LatencyMs: latencyMs,
		ClientIP:  clientIP,
		IsCanary:  canary,
	}

	c.mux.Lock()
	if len(c.recentLogs) >= c.maxLogs {
		c.recentLogs = c.recentLogs[1:]
	}
	c.recentLogs = append(c.recentLogs, logEntry)

	if cap(c.latencySamples) > 0 {
		if len(c.latencySamples) < cap(c.latencySamples) {
			c.latencySamples = append(c.latencySamples, sampleMs)
		} else {
			c.latencySamples[c.latencyHead] = sampleMs
			c.latencyHead = (c.latencyHead + 1) % cap(c.latencySamples)
		}
	}
	c.mux.Unlock()
}

func (c *Collector) CalculateRPS() float64 {
	c.mux.Lock()
	defer c.mux.Unlock()

	now := time.Now()
	elapsed := now.Sub(c.lastRPSCheckTime).Seconds()
	if elapsed >= 1.0 {
		currentTotal := atomic.LoadInt64(&c.totalRequests)
		reqDelta := currentTotal - c.lastRPSReqCount
		c.currentRPS = float64(reqDelta) / elapsed
		c.lastRPSReqCount = currentTotal
		c.lastRPSCheckTime = now
	}
	return c.currentRPS
}

func (c *Collector) Snapshot(strategyName string, backends []*backend.Backend) DashboardStats {
	rps := c.CalculateRPS()

	totalReqs := atomic.LoadInt64(&c.totalRequests)
	totalLat := atomic.LoadInt64(&c.totalLatencyMs)

	var avgLatency float64
	if totalReqs > 0 {
		avgLatency = float64(totalLat) / float64(totalReqs)
	}

	c.mux.RLock()
	logsCopy := make([]RequestLog, len(c.recentLogs))
	copy(logsCopy, c.recentLogs)
	samplesCopy := make([]float64, len(c.latencySamples))
	copy(samplesCopy, c.latencySamples)
	c.mux.RUnlock()

	sort.Float64s(samplesCopy)
	p50 := percentile(samplesCopy, 0.50)
	p95 := percentile(samplesCopy, 0.95)
	p99 := percentile(samplesCopy, 0.99)

	// Reverse logs for UI so newest appears first
	for i, j := 0, len(logsCopy)-1; i < j; i, j = i+1, j-1 {
		logsCopy[i], logsCopy[j] = logsCopy[j], logsCopy[i]
	}

	backendStats := make([]backend.BackendStats, 0, len(backends))
	var activeConns int64
	for _, b := range backends {
		s := b.GetStats()
		backendStats = append(backendStats, s)
		activeConns += s.ActiveConnections
	}

	uptimeSec := int64(time.Since(c.startTime).Seconds())

	return DashboardStats{
		TotalRequests:       totalReqs,
		TotalSuccess:        atomic.LoadInt64(&c.totalSuccess),
		TotalClientErrors:   atomic.LoadInt64(&c.totalClientErrors),
		TotalServerErrors:   atomic.LoadInt64(&c.totalServerErrors),
		TotalThrottled:      atomic.LoadInt64(&c.totalThrottled),
		TotalCanaryRequests: atomic.LoadInt64(&c.totalCanaryRequests),
		ActiveConnections:   activeConns,
		QueuedRequests:      c.GetQueue(),
		RequestsPerSecond:   rps,
		AverageLatencyMs:    avgLatency,
		P50LatencyMs:        p50,
		P95LatencyMs:        p95,
		P99LatencyMs:        p99,
		Backends:            backendStats,
		RecentLogs:          logsCopy,
		Strategy:            strategyName,
		UptimeSeconds:       uptimeSec,
	}
}

func percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(n))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}
