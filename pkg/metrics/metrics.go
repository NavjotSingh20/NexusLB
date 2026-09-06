package metrics

import (
	"sync"
	"sync/atomic"
	"time"

	"nexuslb/pkg/backend"
)

type RequestLog struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Method    string `json:"method"`
	Path      string `json:"path"`
	Backend   string `json:"backend"`
	Status    int    `json:"status"`
	LatencyMs int64  `json:"latency_ms"`
	ClientIP  string `json:"client_ip"`
}

type DashboardStats struct {
	TotalRequests      int64                  `json:"total_requests"`
	TotalSuccess       int64                  `json:"total_success"`
	TotalClientErrors  int64                  `json:"total_client_errors"`
	TotalServerErrors  int64                  `json:"total_server_errors"`
	ActiveConnections  int64                  `json:"active_connections"`
	QueuedRequests     int64                  `json:"queued_requests"`
	RequestsPerSecond  float64                `json:"rps"`
	AverageLatencyMs   float64                `json:"avg_latency_ms"`
	Backends           []backend.BackendStats `json:"backends"`
	RecentLogs         []RequestLog           `json:"recent_logs"`
	Strategy           string                 `json:"strategy"`
	UptimeSeconds      int64                  `json:"uptime_seconds"`
}

type Collector struct {
	totalRequests     int64
	totalSuccess      int64
	totalClientErrors int64
	totalServerErrors int64
	totalLatencyMs    int64
	queuedRequests    int64

	startTime         time.Time
	lastRPSCheckTime  time.Time
	lastRPSReqCount   int64
	currentRPS        float64

	recentLogs []RequestLog
	maxLogs    int

	mux sync.RWMutex
}

func NewCollector() *Collector {
	now := time.Now()
	return &Collector{
		startTime:        now,
		lastRPSCheckTime: now,
		maxLogs:          50,
		recentLogs:       make([]RequestLog, 0, 50),
	}
}

func (c *Collector) Reset(backends []*backend.Backend) {
	c.mux.Lock()
	atomic.StoreInt64(&c.totalRequests, 0)
	atomic.StoreInt64(&c.totalSuccess, 0)
	atomic.StoreInt64(&c.totalClientErrors, 0)
	atomic.StoreInt64(&c.totalServerErrors, 0)
	atomic.StoreInt64(&c.totalLatencyMs, 0)
	atomic.StoreInt64(&c.queuedRequests, 0)
	c.recentLogs = make([]RequestLog, 0, 50)
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

func (c *Collector) Record(method, path, clientIP, targetBackend string, status int, latency time.Duration) {
	atomic.AddInt64(&c.totalRequests, 1)
	latencyMs := latency.Milliseconds()
	atomic.AddInt64(&c.totalLatencyMs, latencyMs)

	if status >= 200 && status < 400 {
		atomic.AddInt64(&c.totalSuccess, 1)
	} else if status >= 400 && status < 500 {
		atomic.AddInt64(&c.totalClientErrors, 1)
	} else if status >= 500 {
		atomic.AddInt64(&c.totalServerErrors, 1)
	}

	logEntry := RequestLog{
		Timestamp: time.Now().Format("15:04:05.000"),
		Method:    method,
		Path:      path,
		Backend:   targetBackend,
		Status:    status,
		LatencyMs: latencyMs,
		ClientIP:  clientIP,
	}

	c.mux.Lock()
	if len(c.recentLogs) >= c.maxLogs {
		c.recentLogs = c.recentLogs[1:]
	}
	c.recentLogs = append(c.recentLogs, logEntry)
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
	c.mux.RUnlock()

	// Reverse logs for UI so newest appears first
	for i, j := 0, len(logsCopy)-1; i < j; i, j = i+1, j-1 {
		logsCopy[i], logsCopy[j] = logsCopy[j], logsCopy[i]
	}

	var activeConns int64
	backendStats := make([]backend.BackendStats, len(backends))
	for i, b := range backends {
		bs := b.GetStats()
		backendStats[i] = bs
		activeConns += bs.ActiveConnections
	}

	return DashboardStats{
		TotalRequests:     totalReqs,
		TotalSuccess:      atomic.LoadInt64(&c.totalSuccess),
		TotalClientErrors: atomic.LoadInt64(&c.totalClientErrors),
		TotalServerErrors: atomic.LoadInt64(&c.totalServerErrors),
		ActiveConnections: activeConns,
		QueuedRequests:    c.GetQueue(),
		RequestsPerSecond: rps,
		AverageLatencyMs:  avgLatency,
		Backends:          backendStats,
		RecentLogs:        logsCopy,
		Strategy:          strategyName,
		UptimeSeconds:     int64(time.Since(c.startTime).Seconds()),
	}
}
