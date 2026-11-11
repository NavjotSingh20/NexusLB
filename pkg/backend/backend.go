package backend

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type BackendStats struct {
	URL                 string `json:"url"`
	Alive               bool   `json:"alive"`
	ActiveConnections   int64  `json:"active_connections"`
	TotalRequests       int64  `json:"total_requests"`
	TotalErrors         int64  `json:"total_errors"`
	LastLatencyMs       int64  `json:"last_latency_ms"`
	Weight              int64  `json:"weight"`
	IsCanary            bool   `json:"is_canary"`
	CanaryTag           string `json:"canary_tag"`
	CircuitState        string `json:"circuit_state"`
	ConsecutiveFailures int64  `json:"consecutive_failures"`
}

type Backend struct {
	URL               *url.URL
	Alive             bool
	mux               sync.RWMutex
	ReverseProxy      *httputil.ReverseProxy
	ActiveConnections int64
	TotalRequests     int64
	TotalErrors       int64
	LastLatencyMs     int64
	Weight            int64
	CurrentWeight     int64
	IsCanary          bool
	CanaryTag         string
	CircuitBreaker    *CircuitBreaker
}

func NewBackend(rawURL string) (*Backend, error) {
	return NewBackendWithConfig(rawURL, 5, 10*time.Second)
}

// NewBackendWithConfig initializes a Backend with explicit circuit breaker parameters.
func NewBackendWithConfig(rawURL string, maxFailures int64, cooldown time.Duration) (*Backend, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if maxFailures < 1 {
		maxFailures = 5
	}
	if cooldown <= 0 {
		cooldown = 10 * time.Second
	}

	b := &Backend{
		URL:            parsedURL,
		Alive:          true,
		Weight:         1,
		CanaryTag:      "v2.0-canary",
		CircuitBreaker: NewCircuitBreaker(maxFailures, cooldown),
	}

	proxy := httputil.NewSingleHostReverseProxy(parsedURL)

	// Static error handler: thread-safe, configured once at initialization
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, proxyErr error) {
		b.SetAlive(false)
		b.RecordRequest(0, true)

		// Check if the response writer is a transparent retry buffer
		if retrier, ok := rw.(interface{ SetFailed(error) }); ok {
			retrier.SetFailed(proxyErr)
			return
		}

		if setter, ok := rw.(interface{ SetStatusCode(int) }); ok {
			setter.SetStatusCode(http.StatusBadGateway)
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(rw).Encode(map[string]string{
			"error":   "Bad Gateway",
			"message": "NexusLB: Backend connection failed",
			"backend": b.URL.String(),
		})
	}

	// High-performance connection pool transport
	proxy.Transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
	}

	b.ReverseProxy = proxy

	return b, nil
}

func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.Alive = alive
}

func (b *Backend) IsAlive() bool {
	b.mux.RLock()
	alive := b.Alive
	b.mux.RUnlock()
	if !alive {
		return false
	}
	if b.CircuitBreaker != nil {
		return b.CircuitBreaker.Allow()
	}
	return true
}

func (b *Backend) IsUp() bool {
	b.mux.RLock()
	defer b.mux.RUnlock()
	return b.Alive
}

func (b *Backend) IncConn() {
	atomic.AddInt64(&b.ActiveConnections, 1)
}

func (b *Backend) DecConn() {
	atomic.AddInt64(&b.ActiveConnections, -1)
}

func (b *Backend) SetActiveConnections(count int64) {
	if count < 0 {
		count = 0
	}
	atomic.StoreInt64(&b.ActiveConnections, count)
}

func (b *Backend) SetWeight(weight int64) {
	if weight < 1 {
		weight = 1
	}
	atomic.StoreInt64(&b.Weight, weight)
}

func (b *Backend) GetWeight() int64 {
	w := atomic.LoadInt64(&b.Weight)
	if w < 1 {
		return 1
	}
	return w
}

func (b *Backend) SetCanary(isCanary bool, tag string) {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.IsCanary = isCanary
	if tag == "" {
		tag = "v2.0-canary"
	}
	b.CanaryTag = tag
}

func (b *Backend) IsCanaryNode() bool {
	b.mux.RLock()
	defer b.mux.RUnlock()
	return b.IsCanary
}

func (b *Backend) GetCanaryTag() string {
	b.mux.RLock()
	defer b.mux.RUnlock()
	return b.CanaryTag
}

func (b *Backend) RecordRequest(latency time.Duration, isErr bool) {
	atomic.AddInt64(&b.TotalRequests, 1)
	if isErr {
		atomic.AddInt64(&b.TotalErrors, 1)
		if b.CircuitBreaker != nil {
			b.CircuitBreaker.RecordFailure()
		}
	} else {
		if b.CircuitBreaker != nil {
			b.CircuitBreaker.RecordSuccess()
		}
	}
	atomic.StoreInt64(&b.LastLatencyMs, latency.Milliseconds())
}

func (b *Backend) GetStats() BackendStats {
	b.mux.RLock()
	alive := b.Alive
	isCanary := b.IsCanary
	canaryTag := b.CanaryTag
	b.mux.RUnlock()

	circuitState := "closed"
	var failures int64
	if b.CircuitBreaker != nil {
		circuitState = string(b.CircuitBreaker.State())
		failures = b.CircuitBreaker.Failures()
	}

	return BackendStats{
		URL:                 b.URL.String(),
		Alive:               alive && (circuitState != "open"),
		ActiveConnections:   atomic.LoadInt64(&b.ActiveConnections),
		TotalRequests:       atomic.LoadInt64(&b.TotalRequests),
		TotalErrors:         atomic.LoadInt64(&b.TotalErrors),
		LastLatencyMs:       atomic.LoadInt64(&b.LastLatencyMs),
		Weight:              b.GetWeight(),
		IsCanary:            isCanary,
		CanaryTag:           canaryTag,
		CircuitState:        circuitState,
		ConsecutiveFailures: failures,
	}
}
