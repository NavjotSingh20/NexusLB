package backend

import (
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type BackendStats struct {
	URL               string `json:"url"`
	Alive             bool   `json:"alive"`
	ActiveConnections int64  `json:"active_connections"`
	TotalRequests     int64  `json:"total_requests"`
	TotalErrors       int64  `json:"total_errors"`
	LastLatencyMs     int64  `json:"last_latency_ms"`
	Weight            int64  `json:"weight"`
	IsCanary          bool   `json:"is_canary"`
	CanaryTag         string `json:"canary_tag"`
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
}

func NewBackend(rawURL string) (*Backend, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	proxy := httputil.NewSingleHostReverseProxy(parsedURL)

	b := &Backend{
		URL:          parsedURL,
		Alive:        true,
		ReverseProxy: proxy,
		Weight:       1,
		CanaryTag:    "v2.0-canary",
	}

	return b, nil
}

func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.Alive = alive
}

func (b *Backend) IsAlive() bool {
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
	}
	atomic.StoreInt64(&b.LastLatencyMs, latency.Milliseconds())
}

func (b *Backend) GetStats() BackendStats {
	b.mux.RLock()
	alive := b.Alive
	isCanary := b.IsCanary
	canaryTag := b.CanaryTag
	b.mux.RUnlock()

	return BackendStats{
		URL:               b.URL.String(),
		Alive:             alive,
		ActiveConnections: atomic.LoadInt64(&b.ActiveConnections),
		TotalRequests:     atomic.LoadInt64(&b.TotalRequests),
		TotalErrors:       atomic.LoadInt64(&b.TotalErrors),
		LastLatencyMs:     atomic.LoadInt64(&b.LastLatencyMs),
		Weight:            b.GetWeight(),
		IsCanary:          isCanary,
		CanaryTag:         canaryTag,
	}
}
