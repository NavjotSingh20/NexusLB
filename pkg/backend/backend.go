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
	b.mux.RUnlock()

	return BackendStats{
		URL:               b.URL.String(),
		Alive:             alive,
		ActiveConnections: atomic.LoadInt64(&b.ActiveConnections),
		TotalRequests:     atomic.LoadInt64(&b.TotalRequests),
		TotalErrors:       atomic.LoadInt64(&b.TotalErrors),
		LastLatencyMs:     atomic.LoadInt64(&b.LastLatencyMs),
	}
}
