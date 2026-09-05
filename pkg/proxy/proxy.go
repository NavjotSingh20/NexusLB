package proxy

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"time"

	"nexuslb/pkg/balancer"
	"nexuslb/pkg/metrics"
)

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (rec *responseRecorder) WriteHeader(code int) {
	if !rec.written {
		rec.statusCode = code
		rec.written = true
	}
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *responseRecorder) Write(b []byte) (int, error) {
	if !rec.written {
		rec.statusCode = http.StatusOK
		rec.written = true
	}
	return rec.ResponseWriter.Write(b)
}

type ProxyHandler struct {
	balancer  balancer.Balancer
	collector *metrics.Collector
}

func NewProxyHandler(b balancer.Balancer, c *metrics.Collector) *ProxyHandler {
	return &ProxyHandler{
		balancer:  b,
		collector: c,
	}
}

func (p *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	clientIP := getClientIP(r)

	target, err := p.balancer.NextBackend(r)
	if err != nil {
		duration := time.Since(start)
		p.collector.Record(r.Method, r.URL.Path, clientIP, "none", http.StatusServiceUnavailable, duration)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"error":   "Service Unavailable",
			"message": "NexusLB: No healthy upstream backend servers available",
		})
		return
	}

	target.IncConn()
	defer target.DecConn()

	rec := &responseRecorder{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
	}

	// Add reverse proxy tracing headers
	r.Header.Set("X-Forwarded-For", clientIP)
	r.Header.Set("X-Forwarded-Host", r.Host)
	if r.TLS != nil {
		r.Header.Set("X-Forwarded-Proto", "https")
	} else {
		r.Header.Set("X-Forwarded-Proto", "http")
	}

	// Custom error handler for connection drops
	target.ReverseProxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, proxyErr error) {
		target.SetAlive(false)
		rec.statusCode = http.StatusBadGateway
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(rw).Encode(map[string]string{
			"error":   "Bad Gateway",
			"message": "NexusLB: Backend connection failed",
			"backend": target.URL.String(),
		})
	}

	// Execute proxy forwarding
	target.ReverseProxy.ServeHTTP(rec, r)

	elapsed := time.Since(start)
	isErr := rec.statusCode >= 500
	target.RecordRequest(elapsed, isErr)
	p.collector.Record(r.Method, r.URL.Path, clientIP, target.URL.String(), rec.statusCode, elapsed)
}

func getClientIP(r *http.Request) string {
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
