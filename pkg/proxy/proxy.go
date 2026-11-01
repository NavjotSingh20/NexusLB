package proxy

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

	"nexuslb/pkg/balancer"
	"nexuslb/pkg/metrics"
	"nexuslb/pkg/ratelimit"
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

func (rec *responseRecorder) SetStatusCode(code int) {
	rec.statusCode = code
	rec.written = true
}

// Hijack implements http.Hijacker for WebSocket and connection hijacking support
func (rec *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := rec.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, errors.New("underlying ResponseWriter does not implement http.Hijacker")
}

// Flush implements http.Flusher for streaming responses and SSE support
func (rec *responseRecorder) Flush() {
	if fl, ok := rec.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

// Unwrap allows http.ResponseController to access the underlying ResponseWriter
func (rec *responseRecorder) Unwrap() http.ResponseWriter {
	return rec.ResponseWriter
}

type ProxyHandler struct {
	balancer  balancer.Balancer
	collector *metrics.Collector
	limiter   *ratelimit.Limiter
}

func NewProxyHandler(b balancer.Balancer, c *metrics.Collector, l *ratelimit.Limiter) *ProxyHandler {
	return &ProxyHandler{
		balancer:  b,
		collector: c,
		limiter:   l,
	}
}

func (p *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	clientIP := balancer.ExtractClientIP(r)

	// Layer 1: Edge Security & Rate Limiting Shield
	if p.limiter != nil && p.limiter.IsEnabled() {
		allowed, remaining, retryAfter := p.limiter.Allow(clientIP)
		if !allowed {
			duration := time.Since(start)
			p.collector.Record(r.Method, r.URL.Path, clientIP, "Edge Shield", http.StatusTooManyRequests, duration)

			retrySec := int(retryAfter.Seconds())
			if retrySec < 1 {
				retrySec = 1
			}

			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(int(p.limiter.Rate())))
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("Retry-After", strconv.Itoa(retrySec))
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error":               "Too Many Requests",
				"message":             "NexusLB Edge Shield: Rate limit exceeded for IP " + clientIP,
				"retry_after_seconds": retrySec,
				"client_ip":           clientIP,
			})
			return
		}

		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(int(p.limiter.Rate())))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
	}

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

	isCanary := balancer.IsCanaryRequest(r) || target.IsCanaryNode()
	if target.IsCanaryNode() {
		w.Header().Set("X-Nexus-Canary", target.GetCanaryTag())
	}

	rec := &responseRecorder{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
	}

	// Add reverse proxy tracing headers
	if prior := r.Header.Get("X-Forwarded-For"); prior != "" {
		r.Header.Set("X-Forwarded-For", prior+", "+clientIP)
	} else {
		r.Header.Set("X-Forwarded-For", clientIP)
	}
	r.Header.Set("X-Real-IP", clientIP)
	r.Header.Set("X-Forwarded-Host", r.Host)
	if r.TLS != nil {
		r.Header.Set("X-Forwarded-Proto", "https")
	} else {
		r.Header.Set("X-Forwarded-Proto", "http")
	}

	// Execute proxy forwarding
	target.ReverseProxy.ServeHTTP(rec, r)

	elapsed := time.Since(start)
	isErr := rec.statusCode >= 500
	target.RecordRequest(elapsed, isErr)
	p.collector.Record(r.Method, r.URL.Path, clientIP, target.URL.String(), rec.statusCode, elapsed, isCanary)
}
