package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"nexuslb/pkg/backend"
	"nexuslb/pkg/balancer"
	"nexuslb/pkg/metrics"
	"nexuslb/pkg/ratelimit"
)

// retryRecorder stages upstream response data before committing to client socket.
// If an upstream server drops connection before writing headers, it marks failure
// allowing the proxy to retry an alternate healthy backend with zero dropped requests.
type retryRecorder struct {
	underlying http.ResponseWriter
	statusCode int
	header     http.Header
	body       bytes.Buffer
	committed  bool
	failed     bool
	failureErr error
}

func newRetryRecorder(w http.ResponseWriter) *retryRecorder {
	return &retryRecorder{
		underlying: w,
		statusCode: http.StatusOK,
		header:     make(http.Header),
	}
}

func (rec *retryRecorder) Header() http.Header {
	return rec.header
}

func (rec *retryRecorder) WriteHeader(code int) {
	if !rec.committed && !rec.failed {
		rec.statusCode = code
	}
}

func (rec *retryRecorder) Write(b []byte) (int, error) {
	if rec.failed {
		return 0, rec.failureErr
	}
	if rec.committed {
		return rec.underlying.Write(b)
	}
	return rec.body.Write(b)
}

func (rec *retryRecorder) SetStatusCode(code int) {
	rec.statusCode = code
}

func (rec *retryRecorder) SetFailed(err error) {
	rec.failed = true
	rec.failureErr = err
}

func (rec *retryRecorder) Commit() {
	if rec.committed || rec.failed {
		return
	}
	rec.committed = true
	// Copy headers to underlying ResponseWriter
	for k, vv := range rec.header {
		for _, v := range vv {
			rec.underlying.Header().Add(k, v)
		}
	}
	rec.underlying.WriteHeader(rec.statusCode)
	if rec.body.Len() > 0 {
		rec.underlying.Write(rec.body.Bytes())
	}
}

// Hijack implements http.Hijacker for WebSocket and connection hijacking support
func (rec *retryRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	rec.Commit()
	if hj, ok := rec.underlying.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, errors.New("underlying ResponseWriter does not implement http.Hijacker")
}

// Flush implements http.Flusher for streaming responses and SSE support
func (rec *retryRecorder) Flush() {
	rec.Commit()
	if fl, ok := rec.underlying.(http.Flusher); ok {
		fl.Flush()
	}
}

// Unwrap allows http.ResponseController to access the underlying ResponseWriter
func (rec *retryRecorder) Unwrap() http.ResponseWriter {
	return rec.underlying
}

type ProxyHandler struct {
	balancer   balancer.Balancer
	collector  *metrics.Collector
	limiter    *ratelimit.Limiter
	maxRetries int
}

func NewProxyHandler(b balancer.Balancer, c *metrics.Collector, l *ratelimit.Limiter, maxRetries ...int) *ProxyHandler {
	retries := 2
	if len(maxRetries) > 0 && maxRetries[0] >= 0 {
		retries = maxRetries[0]
	}
	return &ProxyHandler{
		balancer:   b,
		collector:  c,
		limiter:    l,
		maxRetries: retries,
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

	// Buffer request body if present to enable transparent retries on POST/PUT requests
	var bodyBytes []byte
	if r.Body != nil {
		bodyBytes, _ = io.ReadAll(r.Body)
		r.Body.Close()
	}

	attempts := 0
	maxAttempts := p.maxRetries + 1
	var lastErr error
	var lastTarget *backend.Backend

	// Layer 2: Load Balancing with Transparent Failover Loop
	for attempts < maxAttempts {
		attempts++

		// Reset request body for this attempt
		if bodyBytes != nil {
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}

		target, err := p.balancer.NextBackend(r)
		if err != nil {
			lastErr = err
			break
		}
		lastTarget = target

		target.IncConn()

		isCanary := balancer.IsCanaryRequest(r) || target.IsCanaryNode()
		if target.IsCanaryNode() {
			w.Header().Set("X-Nexus-Canary", target.GetCanaryTag())
		}

		// Reverse proxy tracing headers
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

		rec := newRetryRecorder(w)
		attemptStart := time.Now()

		target.ReverseProxy.ServeHTTP(rec, r)

		target.DecConn()
		attemptDuration := time.Since(attemptStart)

		if rec.failed {
			// Backend connection failed before response headers committed
			lastErr = rec.failureErr
			target.RecordRequest(attemptDuration, true)
			p.collector.Record(r.Method, r.URL.Path, clientIP, target.URL.String(), http.StatusBadGateway, attemptDuration, isCanary)
			continue // Retry on alternate backend
		}

		// Successful connection: commit staged headers and body to client
		rec.Commit()
		elapsed := time.Since(start)
		isErr := rec.statusCode >= 500
		target.RecordRequest(elapsed, isErr)
		p.collector.Record(r.Method, r.URL.Path, clientIP, target.URL.String(), rec.statusCode, elapsed, isCanary)
		return
	}

	// All retry attempts exhausted or no healthy backends available
	duration := time.Since(start)
	statusCode := http.StatusBadGateway
	if attempts == 1 && lastTarget == nil && lastErr == balancer.ErrNoHealthyBackends {
		statusCode = http.StatusServiceUnavailable
	}

	p.collector.Record(r.Method, r.URL.Path, clientIP, "none", statusCode, duration)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if statusCode == http.StatusServiceUnavailable {
		json.NewEncoder(w).Encode(map[string]string{
			"error":   "Service Unavailable",
			"message": "NexusLB: No healthy upstream backend servers available",
		})
	} else {
		targetName := "unknown"
		if lastTarget != nil {
			targetName = lastTarget.URL.String()
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":        "Bad Gateway",
			"message":      "NexusLB: All upstream retries failed",
			"attempts":     attempts,
			"last_backend": targetName,
			"details":      fmt.Sprintf("%v", lastErr),
		})
	}
}
