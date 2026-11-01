package proxy

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"nexuslb/pkg/backend"
	"nexuslb/pkg/balancer"
	"nexuslb/pkg/metrics"
	"nexuslb/pkg/ratelimit"
)

func TestProxyHandler_SuccessfulForwarding(t *testing.T) {
	// 1. Mock Upstream Backend
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientIP := r.Header.Get("X-Real-IP")
		xff := r.Header.Get("X-Forwarded-For")
		w.Header().Set("X-Echo-Real-IP", clientIP)
		w.Header().Set("X-Echo-XFF", xff)
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "upstream response payload")
	}))
	defer upstream.Close()

	b, err := backend.NewBackend(upstream.URL)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}

	sm, err := balancer.NewStrategyManager("round_robin", []*backend.Backend{b})
	if err != nil {
		t.Fatalf("failed to create strategy manager: %v", err)
	}

	collector := metrics.NewCollector()
	limiter := ratelimit.NewLimiter(10.0, 10, true)
	defer limiter.Close()

	handler := NewProxyHandler(sm, collector, limiter)

	// 2. Dispatch client request
	req := httptest.NewRequest("GET", "http://nexuslb.local/api/test", nil)
	req.RemoteAddr = "192.168.1.50:49212"
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	resp := rec.Result()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}
	if string(body) != "upstream response payload" {
		t.Errorf("unexpected body: %s", string(body))
	}
	if rec.Header().Get("X-RateLimit-Limit") != "10" {
		t.Errorf("expected X-RateLimit-Limit header 10, got %s", rec.Header().Get("X-RateLimit-Limit"))
	}
	if resp.Header.Get("X-Echo-Real-IP") != "192.168.1.50" {
		t.Errorf("expected X-Real-IP 192.168.1.50, got %s", resp.Header.Get("X-Echo-Real-IP"))
	}

	snap := collector.Snapshot("Round Robin", []*backend.Backend{b})
	if snap.TotalRequests != 1 || snap.TotalSuccess != 1 {
		t.Errorf("expected 1 total request and 1 success in metrics, got %+v", snap)
	}
}

func TestProxyHandler_RateLimitingThrottled(t *testing.T) {
	b, _ := backend.NewBackend("http://127.0.0.1:8001")
	sm, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{b})
	collector := metrics.NewCollector()

	// Limiter with burst capacity 1
	limiter := ratelimit.NewLimiter(1.0, 1, true)
	defer limiter.Close()

	handler := NewProxyHandler(sm, collector, limiter)

	// First request passes
	req1 := httptest.NewRequest("GET", "http://nexuslb.local/", nil)
	req1.RemoteAddr = "192.168.1.100:12345"
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	// Second request from same IP must be throttled with 429
	req2 := httptest.NewRequest("GET", "http://nexuslb.local/", nil)
	req2.RemoteAddr = "192.168.1.100:12345"
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429 Too Many Requests, got %d", rec2.Code)
	}
	if rec2.Header().Get("Retry-After") == "" {
		t.Errorf("expected Retry-After header on throttled response")
	}
	if !strings.Contains(rec2.Body.String(), "Rate limit exceeded") {
		t.Errorf("expected rate limit error message, got %s", rec2.Body.String())
	}
}

func TestProxyHandler_NoHealthyBackends(t *testing.T) {
	b, _ := backend.NewBackend("http://127.0.0.1:8001")
	b.SetAlive(false) // simulate offline
	sm, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{b})
	collector := metrics.NewCollector()
	limiter := ratelimit.NewLimiter(10.0, 10, false)
	defer limiter.Close()

	handler := NewProxyHandler(sm, collector, limiter)

	req := httptest.NewRequest("GET", "http://nexuslb.local/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "No healthy upstream backend") {
		t.Errorf("unexpected body: %s", rec.Body.String())
	}
}

func TestProxyHandler_BackendConnectionFailure(t *testing.T) {
	// Point to an unused local port where no server is listening
	b, _ := backend.NewBackend("http://127.0.0.1:59999")
	sm, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{b})
	collector := metrics.NewCollector()
	limiter := ratelimit.NewLimiter(10.0, 10, false)
	defer limiter.Close()

	handler := NewProxyHandler(sm, collector, limiter)

	req := httptest.NewRequest("GET", "http://nexuslb.local/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// ErrorHandler should catch dial refusal, mark backend dead, and return 502 Bad Gateway
	if rec.Code != http.StatusBadGateway {
		t.Errorf("expected 502 Bad Gateway, got %d", rec.Code)
	}
	if b.IsAlive() {
		t.Errorf("expected backend to be marked offline after connection failure")
	}
	if !strings.Contains(rec.Body.String(), "Bad Gateway") {
		t.Errorf("unexpected body: %s", rec.Body.String())
	}
}

func TestProxyHandler_CanaryRouting(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "canary v2.0 response")
	}))
	defer upstream.Close()

	b, _ := backend.NewBackend(upstream.URL)
	b.SetCanary(true, "v2.0-canary")

	sm, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{b})
	collector := metrics.NewCollector()
	limiter := ratelimit.NewLimiter(10.0, 10, false)
	defer limiter.Close()

	handler := NewProxyHandler(sm, collector, limiter)

	req := httptest.NewRequest("GET", "http://nexuslb.local/", nil)
	req.Header.Set("X-Canary", "true")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rec.Code)
	}
	if tag := rec.Header().Get("X-Nexus-Canary"); tag != "v2.0-canary" {
		t.Errorf("expected X-Nexus-Canary header 'v2.0-canary', got %s", tag)
	}
}

type mockHijackableRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (m *mockHijackableRecorder) Flush() {
	m.flushed = true
}

func (m *mockHijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, errors.New("mock hijack successful check")
}

func TestResponseRecorder_FlusherAndHijacker(t *testing.T) {
	mock := &mockHijackableRecorder{ResponseRecorder: httptest.NewRecorder()}
	rec := &responseRecorder{
		ResponseWriter: mock,
		statusCode:     http.StatusOK,
	}

	// Test Flush
	rec.Flush()
	if !mock.flushed {
		t.Errorf("expected Flush() to forward to underlying flusher")
	}

	// Test Hijack
	_, _, err := rec.Hijack()
	if err == nil || !strings.Contains(err.Error(), "mock hijack successful check") {
		t.Errorf("expected hijack error from underlying mock, got %v", err)
	}

	// Test Unwrap
	if unwrapped := rec.Unwrap(); unwrapped != mock {
		t.Errorf("expected Unwrap to return underlying ResponseWriter")
	}
}

func TestProxyHandler_ConcurrentRequests(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	}))
	defer upstream.Close()

	b, _ := backend.NewBackend(upstream.URL)
	sm, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{b})
	collector := metrics.NewCollector()
	limiter := ratelimit.NewLimiter(1000.0, 1000, true)
	defer limiter.Close()

	handler := NewProxyHandler(sm, collector, limiter)

	var wg sync.WaitGroup
	workers := 50

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req := httptest.NewRequest("GET", "http://nexuslb.local/", nil)
			req.RemoteAddr = fmt.Sprintf("192.168.1.%d:12345", (id%10)+1)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("worker %d: expected 200 OK, got %d", id, rec.Code)
			}
		}(i)
	}

	wg.Wait()

	snap := collector.Snapshot("Round Robin", []*backend.Backend{b})
	if snap.TotalRequests != int64(workers) {
		t.Errorf("expected %d total requests, got %d", workers, snap.TotalRequests)
	}
}

