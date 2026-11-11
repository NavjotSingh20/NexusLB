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
	rec := newRetryRecorder(mock)

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

func TestProxyHandler_TransparentFailover(t *testing.T) {
	// Backend 1: offline / dead port
	deadBackend, _ := backend.NewBackend("http://127.0.0.1:59998")

	// Backend 2: healthy live server
	liveServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "failover-success")
	}))
	defer liveServer.Close()

	liveBackend, _ := backend.NewBackend(liveServer.URL)

	// Balancer with dead backend first, live backend second
	sm, err := balancer.NewStrategyManager("round_robin", []*backend.Backend{deadBackend, liveBackend})
	if err != nil {
		t.Fatalf("failed to create strategy manager: %v", err)
	}

	collector := metrics.NewCollector()
	limiter := ratelimit.NewLimiter(10.0, 10, false)
	defer limiter.Close()

	// MaxRetries = 2
	handler := NewProxyHandler(sm, collector, limiter, 2)

	req := httptest.NewRequest("GET", "http://nexuslb.local/api/resource", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	resp := rec.Result()
	body, _ := io.ReadAll(resp.Body)

	// Transparent retry should have intercepted deadBackend failure and routed to liveBackend!
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK after transparent failover, got %d. Body: %s", resp.StatusCode, string(body))
	}
	if string(body) != "failover-success" {
		t.Errorf("expected 'failover-success', got '%s'", string(body))
	}
	if deadBackend.IsAlive() {
		t.Errorf("expected dead backend to be marked offline after failover")
	}
}

func TestProxyHandler_RequestBodyRetry(t *testing.T) {
	deadBackend, _ := backend.NewBackend("http://127.0.0.1:59997")

	var receivedBody string
	liveServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		receivedBody = string(b)
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, "created")
	}))
	defer liveServer.Close()

	liveBackend, _ := backend.NewBackend(liveServer.URL)

	sm, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{deadBackend, liveBackend})
	collector := metrics.NewCollector()
	limiter := ratelimit.NewLimiter(10.0, 10, false)
	defer limiter.Close()

	handler := NewProxyHandlerWithOptions(sm, collector, limiter, ProxyOptions{
		MaxRetries:         2,
		MaxRetryBodyBytes:  1024,
		RetryNonIdempotent: true,
	})

	postPayload := `{"item":"test-order","qty":42}`
	req := httptest.NewRequest("POST", "http://nexuslb.local/orders", strings.NewReader(postPayload))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("expected 201 Created after retry, got %d", rec.Code)
	}
	if receivedBody != postPayload {
		t.Errorf("expected live server to receive full request body %s, got %s", postPayload, receivedBody)
	}
}

func TestProxyHandler_IdempotencyRetryPolicy(t *testing.T) {
	liveServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, string(b))
	}))
	defer liveServer.Close()

	// 1. Plain POST without Idempotency-Key must NOT retry when RetryNonIdempotent is false
	dead1, _ := backend.NewBackend("http://127.0.0.1:59996")
	live1, _ := backend.NewBackend(liveServer.URL)
	sm1, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{dead1, live1})
	collector1 := metrics.NewCollector()
	limiter1 := ratelimit.NewLimiter(10.0, 10, false)
	defer limiter1.Close()

	hDefault := NewProxyHandler(sm1, collector1, limiter1, 2)
	reqPost := httptest.NewRequest("POST", "http://nexuslb.local/orders", strings.NewReader(`{"op":"charge"}`))
	recPost := httptest.NewRecorder()
	hDefault.ServeHTTP(recPost, reqPost)
	if recPost.Code != http.StatusBadGateway {
		t.Fatalf("expected non-idempotent POST without key to stop after 1 failed attempt with 502, got %d", recPost.Code)
	}

	// 2. POST WITH Idempotency-Key MUST retry cleanly onto live backend
	dead2, _ := backend.NewBackend("http://127.0.0.1:59995")
	live2, _ := backend.NewBackend(liveServer.URL)
	sm2, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{dead2, live2})
	hIdem := NewProxyHandler(sm2, collector1, limiter1, 2)

	reqKey := httptest.NewRequest("POST", "http://nexuslb.local/orders", strings.NewReader(`{"op":"charge-safe"}`))
	reqKey.Header.Set("Idempotency-Key", "idem-123")
	recKey := httptest.NewRecorder()
	hIdem.ServeHTTP(recKey, reqKey)
	if recKey.Code != http.StatusOK || recKey.Body.String() != `{"op":"charge-safe"}` {
		t.Fatalf("expected idempotent POST to retry cleanly and return payload, got %d %q", recKey.Code, recKey.Body.String())
	}

	// 3. PUT is idempotent by default and MUST retry cleanly
	dead3, _ := backend.NewBackend("http://127.0.0.1:59994")
	live3, _ := backend.NewBackend(liveServer.URL)
	sm3, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{dead3, live3})
	hPut := NewProxyHandler(sm3, collector1, limiter1, 2)

	reqPut := httptest.NewRequest("PUT", "http://nexuslb.local/orders/1", strings.NewReader(`{"state":"paid"}`))
	recPut := httptest.NewRecorder()
	hPut.ServeHTTP(recPut, reqPut)
	if recPut.Code != http.StatusOK || recPut.Body.String() != `{"state":"paid"}` {
		t.Fatalf("expected PUT to retry cleanly, got %d %q", recPut.Code, recPut.Body.String())
	}
}

func TestProxyHandler_MaxRetryBodyBytes(t *testing.T) {
	liveServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "%d:%s", len(b), string(b[:5]))
	}))
	defer liveServer.Close()

	// Payload larger than cap (32 bytes limit vs 100 bytes body) must NOT retry
	// when first backend fails, and must still stream intact when first backend is live.
	dead, _ := backend.NewBackend("http://127.0.0.1:59993")
	live, _ := backend.NewBackend(liveServer.URL)
	smDeadFirst, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{dead, live})
	collector := metrics.NewCollector()
	limiter := ratelimit.NewLimiter(10.0, 10, false)
	defer limiter.Close()

	handler := NewProxyHandlerWithOptions(smDeadFirst, collector, limiter, ProxyOptions{
		MaxRetries:        2,
		MaxRetryBodyBytes: 32,
	})

	largePayload := strings.Repeat("abcde", 20) // 100 bytes
	reqTooLarge := httptest.NewRequest("PUT", "http://nexuslb.local/upload", strings.NewReader(largePayload))
	recTooLarge := httptest.NewRecorder()
	handler.ServeHTTP(recTooLarge, reqTooLarge)
	if recTooLarge.Code != http.StatusBadGateway {
		t.Fatalf("expected oversized PUT body (>32 bytes) not to retry across dead backend, got %d", recTooLarge.Code)
	}

	// Unknown Content-Length (chunked stream) > 32 bytes hitting live backend must splice full intact stream
	smLiveOnly, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{live})
	hStream := NewProxyHandlerWithOptions(smLiveOnly, collector, limiter, ProxyOptions{
		MaxRetries:        2,
		MaxRetryBodyBytes: 32,
	})
	reqChunked := httptest.NewRequest("PUT", "http://nexuslb.local/upload", io.NopCloser(strings.NewReader(largePayload)))
	reqChunked.ContentLength = -1 // simulate chunked unknown length
	recStream := httptest.NewRecorder()
	hStream.ServeHTTP(recStream, reqChunked)
	if recStream.Code != http.StatusOK || recStream.Body.String() != "100:abcde" {
		t.Fatalf("expected chunked oversized stream to splice full 100 bytes cleanly, got %d %q", recStream.Code, recStream.Body.String())
	}
}

func TestProxyHandler_RequestID_GeneratedAndPreserved(t *testing.T) {
	var upstreamSeenIDs []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		upstreamSeenIDs = append(upstreamSeenIDs, id)
		// Deliberately echo X-Request-ID back from upstream to ensure no duplicate headers
		w.Header().Set("X-Request-ID", id)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	b, _ := backend.NewBackend(upstream.URL)
	sm, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{b})
	collector := metrics.NewCollector()
	limiter := ratelimit.NewLimiter(10.0, 10, false)
	defer limiter.Close()

	handler := NewProxyHandler(sm, collector, limiter)

	// 1. Missing X-Request-ID -> auto-generated, propagated, single header value, recorded in log
	reqGen := httptest.NewRequest("GET", "http://nexuslb.local/a", nil)
	recGen := httptest.NewRecorder()
	handler.ServeHTTP(recGen, reqGen)

	genID := recGen.Header().Get("X-Request-ID")
	if genID == "" || len(recGen.Header().Values("X-Request-ID")) != 1 {
		t.Fatalf("expected single generated X-Request-ID header, got %v", recGen.Header().Values("X-Request-ID"))
	}
	if len(upstreamSeenIDs) != 1 || upstreamSeenIDs[0] != genID {
		t.Fatalf("expected upstream to receive %q, got %v", genID, upstreamSeenIDs)
	}

	// 2. Caller-provided X-Request-ID -> preserved end-to-end
	reqKeep := httptest.NewRequest("GET", "http://nexuslb.local/b", nil)
	reqKeep.Header.Set("X-Request-ID", "trace-client-999")
	recKeep := httptest.NewRecorder()
	handler.ServeHTTP(recKeep, reqKeep)

	if recKeep.Header().Get("X-Request-ID") != "trace-client-999" {
		t.Fatalf("expected preserved X-Request-ID trace-client-999, got %q", recKeep.Header().Get("X-Request-ID"))
	}
	snap := collector.Snapshot("Round Robin", []*backend.Backend{b})
	if len(snap.RecentLogs) < 2 || snap.RecentLogs[0].ID != "trace-client-999" {
		t.Fatalf("expected newest metric log to carry trace-client-999, got %+v", snap.RecentLogs)
	}
}

func TestProxyHandler_NoDuplicateXFFOnRetry(t *testing.T) {
	deadBackend, _ := backend.NewBackend("http://127.0.0.1:59992")

	var gotXFF string
	liveServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotXFF = r.Header.Get("X-Forwarded-For")
		w.WriteHeader(http.StatusOK)
	}))
	defer liveServer.Close()

	liveBackend, _ := backend.NewBackend(liveServer.URL)
	sm, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{deadBackend, liveBackend})
	collector := metrics.NewCollector()
	limiter := ratelimit.NewLimiter(10.0, 10, false)
	defer limiter.Close()

	handler := NewProxyHandler(sm, collector, limiter, 2)
	req := httptest.NewRequest("GET", "http://nexuslb.local/retry-xff", nil)
	req.RemoteAddr = "203.0.113.45:5555"
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK after retry, got %d", rec.Code)
	}
	if gotXFF != "203.0.113.45" {
		t.Fatalf("expected single non-duplicated X-Forwarded-For '203.0.113.45' on retried backend, got %q", gotXFF)
	}
}

func TestProxyHandler_CanaryFallbackNoHeaderLeak(t *testing.T) {
	// Dead canary node is tried first on canary request, then fails over to healthy stable node.
	deadCanary, _ := backend.NewBackend("http://127.0.0.1:59991")
	deadCanary.SetCanary(true, "v2.0-canary")

	stableServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "stable-ok")
	}))
	defer stableServer.Close()

	stableBackend, _ := backend.NewBackend(stableServer.URL)
	sm, _ := balancer.NewStrategyManager("round_robin", []*backend.Backend{deadCanary, stableBackend})
	collector := metrics.NewCollector()
	limiter := ratelimit.NewLimiter(10.0, 10, false)
	defer limiter.Close()

	handler := NewProxyHandler(sm, collector, limiter, 2)
	req := httptest.NewRequest("GET", "http://nexuslb.local/canary-fallback", nil)
	req.Header.Set("X-Canary", "true")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || rec.Body.String() != "stable-ok" {
		t.Fatalf("expected fallback to stable backend 200 stable-ok, got %d %q", rec.Code, rec.Body.String())
	}
	if tag := rec.Header().Get("X-Nexus-Canary"); tag != "" {
		t.Fatalf("expected no X-Nexus-Canary header leak when fallback hit stable node, got %q", tag)
	}
}

