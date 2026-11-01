# 04 : Premature Socket Commitment and Broken Failover in Mid-Stream Upstream Socket Resets

# Two-Phase Staged Response Recording and Non-Idempotent Request Replay

## Error:

During automated fault injection testing (abruptly terminating an upstream process while in-flight traffic was being dispatched), clients received immediate `502 Bad Gateway` errors instead of being transparently re-routed to surviving healthy backends:

```text
[CHAOS TEST] Simulating catastrophic socket drop on Backend 1 (http://localhost:8001)
[CLIENT]    curl -i http://localhost:8080/api/checkout -d '{"cart_id":42}'
HTTP/1.1 502 Bad Gateway
Content-Type: application/json
Date: Mon, 02 Nov 2026 00:15:32 GMT
Content-Length: 68

{"error":"Bad Gateway","message":"NexusLB: Backend connection failed"}

[ANOMALY] Client received HTTP 502 Bad Gateway! Request was dropped despite Nodes 2 & 3 running at 0% load.
[ANOMALY] WebSocket upgrade test to ws://localhost:8080/ws failed: "underlying ResponseWriter does not implement http.Hijacker".
```

## Relevant Context

NexusLB’s architecture documentation specifies a high-availability invariant: *if an upstream server crashes or drops mid-stream, client requests must be transparently retried on an alternate healthy node with zero dropped requests*.

In the initial proxy implementation, `ProxyHandler.ServeHTTP` delegated execution directly to `httputil.ReverseProxy`:

```go
// Flawed direct execution without retry staging
func (p *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    ...
    target, err := p.balancer.NextBackend(r)
    rec := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}

    // Direct invocation writes headers straight to client socket
    target.ReverseProxy.ServeHTTP(rec, r)

    elapsed := time.Since(start)
    target.RecordRequest(elapsed, rec.statusCode >= 500)
    p.collector.Record(..., rec.statusCode, elapsed)
}
```

When Backend 1 crashed, Go's reverse proxy caught the connection refusal in its `ErrorHandler` and executed `w.WriteHeader(http.StatusBadGateway)`.

## Key Observation

1. **The Irreversibility of HTTP Socket Commitment**: In the HTTP/1.1 protocol, once the status line and headers (`HTTP/1.1 502 Bad Gateway\r\n...`) are written to the client TCP socket, the connection state is irreversibly committed. The proxy cannot "rewind" the network buffer or send a subsequent `200 OK` from an alternate server. Any failover mechanism must intercept failures *before* byte transmission to the client socket.
2. **Request Body Exhaustion on Streaming Pipes**: An HTTP request payload (`r.Body`) is an unbuffered `io.ReadCloser` streaming pipe. Once `target.ReverseProxy` reads `r.Body` to send bytes to Backend 1, the stream is exhausted (`io.EOF`). Attempting to forward `r` to Backend 2 without replenishing the stream causes Backend 2 to receive an empty body, failing POST/PUT API contracts with `400 Bad Request`.
3. **Protocol Stripping with Incomplete Wrappers**: `httputil.ReverseProxy` detects protocol upgrades (e.g. WebSockets, HTTP/2 multiplexing) and real-time streaming (Server-Sent Events) by checking if `http.ResponseWriter` implements `http.Hijacker` and `http.Flusher`. Because `responseRecorder` omitted these methods, WebSocket handshakes failed with panics and SSE streams were buffered indefinitely.

## Solution

1. **Two-Phase Staged Response Recorder (`retryRecorder`)**: Engineered a specialized response recorder in `pkg/proxy/proxy.go` that stages status codes, headers, and payload bytes in memory. Socket writes to the client `http.ResponseWriter` are postponed until the backend completes transmission without connection errors:

```go
type retryRecorder struct {
    underlying http.ResponseWriter
    statusCode int
    header     http.Header
    body       bytes.Buffer
    committed  bool
    failed     bool
    failureErr error
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

func (rec *retryRecorder) SetFailed(err error) {
    rec.failed = true
    rec.failureErr = err
}

func (rec *retryRecorder) Commit() {
    if rec.committed || rec.failed { return }
    rec.committed = true
    for k, vv := range rec.header {
        for _, v := range vv { rec.underlying.Header().Add(k, v) }
    }
    rec.underlying.WriteHeader(rec.statusCode)
    if rec.body.Len() > 0 {
        rec.underlying.Write(rec.body.Bytes())
    }
}
```

2. **Non-Idempotent Request Body Buffering**: In `ServeHTTP`, the incoming `r.Body` is buffered into memory once. Prior to each retry attempt, `r.Body` is re-wrapped with `io.NopCloser(bytes.NewReader(bodyBytes))`:

```go
var bodyBytes []byte
if r.Body != nil {
    bodyBytes, _ = io.ReadAll(r.Body)
    r.Body.Close()
}

for attempts < maxAttempts {
    attempts++
    if bodyBytes != nil {
        r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
    }
    target, err := p.balancer.NextBackend(r)
    ...
    rec := newRetryRecorder(w)
    target.ReverseProxy.ServeHTTP(rec, r)

    if rec.failed {
        // Socket drop caught before client write; try alternate backend
        target.RecordRequest(attemptDuration, true)
        continue
    }

    // Success: flush staged headers and body
    rec.Commit()
    return
}
```

3. **Protocol Passthrough Implementation**: Added `Hijack()`, `Flush()`, and `Unwrap()` to `retryRecorder`. Calling `Hijack()` or `Flush()` commits any staged data immediately and un-wraps to the underlying network socket, fully enabling WebSocket protocols and streaming endpoints.

## Verification

Validated transparent retry and payload integrity via automated test suites in `pkg/proxy/proxy_test.go`:

```bash
go test -v ./pkg/proxy -run TestProxyHandler_TransparentFailover
=== RUN   TestProxyHandler_TransparentFailover
--- PASS: TestProxyHandler_TransparentFailover (0.00s)
PASS

go test -v ./pkg/proxy -run TestProxyHandler_RequestBodyRetry
=== RUN   TestProxyHandler_RequestBodyRetry
--- PASS: TestProxyHandler_RequestBodyRetry (0.00s)
PASS

go test -v ./pkg/proxy -run TestResponseRecorder_FlusherAndHijacker
=== RUN   TestResponseRecorder_FlusherAndHijacker
--- PASS: TestResponseRecorder_FlusherAndHijacker (0.00s)
PASS
```

When Backend 1 was killed mid-connection, the proxy intercepted the error in memory, selected Backend 2, delivered the complete request body, and returned `200 OK` to the client in `< 2ms` with zero dropped requests.

