# 01 : Upstream Status Capture and Header Mutation Failure with httputil.ReverseProxy

# Response Telemetry Loss in Streaming Reverse Proxy Pipeline

## Error:

Metrics collector and real-time dashboard recorded all proxied requests with status code `200 OK` and zero error counts, even when upstream backend servers threw HTTP 500 errors or crashed mid-connection:

> [METRICS REPORT] Recorded Status: 200 OK, Target: :8002 (Actual Upstream Response: 500 Internal Server Error)
> [DASHBOARD] Total Errors: 0, Server Errors: 0 (False telemetry reporting)

## Relevant Context

In `pkg/proxy/proxy.go`, the proxy handler utilized Go's standard library `net/http/httputil.ReverseProxy` to stream client requests directly to backend servers:

```go
func (p *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    target, _ := p.balancer.NextBackend(r)

    start := time.Now()
    target.ReverseProxy.ServeHTTP(w, r)
    duration := time.Since(start)

    // Attempting to record metrics post-execution
    p.collector.Record(r.Method, r.URL.Path, clientIP, target.URL.String(), 200, duration)
}
```

The telemetry layer required capturing the true HTTP status code returned by the upstream server to calculate error rates, update backend health records, and feed the real-time request activity table.

## Key Observation

The Go standard library `http.ResponseWriter` is an interface providing `Header()`, `Write([]byte)`, and `WriteHeader(statusCode int)`. Crucially, it does **not** expose a getter method (such as `StatusCode()`) to read back the status code after it has been written.

When `target.ReverseProxy.ServeHTTP(w, r)` executes, it handles the round-trip internally: it streams headers and chunks of the response body directly into `w`. Once `ReverseProxy` invokes `w.WriteHeader(code)`, the status code is committed to the underlying TCP connection and cannot be inspected by outer wrapping code.

Furthermore, if a backend connection fails outright (e.g. connection refused, network partition), the default `ReverseProxy` implementation directly writes an unformatted `502 Bad Gateway` error and closes the stream, bypassing the load balancer's error statistics and health recovery counters entirely.

## Solution

Implemented a specialized `responseRecorder` wrapper that intercepts and captures status codes transparently without interfering with streaming payloads, combined with a custom `ReverseProxy.ErrorHandler`:

```go
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
```

And integrated into the proxy execution pipeline:

```go
rec := &responseRecorder{
    ResponseWriter: w,
    statusCode:     http.StatusOK,
}

target.ReverseProxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
    target.SetAlive(false)
    rec.statusCode = http.StatusBadGateway
    rw.Header().Set("Content-Type", "application/json")
    rw.WriteHeader(http.StatusBadGateway)
    json.NewEncoder(rw).Encode(map[string]string{
        "error": "Bad Gateway",
        "message": "NexusLB: Backend connection failed",
    })
}

target.ReverseProxy.ServeHTTP(rec, r)

elapsed := time.Since(start)
target.RecordRequest(elapsed, rec.statusCode >= 500)
p.collector.Record(r.Method, r.URL.Path, clientIP, target.URL.String(), rec.statusCode, elapsed)
```

**Because**

Wrapping `http.ResponseWriter` at the handler boundary intercepts the `WriteHeader` call as it transitions from the upstream response stream to the client socket. This enables non-invasive metric telemetry extraction without buffering the HTTP payload in RAM, preserving full zero-allocation streaming efficiency while accurately capturing upstream health degradation.
