# 02 : Goroutine Leakage and Premature Stream Severing in Real-Time SSE Telemetry

# HTTP Server WriteTimeout Conflict and Orphaned Stream Worker Accumulation

## Error:

The real-time observability dashboard experienced continuous connection dropping and severe background goroutine accumulation during soak testing:

> [DASHBOARD CLIENT] EventSource connection closed abruptly after 15 seconds: net/http: abort Handler: context deadline exceeded
> [CLIENT RECONNECT] Attempting reconnect in 2000ms... (UI flashing / telemetry jitter)
> [DIAGNOSTICS] runtime.NumGoroutine() jumped from 14 to 386 after 50 page reloads
> [LEAK WARNING] Hundreds of orphaned stream loops remained active in memory, repeatedly marshalling JSON snapshots for closed TCP sockets.

## Relevant Context

NexusLB features an embedded real-time observability dashboard that streams metrics (throughput RPS, average latency, active backend states, and live traffic logs) to the browser UI using Server-Sent Events (SSE) via `/api/stream` in `pkg/dashboard/server.go`.

The HTTP server was initialized with standard defensive security timeouts to protect the proxy against Slowloris attacks:

```go
s.httpServer = &http.Server{
    Addr:         s.addr,
    Handler:      mux,
    ReadTimeout:  10 * time.Second,
    WriteTimeout: 15 * time.Second, // Standard defensive HTTP write timeout
}
```

The SSE streaming handler maintained an infinite timer loop to serialize and push telemetry snapshots every second:

```go
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
    flusher, _ := w.(http.Flusher)
    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")

    ticker := time.NewTicker(1 * time.Second)

    // Unbounded streaming loop
    for range ticker.C {
        stats := s.collector.Snapshot(s.balancer.Name(), s.balancer.GetBackends())
        data, _ := json.Marshal(stats)
        fmt.Fprintf(w, "data: %s\n\n", data)
        flusher.Flush()
    }
}
```

## Key Observation

This architecture introduced two interlocking architectural conflicts between standard HTTP timeout semantics and long-lived streaming protocols:

1. **Global `WriteTimeout` Deadline Collision**: In Go's `net/http` package, `http.Server.WriteTimeout` does not measure inactivity between writes; it measures the **cumulative wall-clock duration** from the moment the request header is read until the response is completely terminated. Because SSE connections are designed to persist indefinitely (hours or days), a 15-second `WriteTimeout` forcefully killed the connection after 15 seconds with `context deadline exceeded`, triggering continuous UI reconnect cycles.
2. **Channel-Loop Goroutine Leakage**: When a client refreshed the browser or closed the tab, the underlying TCP socket was closed by the browser. However, the handler was blocked inside `for range ticker.C`. Because `fmt.Fprintf` writes to a local TCP send buffer, buffered writes do not consistently return immediate errors upon client disconnect. Consequently, the handler loop never broke, and the goroutine remained permanently allocated in RAM. Reloading the page spawned a new goroutine while the old one continued marshalling metrics indefinitely.
3. **Timer Leak**: Creating `time.NewTicker` without explicit cleanup (`ticker.Stop()`) left underlying runtime timer structures pinned in memory.

## Solution

Resolved the streaming lifetime conflict by configuring zero `WriteTimeout` on the HTTP server while shifting lifecycle governance to explicit per-request cancellation detection using `r.Context().Done()`:

```go
func (s *Server) Start() error {
    mux := http.NewServeMux()
    // Register routes...

    s.httpServer = &http.Server{
        Addr:         s.addr,
        Handler:      mux,
        ReadTimeout:  10 * time.Second,
        WriteTimeout: 0, // Zero timeout allows long-lived SSE streaming connections
    }

    return s.httpServer.ListenAndServe()
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
    flusher, ok := w.(http.Flusher)
    if !ok {
        http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")
    w.Header().Set("Access-Control-Allow-Origin", "*")

    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop() // Prevents timer resource leak

    // Push initial snapshot immediately upon connection
    stats := s.getStats()
    data, _ := json.Marshal(stats)
    fmt.Fprintf(w, "data: %s\n\n", data)
    flusher.Flush()

    for {
        select {
        case <-r.Context().Done():
            // Client closed tab or navigated away; clean exit
            return

        case <-ticker.C:
            stats := s.getStats()
            data, err := json.Marshal(stats)
            if err != nil {
                continue
            }

            _, err = fmt.Fprintf(w, "data: %s\n\n", data)
            if err != nil {
                // Socket disconnected; terminate loop immediately
                return
            }
            flusher.Flush()
        }
    }
}
```

**Because**

Setting `WriteTimeout: 0` removes the arbitrary global execution cap, enabling legitimate persistent SSE streams. Simultaneously, listening to `r.Context().Done()` inside a non-blocking `select` multiplexer ensures that when the client terminates the connection, the cancellation signal propagates instantaneously down the Go runtime context tree, terminating the streaming loop, releasing the ticker, and reclaiming the goroutine without leaking memory.
