# 02 : Asymmetric Backend Starvation and Connection Clustering in Least Connections Routing

# Clustering on Minimum-Connection Ties During Sequential Request Bursts

## Error:

Traffic distribution benchmarks revealed severe load skewing when using Least Connections under sequential and rapid-burst traffic:

> [LOAD TEST] Benchmark: 1,000 Sequential Requests (Least Connections)
> [DISTRIBUTION] Backend 1: 994 reqs (99.4%), Backend 2: 3 reqs (0.3%), Backend 3: 3 reqs (0.3%)
> [WARNING] Node starvation detected: Backend instances 2 and 3 remained idle despite identical capacity and zero active connections.

## Relevant Context

The classic Least Connections algorithm routes incoming traffic to whichever healthy upstream node currently has the lowest number of active in-flight TCP connections (`ActiveConnections`).

In the prototype implementation of `LeastConnections` in `pkg/balancer/least_conn.go`, the selection loop inspected nodes linearly from index `0` through `n-1`:

```go
func (lc *LeastConnections) NextBackend(r *http.Request) (*backend.Backend, error) {
    lc.mux.RLock()
    defer lc.mux.RUnlock()

    var chosen *backend.Backend
    minConns := int64(-1)

    // Naive linear scan from index 0 to n-1
    for _, candidate := range lc.backends {
        if !candidate.IsAlive() {
            continue
        }

        conns := atomic.LoadInt64(&candidate.ActiveConnections)
        if minConns == -1 || conns < minConns {
            minConns = conns
            chosen = candidate
        }
    }

    if chosen == nil {
        return nil, ErrNoHealthyBackends
    }
    return chosen, nil
}
```

When client requests were processed with high response velocities (e.g. sub-millisecond latencies for caching endpoints or fast APIs), each connection was established, handled, and closed before the next incoming request reached the reverse proxy.

## Key Observation

Under real-world workloads, many requests have short execution durations. When consecutive requests arrive sequentially or with slight interleaving:

1. **Persistent Zero-State**: When Request 1 arrives, all three backends have `ActiveConnections == 0`. The loop evaluates index 0 (Backend 1) with `conns == 0`, sets `minConns = 0`, and selects Backend 1.
2. **Immediate Decrement**: By the time Request 2 arrives 1 millisecond later, Backend 1 has already responded and decremented its counter back to `0`.
3. **Index 0 Bias (First Minimum Trap)**: The comparison `conns < minConns` evaluates to `false` for Backend 2 (`0 < 0` is false) and Backend 3 (`0 < 0` is false). Consequently, index 0 is chosen repeatedly, starving subsequent backends and degenerating a multi-node cluster into a single-node bottleneck.
4. Conversely, using strict `<=` (`conns <= minConns`) merely shifted the bias to the final index (`n-1`), repeating the exact same monopolization in reverse.

To preserve the benefits of Least Connections during latency spikes without devolving into single-server starvation during low-latency operations, equal-connection states ("ties") required fair, non-deterministic arbitration.

## Solution

Implemented an atomic rotating circular offset (`lc.offset`) that dynamically shifts the starting evaluation index for each incoming request, yielding fair Round-Robin tie-breaking across nodes with identical active connection counts:

```go
func (lc *LeastConnections) NextBackend(r *http.Request) (*backend.Backend, error) {
    lc.mux.RLock()
    defer lc.mux.RUnlock()

    n := len(lc.backends)
    if n == 0 {
        return nil, ErrNoHealthyBackends
    }

    // Circular offset advances atomically on every request for fair tie-breaking
    startIdx := int(atomic.AddUint64(&lc.offset, 1) - 1) % n

    var chosen *backend.Backend
    minConns := int64(-1)

    // Evaluate nodes circularly starting from the dynamic offset
    for i := 0; i < n; i++ {
        idx := (startIdx + i) % n
        candidate := lc.backends[idx]

        if !candidate.IsAlive() {
            continue
        }

        conns := atomic.LoadInt64(&candidate.ActiveConnections)
        if minConns == -1 || conns < minConns {
            minConns = conns
            chosen = candidate
        }
    }

    if chosen == nil {
        return nil, ErrNoHealthyBackends
    }

    return chosen, nil
}
```

**Because**

Advancing an atomic offset `startIdx := int(atomic.AddUint64(&lc.offset, 1) - 1) % n` ensures that whenever multiple backends tie for the lowest active connection count (e.g., all at 0 or all at 1), the first evaluated backend changes sequentially on every request. This seamlessly blends the throughput-maximizing fairness of Round-Robin during uniform zero-backlog states with the adaptive load-shedding intelligence of Least Connections during upstream latency surges.
