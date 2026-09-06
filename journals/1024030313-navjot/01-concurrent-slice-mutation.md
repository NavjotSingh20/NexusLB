# 01 : Race Condition in Dynamic Backend Selection Under Concurrent Traffic

# Concurrent Slice Mutation During Health-State Transitions

## Error:

Runtime panic encountered during concurrent request forwarding while background health checks were altering backend availability states:

> panic: runtime error: index out of range [2] with length 2
> goroutine 84 [running]:
> nexuslb/pkg/balancer.(*RoundRobin).NextBackend(0xc0000a6000, 0xc000108000)
> 	/nexuslb/pkg/balancer/balancer.go:41 +0x138

## Relevant Context

In the initial implementation of the Round-Robin algorithm in `pkg/balancer/balancer.go`, the routing engine filtered the slice of backends dynamically on every request to only include active servers:

```go
func (rr *RoundRobin) NextBackend(r *http.Request) (*backend.Backend, error) {
    healthy := make([]*backend.Backend, 0)
    for _, b := range rr.backends {
        if b.Alive {
            healthy = append(healthy, b)
        }
    }

    if len(healthy) == 0 {
        return nil, ErrNoHealthyBackends
    }

    // Advance counter and modulo with the filtered length
    idx := atomic.AddUint64(&rr.current, 1) % uint64(len(healthy))
    return healthy[idx], nil
}
```

Simultaneously, the active health checker goroutine in `pkg/health/checker.go` periodically probed upstream targets and directly mutated the boolean flag `b.Alive = false` upon receiving timeouts or HTTP 5xx responses.

## Key Observation

The design suffered from two major concurrency flaws:

1. **Data Race on `b.Alive`**: Reading and mutating `b.Alive` across simultaneous HTTP worker goroutines and the background health checker without synchronization caused memory access races (`go test -race` signaled immediate data race warnings).
2. **Unsynchronized Slice Allocation and Stale Indexing**: Filtering a dynamic slice `healthy` inside the hot request path incurred heap allocations on every request. Worse, under high concurrency, another goroutine could change the effective count between checking `len(healthy)` and referencing `healthy[idx]`, leading to index out-of-range panics or inconsistent load distributions.

## Solution

Replaced dynamic slice re-allocation with a fixed-size ring of backend pointers, protecting node health states with thread-safe atomic accessors, and implementing ring traversal from a circular atomic counter:

```go
func (rr *RoundRobin) NextBackend(r *http.Request) (*backend.Backend, error) {
    rr.mux.RLock()
    defer rr.mux.RUnlock()

    n := len(rr.backends)
    if n == 0 {
        return nil, ErrNoHealthyBackends
    }

    // Atomic fetch-and-add on a single global sequence counter
    startIdx := int(atomic.AddUint64(&rr.current, 1) - 1) % n

    // Inspect nodes sequentially across the fixed ring without mutating the slice
    for i := 0; i < n; i++ {
        candidate := rr.backends[(startIdx+i)%n]
        if candidate.IsAlive() {
            return candidate, nil
        }
    }

    return nil, ErrNoHealthyBackends
}
```

**Because**

Iterating over a static slice using modular arithmetic `(startIdx + i) % n` eliminates heap allocations in the critical request path. Decoupling the counter progression from dynamic slice shrinking ensures that index bounds are invariant, while atomic reads on `candidate.IsAlive()` allow lock-free state queries that remain completely thread-safe under heavy concurrent traffic.
