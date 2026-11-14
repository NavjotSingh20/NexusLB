# 05 : Orphaned Cluster Perturbations and Struct-Copy Hook Nil Pointer Dereference in Autonomous Chaos Simulation

# Bounded Concurrency Worker Pools, Idempotent Baseline Rollback, and Defensive Hook Pointer Semantics

## Error:

During automated end-to-end multi-scenario chaos simulation testing (launching autonomous background traffic waves coupled with fault injection drills), the load balancer gateway abruptly terminated with a fatal nil pointer panic:

```text
2026/11/12 00:20:16 [INIT] Real-life traffic simulator standby (start via Dashboard or -simulate flag)
2026/11/12 00:20:16 Ingress Proxy:     http://localhost:8080
2026/11/12 00:20:16 Admin Dashboard:   http://localhost:8081
panic: runtime error: invalid memory address or nil pointer dereference
[signal 0xc0000005 code=0x0 addr=0x0 pc=0x7ff6d831996b]

goroutine 1082 [running]:
nexuslb/pkg/simulator.(*Simulator).runNodeOutageWave(0x2d34edc42700, {0x7ff6d8754168, 0x2d34edcae690}, 0x1f4bbb537)
	C:/Users/navjo/OneDrive/Desktop/NexusLB/pkg/simulator/simulator.go:1004 +0x48b
nexuslb/pkg/simulator.(*Simulator).executeWave(0x2d34edc42700, {0x7ff6d8754168, 0x2d34edcae690}, {0x7ff6d839ad85?, 0x14?})
	C:/Users/navjo/OneDrive/Desktop/NexusLB/pkg/simulator/simulator.go:861 +0x585
nexuslb/pkg/simulator.(*Simulator).directorLoop(0x2d34edc42700, {0x7ff6d8754168, 0x2d34edcae690})
	C:/Users/navjo/OneDrive/Desktop/NexusLB/pkg/simulator/simulator.go:787 +0x2b8
created by nexuslb/pkg/simulator.(*Simulator).Start in goroutine 95
	C:/Users/navjo/OneDrive/Desktop/NexusLB/pkg/simulator/simulator.go:350 +0x211
```

Additionally, during earlier integration trials where simulations were stopped mid-wave via `POST /api/sim/stop`, backends were left in an **orphaned degraded state**:
- Node 1 retained an injected artificial latency of `350ms`.
- Nodes 2 & 3 retained asymmetric capacity weights (`5:4:1`), permanently skewing Weighted Round Robin routing after the test had concluded.
- Node 2 remained flagged as offline (`alive = false`), requiring a manual restart of the entire gateway process.

## Relevant Context

NexusLB’s built-in **Real-Life Traffic & Chaos Simulator** (`pkg/simulator/simulator.go`) stress-tests the reverse proxy and auto-mitigation algorithms by continuously generating:
1. **Diverse Client Traffic**: Poisson-distributed arrival of randomized public IPv4 addresses across 8 CIDR blocks, sticky client sessions, canary headers (`X-Canary: true`), and slow database queries.
2. **Reversible Chaos Waves**: Autonomous multi-node failure drills (`node_outage_failover`), artificial latency spikes (`latency_spike`), and progressive canary rollouts (`canary_weighted_rollout`).

To decouple the simulator from concrete networking topologies, `Simulator` defines a dependency-inversion interface through `ClusterHooks`:

```go
type ClusterHooks struct {
    ProxyURL            string
    GetBackends         func() []*backend.Backend
    SetBackendDelay     func(url string, ms int64) error
    ToggleBackendHealth func(url, state string) error
    CheckHealth         func()
    StrategyKey         func() string
    ...
}
```

In `New(cfg SimConfig, hooks ClusterHooks) *Simulator`, default implementations were designed to fall back to HTTP calls against `/chaos/*` on mock backends if optional hooks were omitted:

```go
// Flawed Constructor Pattern
func New(cfg SimConfig, hooks ClusterHooks) *Simulator {
    ...
    s := &Simulator{
        hooks:      hooks, // Value copy of hooks struct!
        httpClient: httpClient,
        intensity:  intensity,
        ...
    }

    // Mutating local parameter 'hooks', NOT 's.hooks'!
    if hooks.SetBackendDelay == nil {
        hooks.SetBackendDelay = s.defaultSetBackendDelay
    }
    if hooks.ToggleBackendHealth == nil {
        hooks.ToggleBackendHealth = s.defaultToggleBackendHealth
    }

    return s
}
```

## Key Observation

1. **Go Struct Value-Copy Semantics**: In Go, structs passed by value are copied onto the function call stack. The constructor initialized `s.hooks = hooks` *before* checking whether `SetBackendDelay` or `ToggleBackendHealth` were `nil`. When lines 288-293 populated `hooks.SetBackendDelay`, they mutated the **local copy** on the stack. `s.hooks` inside the returned `*Simulator` remained populated with `nil` function pointers! When the background `directorLoop` entered `runNodeOutageWave` and called `s.hooks.ToggleBackendHealth(urlStr, "down")`, dereferencing the nil function pointer triggered an unrecoverable SIGSEGV panic at runtime.
2. **Orphaned State Under Asynchronous Preemption**: Chaos drills introduce state mutations across multiple subsystems:
   - Backend simulated delay (`b.SetSimulatedDelay(ms)`)
   - Backend capacity weight (`b.SetWeight(w)`)
   - Canary routing tags (`b.SetCanary(true, "v2.0-canary")`)
   - Health status and circuit breaker tripping (`target.SetAlive(false)`)
   When a user clicks "Stop Sim" mid-wave or an immediate wave trigger preempts the current phase, executing the next wave or halting without a transactional rollback left these mutations orphaned in memory, permanently corrupting cluster routing baselines.
3. **Unbounded Goroutine Spawning Under Volumetric Floods**: In naive traffic generators, looping over `for i := 0; i < count; i++ { go fireRequest() }` spawns an unbounded number of concurrent goroutines. Under "heavy" intensity or botnet DDoS waves (70+ RPS), this rapidly exhausts OS file descriptors (ephemeral TCP sockets) and triggers thread starvation.

## Solution

1. **Defensive Hook Pointer Semantics and Nil-Safe Guards**: Fixed the struct copy bug by mutating `s.hooks` directly on the struct pointer, added defensive nil-guards before every hook invocation with graceful fallbacks to in-memory `Backend` methods, and validated HTTP responses from mock chaos endpoints:

```go
func New(cfg SimConfig, hooks ClusterHooks) *Simulator {
    ...
    s := &Simulator{
        hooks:      hooks,
        httpClient: httpClient,
        ...
    }

    // Mutate struct field directly
    if s.hooks.SetBackendDelay == nil {
        s.hooks.SetBackendDelay = s.defaultSetBackendDelay
    }
    if s.hooks.ToggleBackendHealth == nil {
        s.hooks.ToggleBackendHealth = s.defaultToggleBackendHealth
    }

    return s
}

// In runNodeOutageWave:
if s.hooks.ToggleBackendHealth != nil {
    if err := s.hooks.ToggleBackendHealth(urlStr, "down"); err != nil {
        target.SetAlive(false) // Fallback if mock endpoint is unreachable
    } else if s.hooks.CheckHealth != nil {
        s.hooks.CheckHealth()  // Immediately notify active health checker
    }
} else {
    target.SetAlive(false)
}
```

2. **Transactional Chaos Snapshotting (`chaosBackup`)**: Prior to applying any temporary mutation during a chaos wave, the engine captures a snapshot of original per-node parameters under `s.chaosMux`:

```go
type chaosBackup struct {
    delays    map[string]int64
    weights   map[string]int64
    canary    map[string]bool
    canaryTag map[string]string
    downURLs  map[string]bool
}

func (s *Simulator) ensureBackupLocked() *chaosBackup {
    if s.backup == nil {
        s.backup = newChaosBackup()
        for _, b := range s.hooks.GetBackends() {
            u := b.URL.String()
            s.backup.delays[u] = b.GetSimulatedDelay()
            s.backup.weights[u] = b.GetWeight()
            s.backup.canary[u] = b.IsCanaryNode()
            s.backup.canaryTag[u] = b.GetCanaryTag()
        }
    }
    return s.backup
}
```

3. **Deterministic, Idempotent Baseline Restoration (`restoreChaos`)**: Encapsulated state recovery in `restoreChaos(resetCircuitBreakers bool)`. Whether a wave completes naturally, gets preempted by a manual wave trigger, or is aborted via `Stop()`, original weights, latencies, canary tags, node health, and touched circuit breakers are atomically restored:

```go
func (s *Simulator) restoreChaos(resetCircuitBreakers bool) {
    s.chaosMux.Lock()
    bk := s.backup
    s.backup = nil
    touched := s.touchedCircuitURLs
    s.touchedCircuitURLs = nil
    s.chaosMux.Unlock()

    if bk == nil && len(touched) == 0 { return }

    // Revert delays, weights, canary tags, and down nodes...
    for u, origDelay := range bk.delays {
        _ = s.hooks.SetBackendDelay(u, origDelay)
        if b, ok := backendsByURL[u]; ok { b.SetSimulatedDelay(origDelay) }
    }
    for u, origWeight := range bk.weights {
        if b, ok := backendsByURL[u]; ok { b.SetWeight(origWeight) }
    }
    for u := range bk.downURLs {
        if s.hooks.ToggleBackendHealth != nil { _ = s.hooks.ToggleBackendHealth(u, "up") }
        if b, ok := backendsByURL[u]; ok { b.SetAlive(true) }
    }
    if resetCircuitBreakers {
        for u := range touched {
            if b, ok := backendsByURL[u]; ok && b.CircuitBreaker != nil {
                b.CircuitBreaker.Reset()
            }
        }
    }
}
```

4. **Bounded Concurrency Semaphore**: Implemented a buffered channel semaphore (`sem chan struct{}` with `maxInFlight = 64`). Worker goroutines acquire a slot before dispatching HTTP calls and release it upon completion, preventing socket pool exhaustion under aggressive traffic rates.

## Verification & Automated Test Results

Tested in `pkg/simulator/simulator_test.go`:
- `TestSimulator_DefaultChaosHooksNilSafe`: Constructed a simulator omitting `SetBackendDelay` and `ToggleBackendHealth`; executed `WaveLatency` and `WaveNodeOutage`; verified that default hooks executed against an HTTP test server without panic, recorded 100% of calls, and restored state cleanly upon `Stop()`.
- `TestSimulator_ReversibleChaosAndStopRestore`: Verified that an initial non-trivial configuration (`weights = [2, 3, 1]`) was perfectly preserved after triggering and aborting `WaveCanaryWRR` and `WaveLatency`.
- `TestSimulator_DDoSWavesAndEdgeShield429`: Simulated 5 concurrent attacker IPs; verified that Edge Shield correctly dropped attacker requests with HTTP 429 while healthy organic traffic maintained 100% success rate.

**Because**

Chaos testing in operational infrastructure must obey strict reversible transactional boundaries. Coupling pre-wave snapshot isolation with idempotent restoration ensures that synthetic faults never escape their test boundaries or permanently degrade production state, while robust struct-pointer lifecycle semantics guard against fatal runtime panics.

