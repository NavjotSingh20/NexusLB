# 03 : Micro-Burst Spikes and Uneven Interleaving in Weighted Round-Robin Scheduling

# Smooth Weighted Round-Robin (SWRR) Dynamic Weight Tracking and Failure Normalization

## Error:

Traffic benchmark logs under high request rates revealed severe clustering when testing an initial naive weighted round-robin implementation with weights `{5, 1, 1}`:

```
[BENCHMARK] Dispatching 21 sequential requests (Configured Weights: Backend 1 = 5x, Backend 2 = 1x, Backend 3 = 1x)
[SEQUENCE]  B1, B1, B1, B1, B1 -> B2 -> B3 -> B1, B1, B1, B1, B1 -> B2 -> B3 -> B1, B1, B1, B1, B1 -> B2 -> B3
[ANOMALY]   Backend 1 CPU Utilization: 88.4% (Micro-Burst Cluster)
[ANOMALY]   Backend 2 & 3 CPU Utilization: 4.1% (Starved during B1 burst window)
[WARNING]   Upstream latency spike: Backend 1 request queue backlog caused p99 latency to jump from 1.2ms to 28.6ms during consecutive 5-request bursts.
```

## Relevant Context

In heterogeneous server environments, upstream backends possess unequal compute resources (e.g., dual quad-core nodes vs. single-core worker instances). Assigning integer weights allows higher-capacity instances to absorb proportionally more incoming traffic.

A naive approach to weighted round-robin uses a simple counter or integer expansion loop:

```go
// Naive Weighted Round-Robin Prototype
func (wrr *NaiveWRR) NextBackend(r *http.Request) (*backend.Backend, error) {
    wrr.mux.Lock()
    defer wrr.mux.Unlock()

    // Exhausts node's entire weight quota before moving to the next node
    for {
        candidate := wrr.backends[wrr.currentIndex]
        if wrr.currentCount < candidate.GetWeight() && candidate.IsAlive() {
            wrr.currentCount++
            return candidate, nil
        }
        wrr.currentCount = 0
        wrr.currentIndex = (wrr.currentIndex + 1) % len(wrr.backends)
    }
}
```

While mathematically accurate over large totals (Backend 1 handles $5 / 7 \approx 71.4\%$ of total requests), this algorithm dispatches all 5 requests consecutively to Backend 1 before delegating a single request to Backend 2 or Backend 3.

## Key Observation

1. **Micro-Burst Congestion**: Concentrating consecutive requests on the highest-weight node creates localized thread pool starvation, socket backlog delays, and CPU thrashing. Rather than smoothing the load, naive weighting creates artificial traffic surges.
2. **Smooth Interleaving Requirement**: For weights $\{5, 1, 1\}$, traffic should not arrive as $\{B_1, B_1, B_1, B_1, B_1, B_2, B_3\}$. Instead, it should interleave smoothly:
   $$\{B_1, B_1, B_2, B_1, B_3, B_1, B_1\}$$
   In this sequence, every node receives requests evenly distributed across time, and no server sits completely idle waiting for another server's weight quota to finish.
3. **Dynamic Re-normalization on Node Outages**: When Backend 1 is manually powered off or fails active health checks, the effective total weight dynamically drops from $5 + 1 + 1 = 7$ to $1 + 1 = 2$. If the algorithm depends on static pre-computed schedules, an offline node either triggers runtime panics or skews traffic ratios.

## Solution

Implemented the NGINX-standard **Smooth Weighted Round-Robin (SWRR)** algorithm in `pkg/balancer/weighted_rr.go`. SWRR tracks stateful, dynamic `CurrentWeight` values for each backend instance:

1. On each incoming request, every healthy backend increases its `CurrentWeight` by its configured effective `Weight`:
   $$\text{CurrentWeight}_i \leftarrow \text{CurrentWeight}_i + \text{Weight}_i$$
2. The healthy node with the maximum `CurrentWeight` is selected.
3. The selected node's `CurrentWeight` is decremented by the total effective weight of all currently healthy backends:
   $$\text{CurrentWeight}_{\text{selected}} \leftarrow \text{CurrentWeight}_{\text{selected}} - \sum_{\text{healthy}} \text{Weight}_i$$

```go
package balancer

import (
	"sync"
	"nexuslb/pkg/backend"
)

type WeightedRoundRobin struct {
	backends []*backend.Backend
	mux      sync.RWMutex
}

func NewWeightedRoundRobin(backends []*backend.Backend) *WeightedRoundRobin {
	return &WeightedRoundRobin{backends: backends}
}

func (w *WeightedRoundRobin) NextBackend(r *http.Request) (*backend.Backend, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	var best *backend.Backend
	var totalWeight int64 = 0

	for _, b := range w.backends {
		if !b.IsAlive() {
			continue
		}

		effWeight := b.GetWeight()
		if effWeight <= 0 {
			effWeight = 1
		}

		// Step 1: Add effective weight to node's dynamic current weight
		b.CurrentWeight += effWeight
		totalWeight += effWeight

		// Step 2: Track node with the highest current weight
		if best == nil || b.CurrentWeight > best.CurrentWeight {
			best = b
		}
	}

	if best == nil || totalWeight == 0 {
		return nil, ErrNoHealthyBackends
	}

	// Step 3: Deduct total healthy weight from the chosen node
	best.CurrentWeight -= totalWeight

	return best, nil
}
```

### Trace Verification

Running 7 consecutive requests through the SWRR engine with weights $\{B_1: 5, B_2: 1, B_3: 1\}$:

| Req | Node State Before Step 1 | Added Weights | Current Weights After Step 1 | Chosen Node (Max) | State After Step 3 ($-7$) |
|:---:|:---|:---|:---|:---:|:---|
| 1 | `B1: 0, B2: 0, B3: 0` | `+5, +1, +1` | `B1: 5, B2: 1, B3: 1` | **B1** | `B1: -2, B2: 1, B3: 1` |
| 2 | `B1: -2, B2: 1, B3: 1` | `+5, +1, +1` | `B1: 3, B2: 2, B3: 2` | **B1** | `B1: -4, B2: 2, B3: 2` |
| 3 | `B1: -4, B2: 2, B3: 2` | `+5, +1, +1` | `B1: 1, B2: 3, B3: 3` | **B2** | `B1: 1, B2: -4, B3: 3` |
| 4 | `B1: 1, B2: -4, B3: 3` | `+5, +1, +1` | `B1: 6, B2: -3, B3: 4` | **B1** | `B1: -1, B2: -3, B3: 4` |
| 5 | `B1: -1, B2: -3, B3: 4` | `+5, +1, +1` | `B1: 4, B2: -2, B3: 5` | **B3** | `B1: 4, B2: -2, B3: -2` |
| 6 | `B1: 4, B2: -2, B3: -2` | `+5, +1, +1` | `B1: 9, B2: -1, B3: -1` | **B1** | `B1: 2, B2: -1, B3: -1` |
| 7 | `B1: 2, B2: -1, B3: -1` | `+5, +1, +1` | `B1: 7, B2: 0, B3: 0` | **B1** | `B1: 0, B2: 0, B3: 0` |

**Final Generated Sequence**: `B1 -> B1 -> B2 -> B1 -> B3 -> B1 -> B1`. Exactly $5 : 1 : 1$, completely interleaved without back-to-back clustering spikes.

**Because**

Continuously incrementing each node by its nominal capacity while penalizing the selected node by the aggregate sum of all surviving healthy nodes mathematically prevents load clustering. If any node fails health checks, its weight is automatically omitted from `totalWeight`, ensuring seamless real-time adaptation without resetting client state or allocating auxiliary memory.
