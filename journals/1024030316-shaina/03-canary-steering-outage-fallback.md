# 03 : Progressive Delivery Failure and Hard Outages During Canary Node Degradation

# Layer 7 Multi-Vector Canary Traffic Steering with Transparent Cluster Fallback

## Error:

During canary deployment testing of a new upstream service revision (`v2.0-canary`), taking down the designated canary node for routine maintenance or handling an unhandled exception triggered total request failures for all canary-targeted users:

```
[TEST] Client Request: GET /api/v2/catalog (Header: X-Canary: true)
[PROXIED] Attempting routing to Canary Node: http://localhost:8003 (Status: DOWN / UNHEALTHY)
[ERROR]   HTTP 503 Service Unavailable: no healthy canary backends available
[OUTAGE]  100% of canary-flagged beta clients experienced immediate connection drops and 503 errors despite Nodes 1 and 2 running healthy at 0% load.
```

## Relevant Context

Progressive delivery pipelines isolate risk by deploying new code revisions to a tiny fraction of infrastructure (the Canary instance). Clients opted into beta testing are identified via Layer 7 attributes such as headers (`X-Canary: true`), URL query parameters (`?canary=true`), or session cookies (`canary=true`).

In early architecture drafts, Canary routing was implemented as a segregated, mutually exclusive routing strategy:

```go
// Flawed Isolated Canary Balancer Implementation
func (cm *CanaryManager) NextBackend(r *http.Request) (*backend.Backend, error) {
    if isCanary(r) {
        // Look only within dedicated canary nodes
        for _, b := range cm.canaryNodes {
            if b.IsAlive() {
                return b, nil
            }
        }
        // Fatal Error: Dropped client traffic when canary node was down
        return nil, ErrNoHealthyCanaryBackends
    }
    return cm.stableStrategy.NextBackend(r)
}
```

This naive isolation introduced two critical flaws:
1. **Strategy Incompatibility**: Canary steering was locked into an isolated wrapper rather than functioning harmoniously across all cluster routing algorithms (Round Robin, Least Connections, IP Hash, and Weighted RR).
2. **Hard Outage on Canary Failure**: Because the canary node was running experimental code, it had a higher likelihood of transient failure or crash loops. Dropping canary users with HTTP 503 completely contradicted the core principle of progressive delivery—which is to protect user experience.

## Key Observation

1. **Multi-Vector L7 Ingress**: Real-world clients communicate over diverse protocols and interfaces. While automated API pipelines use HTTP request headers (`X-Canary: true`), browser-based testers often arrive via query strings (`?canary=true`) or persistent tracking cookies (`Cookie: canary=true`). All three vectors must be inspected concurrently without payload parsing overhead.
2. **Global Pre-Routing Interception**: Canary evaluation must execute *before* the active cluster balancing strategy, regardless of whether the cluster is running Round Robin, Least Connections, IP Hash, or Weighted Round Robin.
3. **Self-Healing Fallback Guarantee**: If all canary nodes fail health probes or are manually switched off, requests tagged with `X-Canary` must **never** be dropped. They must transparently degrade and fall back to the active cluster strategy (Nodes 1 and 2), providing unbroken service continuity.
4. **Ingress Response Tracing**: The client and upstream observability pipeline must know whether a request was served by the Canary node or a fallback stable node via explicit response headers (`X-Nexus-Canary: v2.0-canary`).

## Solution

Engineered a multi-vector Layer 7 inspection engine coupled with an automatic fallback mechanism inside `pkg/balancer/manager.go` and `pkg/proxy/proxy.go`:

```go
package balancer

import (
	"net/http"
	"strings"
	"nexuslb/pkg/backend"
)

// IsCanaryRequest detects canary targeting across headers, query parameters, and cookies.
func IsCanaryRequest(r *http.Request) bool {
	if r == nil {
		return false
	}

	// Vector 1: HTTP Header check (X-Canary: true / 1)
	if val := r.Header.Get("X-Canary"); strings.EqualFold(val, "true") || val == "1" {
		return true
	}

	// Vector 2: URL Query parameter (?canary=true / 1)
	if q := r.URL.Query().Get("canary"); strings.EqualFold(q, "true") || q == "1" {
		return true
	}

	// Vector 3: Cookie inspection (Cookie: canary=true / 1)
	if cookie, err := r.Cookie("canary"); err == nil {
		if strings.EqualFold(cookie.Value, "true") || cookie.Value == "1" {
			return true
		}
	}

	return false
}

// NextBackend intercepts canary traffic and provides automatic fallback to active strategy.
func (sm *StrategyManager) NextBackend(r *http.Request) (*backend.Backend, error) {
	sm.mux.RLock()
	defer sm.mux.RUnlock()

	// Step 1: Check for Canary targeting
	if IsCanaryRequest(r) {
		for _, b := range sm.backends {
			if b.IsCanaryNode() && b.IsAlive() {
				return b, nil // Successfully routed to healthy canary
			}
		}
		// Self-Healing: If canary node is offline, log fallback and proceed below
	}

	// Step 2: Route through active strategy (Round Robin, Least Conn, IP Hash, WRR)
	return sm.currentStrategy.NextBackend(r)
}
```

### Upstream Proxy Telemetry Enrichment

In `pkg/proxy/proxy.go`, the proxy injects diagnostic metadata before returning the response to the client:

```go
// Inside proxy ReverseProxy Director / ModifyResponse:
if targetBackend.IsCanaryNode() {
    w.Header().Set("X-Nexus-Canary", targetBackend.GetCanaryTag())
    collector.Record(r.Method, r.URL.Path, targetBackend.URL.String(), 200, latency, clientIP, true)
} else {
    collector.Record(r.Method, r.URL.Path, targetBackend.URL.String(), 200, latency, clientIP, false)
}
```

### Verification & Automated Test Results

Tested in `pkg/balancer/canary_test.go`:
- `TestCanaryRouting_HeaderAndCookie`: Verified `X-Canary: true`, `?canary=true`, and `canary=true` cookie consistently steer traffic to Backend 3 (`is_canary = true`).
- `TestCanaryRouting_FallbackWhenCanaryOffline`: Verified that setting `Backend 3.SetAlive(false)` transparently redirects `X-Canary` traffic across stable Nodes 1 and 2 with 100% success rate and zero 503 errors.

**Because**

Decoupling canary routing into an ingress pre-routing interceptor allows progressive delivery to function across all load balancing algorithms simultaneously. Guaranteeing self-healing fallback to the stable cluster when experimental canary nodes fail eliminates single-point-of-failure risks, ensuring that beta release instability never impacts customer-facing availability.
