# NexusLB: High-Availability Reverse Proxy & Intelligent Load Balancer

[![Go Version](https://img.shields.io/badge/Go-1.20%2B-00ADD8?style=flat&logo=go)](https://golang.org/)
[![Status](https://img.shields.io/badge/Status-Active%20MVP-success)](#)
[![Tests](https://img.shields.io/badge/Tests-Passing-brightgreen)](#-testing--verification)
[![License](https://img.shields.io/badge/License-Academic-blue)](#)

> **Course Project for UCS503P (Software Engineering)**  
> **Thapar Institute of Engineering and Technology (TIET), Patiala**  
> **Instructor:** Jhonsy Bansal  
> **Authors (Group 3C23):**  
> - Navjot Singh (`1024030313`)  
> - Shaina Gera (`1024030316`)  
> - Prabhgun Kaur (`1024030320`)  

---

## 📖 Table of Contents

- [Overview](#-overview)
- [Key Features](#-key-features)
- [System Architecture](#-system-architecture)
- [Repository Structure](#-repository-structure)
- [Configuration](#-configuration)
- [Load Balancing Strategies & Hot-Swapping](#-load-balancing-strategies--hot-swapping)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [1. Launch Simulated Backend Cluster](#1-launch-simulated-backend-cluster)
  - [2. Launch NexusLB Ingress Gateway](#2-launch-nexuslb-ingress-gateway)
- [Live Observability Dashboard](#-live-observability-dashboard)
- [Simulating & Testing Active Connections](#-simulating--testing-active-connections)
- [Fault Tolerance & Load Rebalancing](#-fault-tolerance--load-rebalancing)
- [Academic Engineering Journals](#-academic-engineering-journals)
- [API Reference](#-api-reference)
- [Testing & Verification](#-testing--verification)
- [Engineering Highlights & Concurrency](#-engineering-highlights--concurrency)

---

## 🌟 Overview

**NexusLB** is a production-grade, health-aware **Reverse Proxy and Application Load Balancer** engineered from scratch in Go with zero external runtime framework dependencies.

In distributed cloud architectures, simply provisioning multiple backend microservices behind network ports does not guarantee high availability or even utilization. Unmonitored backend crashes, bursty traffic surges, asymmetric compute runtimes, and a lack of operational visibility frequently cause client-facing outages and cascading failures. 

NexusLB resolves these challenges by serving as an intelligent, non-blocking Layer 7 ingress gateway that:

1. **Intelligently routes traffic** across active upstream nodes using pluggable algorithms (Round-Robin, Least Connections, and Deterministic IP Hash).
2. **Monitors backend health actively and reactively**, isolating failing servers instantly with zero dropped requests.
3. **Tracks in-flight concurrency** (`ActiveConnections`) atomically with fair circular tie-breaking.
4. **Presents live telemetry and interactive chaos injection** through an embedded dark-mode dashboard delivered via single-binary `embed.FS` and Server-Sent Events (SSE).

---

## ✨ Key Features

- 🎯 **Pluggable Multi-Strategy Routing Engine**:
  - **Round-Robin**: Monotonically advancing 64-bit atomic ring counter with zero memory allocations on the fast path.
  - **Least Connections**: Dynamically routes to the upstream node with lowest in-flight connections, utilizing an atomic rotating circular offset for fair tie-breaking under sub-millisecond workloads.
  - **Deterministic IP Hashing**: 32-bit FNV-1a hash algorithm featuring client IP normalization (`X-Forwarded-For`, `X-Real-IP`, ephemeral port stripping) and sequential ring walk failover.
  - **Zero-Downtime Hot-Swapping**: Switch algorithms on-the-fly via Web UI or REST API backed by `sync.RWMutex`.
- 🎛️ **Custom Active Connections Simulation & Steppers**:
  - Hold real in-flight connections for custom durations ($N$ connections held for $D$ ms) to observe Least Connections load-shedding live.
  - Interactively increment (`+`), decrement (`-`), or reset (`0`) active connection counters directly on backend cards in the UI or via REST API (`/api/backend/connections`).
- 🩺 **Dual-Mode Health Awareness**:
  - **Proactive Polling Daemon**: Background workers ping `/health` endpoints periodically (default: 3s) with strict timeouts.
  - **Reactive In-Flight Interception**: Custom `httputil.ReverseProxy.ErrorHandler` catches socket resets mid-stream, marks the target node dead, and retries alternate nodes transparently.
- 🔄 **Self-Healing & Auto-Reintegration**: Automatically restores recovered servers to active rotation within $\le 3$ seconds upon receiving `200 OK`.
- 📊 **Embedded Observability Dashboard**:
  - Hermetically bundled into the single binary executable via `embed.FS` (no external runtime asset dependencies).
  - Pushes live cluster telemetry via Server-Sent Events (SSE) every 1 second.
  - Real-time KPIs: Total Requests, Live Throughput (RPS), Average Latency, and Success Rate.
  - Dynamic traffic distribution share bar and real-time request trace logs.
- ⚡ **Interactive Fault Injection**:
  - Individual **ON / OFF power toggle switches** on each backend card to simulate instant outages.
  - Configurable traffic generator (single request, continuous stream, or custom in-flight hold bursts).
  - 1-Click Automated Failover Demo demonstrating baseline, failure, and recovery phases.

---

## 🏗 System Architecture

```
                               ┌────────────────────────┐
                               │     Client Traffic     │
                               └───────────┬────────────┘
                                           │ (Port 8080)
                                           ▼
                               ┌────────────────────────┐
                               │  NexusLB Ingress Proxy │
                               │  (Header Normalization)│
                               └───────────┬────────────┘
                                           │
                                           ▼
                               ┌────────────────────────┐
                               │ Pluggable Strategy Mgr │
                               │ (RR | LeastConn | IPH) │
                               └───────────┬────────────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    │                      │                      │
                    ▼                      ▼                      ▼
         ┌────────────────────┐ ┌────────────────────┐ ┌────────────────────┐
         │  Backend Server 1  │ │  Backend Server 2  │ │  Backend Server 3  │
         │   (Port :8001)     │ │   (Port :8002)     │ │   (Port :8003)     │
         └──────────┬─────────┘ └──────────┬─────────┘ └──────────┬─────────┘
                    │                      │                      │
                    └──────────────────────┼──────────────────────┘
                                           │ Dual-Mode Health Check
                                           ▼
                               ┌────────────────────────┐
                               │ Active Health Checker  │
                               │  & In-Flight Monitor   │
                               └───────────┬────────────┘
                                           │
                                           ▼
                               ┌────────────────────────┐
                               │   Metrics Collector    │
                               │ (RPS, Latency, Logs)   │
                               └───────────┬────────────┘
                                           │
                                           ▼ (Port 8081)
                               ┌────────────────────────┐
                               │ Observability Dashboard│
                               │ (SSE Stream + embed.FS)│
                               └────────────────────────┘
```

---

## 📁 Repository Structure

```
NexusLB/
├── backend/
│   └── server.go             # Multi-server mock upstream cluster (:8001, :8002, :8003)
├── config/
│   └── config.go             # Configuration models, validation & JSON loader
├── journals/                 # Academic engineering challenge journals (2 per member)
│   ├── 1024030313-navjot/    # Navjot Singh (1024030313)
│   │   ├── 01-concurrent-slice-mutation.md
│   │   ├── 02-least-connections-clustering-tiebreaker.md
│   │   └── index.md
│   ├── 1024030316-shaina/    # Shaina Gera (1024030316)
│   │   ├── 01-reverse-proxy-telemetry-loss.md
│   │   ├── 02-ip-hash-ephemeral-port-affinity-skew.md
│   │   └── index.md
│   └── 1024030320-prabhgun/  # Prabhgun Kaur (1024030320)
│       ├── 01-asset-embedding-boundary-violation.md
│       ├── 02-sse-stream-goroutine-leak-timeout-conflict.md
│       └── index.md
├── MST-report/               # Formal LaTeX Mid-Semester Technical Report
│   ├── main.tex              # Complete academic report with TikZ architectural diagrams
│   ├── references.bib        # BibTeX bibliography database
│   └── tietreport.cls        # TIET report formatting class
├── pkg/
│   ├── backend/
│   │   └── backend.go        # Thread-safe Backend state & atomic connection counters
│   ├── balancer/
│   │   ├── balancer.go       # Balancer interface & Round-Robin implementation
│   │   ├── balancer_test.go  # Unit tests for round-robin routing & offline skips
│   │   ├── ip_hash.go        # 32-bit FNV-1a IP hash with sequential ring failover
│   │   ├── ip_hash_test.go   # Unit tests for session affinity & failover
│   │   ├── least_conn.go     # Least Connections with circular offset tie-breaking
│   │   ├── least_conn_test.go# Unit tests for least connections selection
│   │   ├── manager.go        # Thread-safe StrategyManager for zero-downtime swaps
│   │   └── manager_test.go   # Unit tests for strategy manager & concurrency
│   ├── dashboard/
│   │   ├── server.go         # Dashboard HTTP server, SSE telemetry & REST API
│   │   └── server_test.go    # Unit tests for dashboard API endpoints
│   ├── health/
│   │   ├── checker.go        # Background active health monitoring daemon
│   │   └── checker_test.go   # Unit tests for active health detection
│   ├── metrics/
│   │   └── metrics.go        # High-performance metrics aggregator & ring buffer
│   └── proxy/
│       └── proxy.go          # Reverse proxy pipeline & streaming response recorder
├── web/                      # Embedded dashboard frontend
│   ├── app.js                # SSE consumer, interactive controls & traffic generator
│   ├── index.html            # Dark-mode dashboard layout & control panels
│   ├── style.css             # Glassmorphic styling & responsive UI tokens
│   └── web.go                # go:embed directive exporting Assets embed.FS
├── config.json               # Runtime JSON configuration
├── go.mod                    # Go module definition
├── main.go                   # Gateway entrypoint & graceful shutdown coordinator
└── README.md                 # Complete project documentation
```

---

## ⚙️ Configuration

NexusLB is configured declaratively using [`config.json`](./config.json):

```json
{
  "proxy_port": ":8080",
  "dashboard_port": ":8081",
  "backends": [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003"
  ],
  "health_check_interval": "3s",
  "health_check_path": "/health",
  "strategy": "round_robin"
}
```

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `proxy_port` | `string` | `":8080"` | Ingress port where NexusLB accepts client HTTP requests |
| `dashboard_port` | `string` | `":8081"` | Administrative port serving the embedded dashboard and REST API |
| `backends` | `[]string` | `3 nodes` | List of upstream HTTP backend server URLs |
| `health_check_interval` | `string` | `"3s"` | Interval between proactive health check probes |
| `health_check_path` | `string` | `"/health"` | HTTP endpoint queried on upstream servers |
| `strategy` | `string` | `"round_robin"` | Initial load balancing strategy (`round_robin`, `least_connections`, `ip_hash`) |

---

## ⚖️ Load Balancing Strategies & Hot-Swapping

NexusLB implements three specialized Layer 7 routing strategies:

| Strategy | Key | Selection Logic | Best Use Case |
| :--- | :--- | :--- | :--- |
| **Round Robin** | `"round_robin"` | Atomic counter traversal across healthy nodes: $$i = (\text{atomic.AddUint64}(\&current, 1) - 1) \pmod n$$ | Homogeneous backends with uniform request processing times. |
| **Least Connections** | `"least_connections"` | Queries in-flight connections: $$b^* = \arg\min_{b \in \mathcal{B}_{\text{alive}}} \{ b.\text{ActiveConns} \}$$ Resolves ties using a circular rotating offset to prevent node starvation under fast workloads. | Heterogeneous workloads, long-lived requests (WebSockets, SSE), complex DB queries. |
| **IP Hashing** | `"ip_hash"` | 32-bit FNV-1a hash over normalized client IP: $$idx = \text{FNV1a}(\text{ExtractClientIP}(r)) \pmod n$$ Deterministic sequential ring walk if primary mapped node fails. | Stateful sessions, local caching, and sticky user affinity without external storage. |

### Zero-Downtime Hot-Swapping

The active routing strategy can be swapped at runtime with zero downtime:
- **Via Dashboard UI**: Click any of the strategy pills in the header banner (`[Round Robin]`, `[Least Connections]`, `[IP Hash]`).
- **Via REST API**:
  ```bash
  curl -X POST "http://localhost:8081/api/strategy?name=least_connections"
  ```
- **Thread Safety**: Governed by `sync.RWMutex` inside `StrategyManager`, allowing thousands of in-flight proxy requests to read strategies concurrently while atomic pointer swaps execute instantaneously.

---

## 🚀 Getting Started

### Prerequisites

- **Go**: Version 1.20 or newer installed ([Download Go](https://golang.org/dl/)).
- **Web Browser**: Chrome, Edge, Firefox, or Safari for the dashboard.
- **Terminal**: Bash, zsh, or PowerShell.

### 1. Launch Simulated Backend Cluster

Start the 3 mock backend servers in a separate terminal:

```bash
go run backend/server.go
```

This concurrently launches 3 mock upstream servers on ports `:8001`, `:8002`, and `:8003`.

### 2. Launch NexusLB Ingress Gateway

In another terminal, start NexusLB:

```bash
go run main.go
```

Or build and execute the binary:

```bash
go build -o nexuslb.exe main.go
./nexuslb.exe
```

Console output confirms startup:
```text
==================================================
    NexusLB - Reverse Proxy & Load Balancer       
==================================================
[INIT] Registered upstream backend: http://localhost:8001
[INIT] Registered upstream backend: http://localhost:8002
[INIT] Registered upstream backend: http://localhost:8003
[INIT] Active routing algorithm: Round Robin
[INIT] Health checker active (probing every 3s on /health)
[PROXY] Reverse proxy listening on http://localhost:8080
[DASHBOARD] Monitoring dashboard listening on http://localhost:8081

NexusLB is running!
Ingress Proxy:     http://localhost:8080
Admin Dashboard:   http://localhost:8081
```

---

## 🖥 Live Observability Dashboard

Open your browser and navigate to:  
👉 **[http://localhost:8081](http://localhost:8081)**

### Dashboard Features:
1. **Header Control Strip**:
   - **Active Algorithm Pill Selector**: One-click switching between Round-Robin, Least Connections, and IP Hashing.
   - **Connection Indicator**: Displays live SSE synchronization status (`CONNECTED` / `RECONNECTING`).
   - **Reset Metrics**: Flushes all rolling counters and log history.
2. **Real-Time KPIs**:
   - **Total Requests**: Total requests handled across all upstreams.
   - **Live Throughput**: Real-time requests per second (RPS) over a rolling window.
   - **Average Latency**: End-to-end proxy processing latency in milliseconds.
   - **Success Rate**: Ratio of successful requests (`2xx/3xx`) to total requests.
3. **Dynamic Traffic Distribution Share Bar**:
   - High-contrast visual bar depicting the real-time share of requests distributed to each backend.
4. **Backend Server Cards**:
   - Live health badge (`HEALTHY` vs `OFFLINE`).
   - Active in-flight connections counter (`ActiveConns`).
   - Interactive connection stepper (`-`, count badge, `+`, `Reset 0`).
   - Interactive power toggle switch (simulates immediate server shutdown).
   - Expected target share indicator (`33.3%`, `50.0%`, or `100%`).
5. **Interactive Traffic Simulator**:
   - **Quick Actions**: Send 1 Request, Burst 10 Requests, Start/Stop Continuous Traffic (2 req/s).
   - **Custom In-Flight Burst**: Dispatch $N$ parallel requests held in-flight for $D$ ms with optional diverse client IP simulation.
   - **1-Click Failover Demo**: Automated walkthrough of baseline, sudden node failure, and self-healing phases.
6. **Live Request Stream Log**:
   - Displays the last 50 requests with timestamps, HTTP method, client IP, target backend, response code, and latency.

---

## 🎛 Simulating & Testing Active Connections

To verify the **Least Connections** algorithm under real-world conditions, NexusLB provides two mechanisms to produce and inspect non-zero active connection states:

### Method A: Custom In-Flight Hold Burst (Traffic Simulator)
1. Open the dashboard at `http://localhost:8081` and select **Least Connections**.
2. In the **Traffic Simulator** panel, set:
   - **Connections**: `15`
   - **Hold Duration**: `3000 ms`
3. Click **"Dispatch In-Flight Burst"**.
4. 15 concurrent goroutines immediately open connections and hold them for 3 seconds. The backend cards instantly display active in-flight counts (e.g., `5`, `5`, `5`).
5. While the burst is held, click **"Send 1 Request"** or inject a delay on Server 1. NexusLB dynamically evaluates in-flight counts and directs new traffic away from heavily loaded instances.

### Method B: Manual Active Connection Steppers
Each backend card includes an interactive stepper to manually simulate artificial load:
- Click **`+`** to increment active connections (e.g., set Server 1 to 5 active connections).
- Click **`-`** to decrement active connections.
- Click **`0`** to reset the counter to zero.
- **Verification**: With Server 1 set to 5 active connections and Least Connections active, dispatch requests through the proxy (`http://localhost:8080/`). Observe that NexusLB automatically routes all new traffic to Servers 2 and 3 until Server 1 is no longer the busiest node.

### Method C: Via REST API
```bash
# Set Server 1 active connections directly to 10
curl -X POST "http://localhost:8081/api/backend/connections?url=http://localhost:8001&count=10"

# Adjust Server 2 active connections by +3
curl -X POST "http://localhost:8081/api/backend/connections?url=http://localhost:8002&delta=3"

# Reset Server 1 active connections to 0
curl -X POST "http://localhost:8081/api/backend/connections?url=http://localhost:8001&count=0"
```

---

## 🛡 Fault Tolerance & Load Rebalancing

NexusLB guarantees that if an upstream server goes offline or crashes, **zero client requests are dropped**.

### Verification via Dashboard:
1. Open [`http://localhost:8081`](http://localhost:8081).
2. Click **"Start Continuous Traffic"** to stream continuous requests.
3. Flip the switch on **Server 2** to **OFF**:
   - Server 2 immediately dims with `OFFLINE (BYPASSED)`.
   - Traffic share instantaneously recalculates to **50.0% Server 1** and **50.0% Server 3**.
   - Server 2 receives **0 requests** while offline.
4. Flip **Server 2** back to **ON**:
   - The proactive health checker queries `/health`, receives `200 OK`, and restores Server 2 within $\le 3$ seconds.
   - Traffic smoothly rebalances to **33.3% across all three nodes**.

### Verification via CLI:
```bash
# 1. Send requests through the proxy (Round-Robin cycles 8001 -> 8002 -> 8003)
curl http://localhost:8080/
curl http://localhost:8080/

# 2. Simulate failure on Server 2
curl -X POST "http://localhost:8081/api/backend/toggle?url=http://localhost:8002&state=down"

# 3. Send requests - Server 2 is skipped automatically with 100% success rate
curl http://localhost:8080/
curl http://localhost:8080/
```

---

## 📝 Academic Engineering Journals

As part of the UCS503P curriculum, each team member authored detailed engineering journals documenting challenging technical obstacles encountered during design, debugging, and benchmarking:

| Student | Journal | Topic | Summary |
| :--- | :--- | :--- | :--- |
| **Navjot Singh** (`1024030313`) | [Journal 01](./journals/1024030313-navjot/01-concurrent-slice-mutation.md) | Concurrent Slice Mutation Race Condition | Fixed a data race where concurrent requests mutating dynamic slice memory corrupted round-robin traversal, replaced with an atomic ring pointer. |
| **Navjot Singh** (`1024030313`) | [Journal 02](./journals/1024030313-navjot/02-least-connections-clustering-tiebreaker.md) | Least Connections Node Starvation & Tie-Breaking | Resolved a load clustering defect under fast endpoints where index 0 monopolized 99.4% of traffic; introduced rotating circular offsets for fair tie-breaking. |
| **Shaina Gera** (`1024030316`) | [Journal 01](./journals/1024030316-shaina/01-reverse-proxy-telemetry-loss.md) | Telemetry Loss with `httputil.ReverseProxy` | Solved upstream status code loss caused by standard reverse proxy streaming directly to sockets; implemented zero-allocation `statusRecorder`. |
| **Shaina Gera** (`1024030316`) | [Journal 02](./journals/1024030316-shaina/02-ip-hash-ephemeral-port-affinity-skew.md) | Ephemeral Port Skew in IP Hashing | Fixed broken sticky sessions where browser parallel TCP ports scattered client traffic; implemented `ExtractClientIP` port stripping and sequential ring walk failover. |
| **Prabhgun Kaur** (`1024030320`) | [Journal 01](./journals/1024030320-prabhgun/01-asset-embedding-boundary-violation.md) | Compile-Time Encapsulation Failure in `go:embed` | Resolved `pattern ../../web/*: cannot use '..'` compile errors by co-locating `web.go` to hermetically bundle UI assets into the binary. |
| **Prabhgun Kaur** (`1024030320`) | [Journal 02](./journals/1024030320-prabhgun/02-sse-stream-goroutine-leak-timeout-conflict.md) | SSE Goroutine Leak & `WriteTimeout` Collision | Resolved stream disconnects every 15s caused by `WriteTimeout` conflicts and eliminated goroutine leakage on tab refreshes using context cancellation. |

---

## 📡 API Reference

### NexusLB Ingress Gateway (`http://localhost:8080`)
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `ANY` | `/*` | Forwards client requests to an upstream backend using the active load balancing strategy |

### NexusLB Admin & Telemetry API (`http://localhost:8081`)
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/` | Serves the embedded dark-mode observability dashboard |
| `GET` | `/api/stats` | Returns JSON snapshot of metrics, active strategy, backend states, and trace logs |
| `GET` | `/api/stream` | Server-Sent Events (SSE) live telemetry feed (1 Hz) |
| `GET` | `/api/strategy` | Returns active strategy key, display name, and list of available algorithms |
| `POST` | `/api/strategy?name=STRAT` | Switches load balancing strategy (`round_robin`, `least_connections`, `ip_hash`) |
| `POST` | `/api/test-request?count=N&delay=MS&sim_ips=[true\|false]` | Dispatches $N$ parallel requests held in-flight for $D$ ms with optional client IP simulation |
| `POST` | `/api/backend/toggle?url=URL&state=[up\|down]` | Toggles backend health state online or offline |
| `POST` | `/api/backend/connections?url=URL&count=N&delta=N` | Directly overrides or adjusts active connections counter on target backend |
| `POST` | `/api/backend/delay?url=URL&ms=N` | Injects persistent artificial delay (latency) on target backend |
| `POST` | `/api/stats/reset` | Resets all metric counters and request trace logs |

### Mock Upstream Cluster (`http://localhost:8001`, `8002`, `8003`)
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/?delay=MS` | Returns server identity and port number with optional processing delay |
| `GET` | `/health` | Returns `200 OK` (healthy) or `503 Service Unavailable` (simulated down) |
| `GET/POST` | `/chaos/toggle?state=[up\|down]` | Toggles failure simulation state for that node |
| `GET/POST` | `/chaos/delay?ms=MS` | Sets persistent artificial delay for that node |

---

## 🧪 Testing & Verification

Automated test suites validate all routing strategies, concurrency safety, active health checking, and API endpoints.

### Run All Unit Tests
```bash
go test -v -count=1 ./...
```

### Run Concurrency Race Detector
```bash
go test -race ./...
```

### Test Suite Highlights:
- `pkg/balancer/balancer_test.go`: Validates Round-Robin circular sequencing, skips dead nodes, and returns `ErrNoHealthyBackends` when all nodes are offline.
- `pkg/balancer/least_conn_test.go`: Verifies routing toward nodes with lowest active connections and validates fair circular tie-breaking.
- `pkg/balancer/ip_hash_test.go`: Confirms deterministic sticky session affinity, ephemeral port stripping, and sequential ring walk failover.
- `pkg/balancer/manager_test.go`: Tests concurrency safety during zero-downtime hot-swapping under simulated high-throughput traffic.
- `pkg/health/checker_test.go`: Uses mock HTTP test servers to verify active health transitions and failure detection.
- `pkg/dashboard/server_test.go`: Tests SSE streaming, strategy switching, connection steppers, and metric reset endpoints.

---

## 💡 Engineering Highlights & Concurrency

1. **Non-Blocking Atomic Indexing**:
   - Rather than allocating or filtering slices on every incoming request (which causes heap pressure and race conditions), NexusLB preserves an invariant ring of backend pointers. An atomic counter (`atomic.AddUint64`) selects candidates in $O(1)$ time with zero locks in the critical path.
2. **Zero-Allocation Status Recorder**:
   - Go's `httputil.ReverseProxy` writes status codes directly to the underlying socket. NexusLB wraps `http.ResponseWriter` in a specialized `statusRecorder` that intercepts `WriteHeader` calls without buffering the response body in memory, preserving zero-allocation streaming.
3. **Fair Circular Tie-Breaking**:
   - Mitigates the "first-minimum trap" in Least Connections routing by advancing an atomic offset on every incoming request, ensuring equitable distribution during low-latency operations while preserving load-shedding during latency spikes.
4. **Hermetic Single-Binary Embedding**:
   - Web assets are bundled directly into the executable using `embed.FS` declared in a co-located [`web/web.go`](./web/web.go) package, avoiding relative path traversal restrictions while enabling standalone single-binary portability.
5. **Context-Aware Streaming**:
   - The SSE telemetry engine listens to `r.Context().Done()` with multiplexed `select` blocks and deferred ticker teardowns, preventing goroutine leaks and eliminating `WriteTimeout` disconnects.
6. **Graceful Shutdown Coordinator**:
   - Listens for `os.Interrupt` (`Ctrl+C`) and `SIGTERM`, stopping ingress traffic, draining in-flight connections with a 5-second timeout context, and cleanly releasing all network listeners.
