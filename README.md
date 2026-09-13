# NexusLB: High-Availability Reverse Proxy & Intelligent Load Balancer

[![Go Version](https://img.shields.io/badge/Go-1.27%2B-00ADD8?style=flat&logo=go)](https://golang.org/)
[![Status](https://img.shields.io/badge/Status-Active%20MVP-success)](#)
[![Tests](https://img.shields.io/badge/Tests-Passing-brightgreen)](#testing--verification)
[![License](https://img.shields.io/badge/License-Academic-blue)](#)

> **Course Project for UCS503P**  
> **Thapar Institute of Engineering and Technology (TIET)**  
> **Instructor:** Jhonsy Bansal  
> **Authors:** Navjot Singh (`1024030313`), Shaina Gera (`1024030316`), Prabhgun Kaur (`1024030320`)

---

## 📖 Table of Contents

- [Overview](#-overview)
- [Key Features](#-key-features)
- [System Architecture](#-system-architecture)
- [Repository Structure](#-repository-structure)
- [Configuration](#-configuration)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [1. Launch Simulated Backend Cluster](#1-launch-simulated-backend-cluster)
  - [2. Launch NexusLB Proxy & Dashboard](#2-launch-nexuslb-proxy--dashboard)
- [Live Observability Dashboard](#-live-observability-dashboard)
- [Fault Tolerance & Load Rebalancing](#-fault-tolerance--load-rebalancing)
- [API Reference](#-api-reference)
- [Testing & Verification](#-testing--verification)
- [Engineering Highlights & Concurrency](#-engineering-highlights--concurrency)

---

## 🌟 Overview

**NexusLB** is a custom, health-aware **Reverse Proxy and Application Load Balancer** built from scratch in Go. 

In distributed systems, simply running multiple backend servers does not solve the challenge of managing incoming traffic reliably. Unmonitored backend crashes, traffic hotspots, and lack of real-time operational visibility often cause client-facing outages. NexusLB bridges this gap by acting as an intelligent reverse proxy gateway that:

1. **Evenly balances incoming requests** across active upstream nodes.
2. **Monitors backend health actively and reactively**, instantly isolating failed nodes.
3. **Redistributes 100% of traffic** to surviving healthy nodes with zero dropped requests.
4. **Presents real-time telemetry** through an embedded dark-mode observability dashboard with interactive failure simulation.

---

## ✨ Key Features

- ⚖️ **Health-Aware Round-Robin Balancing**: Thread-safe circular pointer routing that transparently skips unhealthy or dead nodes without mutating slice memory.
- 🩺 **Dual-Mode Health Detection**:
  - **Proactive (Active Polling)**: Background goroutines ping each backend's `/health` endpoint periodically (default: every 3s).
  - **Reactive (In-Flight Interception)**: A custom `httputil.ReverseProxy.ErrorHandler` catches socket drops mid-transmission and isolates the target immediately.
- 🔄 **Self-Healing & Auto-Reintegration**: Automatically detects when a recovered server returns `200 OK` and smoothly restores it into rotation.
- 📊 **Real-Time Observability Dashboard**:
  - Embedded into the single executable binary via Go's `embed.FS` (no external runtime assets required).
  - Pushes live telemetry via Server-Sent Events (SSE) every 1s.
  - Live KPIs: Total Requests, Live Throughput (RPS), Average Latency, and Success Rate.
  - Interactive Traffic Distribution Share Bar and real-time request trace log table.
- ⚡ **Interactive Fault Injection**:
  - Individual **ON / OFF power slider switches** on every backend card to simulate node failures.
  - **Continuous Traffic Stream Generator** to observe live dynamic rebalancing while flipping switches.
  - **1-Click Automated Failover Demo** walking through baseline, failure, and self-healing phases.

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
                               │  (Header Enrichment)   │
                               └───────────┬────────────┘
                                           │
                                           ▼
                               ┌────────────────────────┐
                               │  Round-Robin Balancer  │
                               │  (Atomic Ring Router)  │
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
                                           │ Proactive Health Checks (every 3s)
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
                               │ (SSE + Web UI Embed)   │
                               └────────────────────────┘
```

---

## 📁 Repository Structure

```
NexusLB/
├── backend/
│   └── server.go             # Multi-server mock upstream cluster (:8001, :8002, :8003)
├── config/
│   └── config.go             # Configuration data models and JSON loader
├── journals/                 # Academic engineering issue journals
│   ├── 1024030313-navjot/    # Navjot Singh: Concurrency & slice race conditions
│   ├── 1024030316-shaina/    # Shaina Gera: Upstream status telemetry capture
│   └── 1024030320-prabhgun/  # Prabhgun Kaur: Compile-time go:embed package boundaries
├── pkg/
│   ├── backend/
│   │   └── backend.go        # Thread-safe Backend state & atomic counters
│   ├── balancer/
│   │   ├── balancer.go       # Pluggable Balancer interface & Round-Robin algorithm
│   │   ├── balancer_test.go  # Unit tests for routing and health skips
│   │   ├── ip_hash.go        # Deterministic 32-bit FNV-1a IP hash routing with ring failover
│   │   ├── ip_hash_test.go   # Unit tests for sticky sessions & failover
│   │   ├── least_conn.go     # Least Connections balancing with atomic counters & fair tie-breaking
│   │   ├── least_conn_test.go# Unit tests for least connections selection
│   │   ├── manager.go        # Thread-safe StrategyManager for zero-downtime hot-swapping
│   │   └── manager_test.go   # Concurrency and hot-swap unit tests
│   ├── dashboard/
│   │   ├── server.go         # Dashboard HTTP server (SSE telemetry, strategy switching & REST API)
│   │   └── server_test.go    # Unit tests for dashboard API endpoints
│   ├── health/
│   │   ├── checker.go        # Background active health monitoring daemon
│   │   └── checker_test.go   # Unit tests for active health detection
│   ├── metrics/
│   │   └── metrics.go        # High-performance metrics aggregator & ring buffer
│   └── proxy/
│       └── proxy.go          # Reverse proxy pipeline & response recorder
├── project-proposal/         # LaTeX formal project proposal specification
├── web/                      # Frontend dashboard assets (embedded in binary)
│   ├── app.js                # SSE stream consumer, dynamic UI updates & traffic generator
│   ├── index.html            # Dark-mode dashboard layout
│   ├── style.css             # Glassmorphism aesthetic & toggle switch styles
│   └── web.go                # go:embed directive exporting Assets embed.FS
├── config.json               # Default runtime configuration
├── go.mod                    # Go module definition
├── main.go                   # Application entrypoint & graceful shutdown coordinator
└── README.md                 # Project documentation
```

---

## ⚙️ Configuration

NexusLB is configured using [`config.json`](./config.json):

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
| `proxy_port` | `string` | `":8080"` | TCP port where NexusLB accepts client traffic |
| `dashboard_port` | `string` | `":8081"` | TCP port hosting the web UI and metrics API |
| `backends` | `[]string` | `3 nodes` | Array of upstream backend URLs |
| `health_check_interval` | `string` | `"3s"` | Interval between active health probes |
| `health_check_path` | `string` | `"/health"`| Endpoint path to probe on upstream servers |
| `strategy` | `string` | `"round_robin"` | Initial load balancing routing algorithm (`round_robin`, `least_connections`, `ip_hash`) |

### Supported Load Balancing Strategies

NexusLB ships with three production-grade routing strategies managed by a thread-safe `StrategyManager`:

| Strategy | Config Key | Description | Best For |
| :--- | :--- | :--- | :--- |
| **Round Robin** | `"round_robin"` | Distributes incoming requests sequentially in a circular ring using atomic pointers. Transparently skips unhealthy nodes. | Homogeneous upstream instances with uniform response latencies. |
| **Least Connections** | `"least_connections"` | Dynamically queries in-flight connection counts (`ActiveConnections`) and routes to the least busy healthy backend. Employs circular tie-breaking to avoid clustering. | Long-lived connections (WebSockets, SSE), complex DB queries, or servers with asymmetric processing power. |
| **IP Hashing** | `"ip_hash"` | Computes a deterministic 32-bit FNV-1a hash over the client's IP address (`X-Forwarded-For`, `X-Real-IP`, or `RemoteAddr`). Features sequential ring walk failover if primary node fails. | Stateful sessions, localized caching, and sticky user affinity without requiring centralized session stores. |

#### Zero-Downtime Hot-Swapping:
- **Web UI**: Click any of the strategy pills in the top action banner (`[Round Robin]`, `[Least Connections]`, `[IP Hash]`).
- **REST API**: Send `POST /api/strategy?name=<strategy_key>`.
- **Thread Safety**: Backed by `sync.RWMutex`, allowing ongoing proxy requests to read strategies concurrently while atomic pointer swaps execute instantaneously.

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

### 2. Launch NexusLB Proxy & Dashboard

In another terminal, start the load balancer:

```bash
go run main.go
```

Or compile and run the standalone executable:

```bash
go build -o nexuslb.exe main.go
./nexuslb.exe
```

NexusLB will output:
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

### Features in the Dashboard:
1. **Live KPIs**:
   - **Total Requests**: Total proxied requests handled since launch.
   - **Throughput**: Real-time requests per second (RPS) calculated over a rolling window.
   - **Avg Latency**: Average round-trip execution latency in milliseconds.
   - **Success Rate**: Ratio of successful requests (`2xx/3xx`) to total requests.
2. **Traffic Distribution Bar**:
   - Visual colored breakdown showing the exact percentage of requests handled by each node.
3. **Backend Node Cards**:
   - Shows live server status (`HEALTHY` vs `OFFLINE`), active in-flight connections, total requests handled, and last latency.
   - **Interactive Power Switch**: Flip any server ON or OFF on-the-fly.
   - **Dynamic Target Share Badge**: Shows the mathematically expected target share (`33.3%`, `50.0%`, or `100%`).
4. **Live Request Stream**:
   - Streaming log of the last 50 requests with timestamps, HTTP method, client IP, targeted backend, HTTP status tag, and execution latency.
5. **Interactive Traffic Simulators**:
   - **Send 1 Request**: Dispatches a single request to the proxy.
   - **Burst 10 Requests**: Dispatches 10 requests to observe Round-Robin cycling.
   - **Start Continuous Traffic**: Streams 2 requests/second indefinitely so you can flip server switches and watch the distribution adapt live.

---

## 🛡 Fault Tolerance & Load Rebalancing

NexusLB guarantees that if an upstream server goes offline or crashes, **no client requests are dropped**.

### Verification via Dashboard:
1. Open [`http://localhost:8081`](http://localhost:8081).
2. Click **"Start Continuous Traffic"** to begin a steady stream of requests.
3. Flip the switch on **Server 2** to **OFF**:
   - Server 2 immediately dims with an `OFFLINE (BYPASSED)` status.
   - The traffic distribution bar immediately recalculates to **50.0% Server 1** and **50.0% Server 3**.
   - Server 2 receives **0 requests** while offline.
4. Flip **Server 2** back to **ON**:
   - NexusLB's health checker probes `/health`, receives `200 OK`, and restores Server 2.
   - Traffic smoothly rebalances to **33.3% across all three nodes**.

### Verification via CLI:
```bash
# 1. Send requests through the proxy
curl http://localhost:8080/
# Output: Response served from Backend Server 1 (port :8001)

curl http://localhost:8080/
# Output: Response served from Backend Server 2 (port :8002)

# 2. Simulate failure on Server 2
curl -X POST "http://localhost:8081/api/backend/toggle?url=http://localhost:8002&state=down"

# 3. Send requests - Server 2 is skipped automatically!
curl http://localhost:8080/
# Output: Response served from Backend Server 3 (port :8003)

curl http://localhost:8080/
# Output: Response served from Backend Server 1 (port :8001)
```

---

## 📡 API Reference

### NexusLB Proxy (`http://localhost:8080`)
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `ANY` | `/*` | Forwards client request to a healthy backend server using the active strategy |

### NexusLB Admin & Metrics API (`http://localhost:8081`)
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/` | Serves the embedded observability dashboard UI |
| `GET` | `/api/stats` | Returns JSON snapshot of current metrics, backend states, logs, and active strategy |
| `GET` | `/api/stream` | Server-Sent Events (SSE) stream pushing metrics every 1 second |
| `GET` | `/api/strategy` | Returns active strategy key, formal name, and list of available algorithms |
| `POST` | `/api/strategy?name=STRAT` | Dynamically switches routing strategy (`round_robin`, `least_connections`, `ip_hash`) |
| `POST` | `/api/test-request?count=N&sim_ips=[true\|false]` | Fires `N` requests through proxy with optional diverse client IP simulation |
| `POST` | `/api/backend/toggle?url=URL&state=[up\|down]` | Toggles a backend server online or offline |
| `POST` | `/api/backend/delay?url=URL&ms=N` | Injects simulated artificial latency (e.g. `300ms`) on target backend |
| `POST` | `/api/stats/reset` | Resets all counters and log buffers to zero |

### Mock Upstream Cluster (`http://localhost:8001`, `8002`, `8003`)
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/?delay=MS` | Returns server identity string and port number with optional per-request delay |
| `GET` | `/health` | Returns `200 OK` (healthy) or `503 Unavailable` (simulated down) |
| `GET/POST`| `/chaos/toggle?state=[up\|down]` | Toggles failure simulation state for that node |
| `GET/POST`| `/chaos/delay?ms=MS` | Sets persistent simulated processing latency for that node |

---

## 🧪 Testing & Verification

Automated unit tests cover Round-Robin distribution, health status transitions, and failover edge cases.

### Run All Unit Tests
```bash
go test -v -count=1 ./...
```

### Run Tests with Race Detector
```bash
go test -race ./...
```

### Test Coverage Highlights:
- `TestRoundRobin`: Validates sequential clockwise traversal, validates that dead nodes are skipped, and confirms `ErrNoHealthyBackends` error when all nodes fail.
- `TestHealthChecker`: Uses mock HTTP test servers to verify active health detection and state transitions.

---

## 💡 Engineering Highlights & Concurrency

1. **Non-Blocking Atomic Round-Robin**:
   - Rather than filtering or allocating slices per request (which causes heap pressure and race conditions), NexusLB preserves an invariant ring of backend pointers. An atomic counter (`atomic.AddUint64`) selects candidates in $O(1)$ time with zero locks in the fast path.
2. **Streaming Response Recorder**:
   - Go's `httputil.ReverseProxy` writes status codes directly to the underlying socket. NexusLB wraps `http.ResponseWriter` in a specialized `responseRecorder` that intercepts `WriteHeader` calls without buffering the response body in memory, preserving zero-allocation streaming.
3. **Hermetic Single-Binary Embed**:
   - Web assets are bundled directly into the executable using `embed.FS` declared in a co-located [`web/web.go`](./web/web.go) package, avoiding relative path traversal restrictions while enabling single-binary portability.
4. **Graceful Shutdown Coordinator**:
   - Listens for `os.Interrupt` (`Ctrl+C`) and `SIGTERM`, stopping ingress traffic, draining in-flight connections with a 5-second timeout context, and cleanly closing all listeners.
