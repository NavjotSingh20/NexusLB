# NexusLB: High-Availability Reverse Proxy & Intelligent Load Balancer

[![Go Version](https://img.shields.io/badge/Go-1.27%2B-00ADD8?style=flat&logo=go)](https://golang.org/)
[![Status](https://img.shields.io/badge/Status-Active-success)](#)
[![Tests](https://img.shields.io/badge/Tests-Passing-brightgreen)](#testing--verification)
[![License](https://img.shields.io/badge/License-MIT-blue)](./LICENSE)

> **Course Project for UCS503P (Software Engineering)**  
> **Thapar Institute of Engineering and Technology (TIET), Patiala**  
> **Instructor:** Jhonsy Bansal  
> **Authors (Group 3C23):**  
> - Navjot Singh (`1024030313`)  
> - Shaina Gera (`1024030316`)  
> - Prabhgun Kaur (`1024030320`)  

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [System Architecture](#system-architecture)
- [Repository Structure](#repository-structure)
- [Configuration & CLI Flags](#configuration--cli-flags)
- [Load Balancing Strategies & Smart Auto](#load-balancing-strategies--smart-auto)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [1. Launch Simulated Backend Cluster](#1-launch-simulated-backend-cluster)
  - [2. Launch NexusLB Ingress Gateway](#2-launch-nexuslb-ingress-gateway)
- [Real-Life Traffic & Chaos Simulator](#real-life-traffic--chaos-simulator)
- [Live Observability Dashboard](#live-observability-dashboard)
- [Simulating & Testing Active Connections](#simulating--testing-active-connections)
- [Fault Tolerance & Load Rebalancing](#fault-tolerance--load-rebalancing)
- [Academic Engineering Journals](#academic-engineering-journals)
- [API Reference](#api-reference)
- [Testing & Verification](#testing--verification)
- [Engineering Highlights & Concurrency](#engineering-highlights--concurrency)

---

## Overview

**NexusLB** is a health-aware **Layer 7 Reverse Proxy and Intelligent Application Load Balancer** engineered from scratch in Go with zero external runtime framework dependencies.

In distributed architectures, simply spreading requests across static ports leaves systems vulnerable to silent upstream blackholes, connection starvation, unequal node capacities, botnet floods, and zero operational visibility. NexusLB solves this as a non-blocking HTTP gateway that combines:

1. **Five Routing Modes Including Smart Auto**: Zero-downtime hot-swapping across **Smart Auto (`adaptive`)**, **Round Robin**, **Smooth Weighted Round Robin**, **Least Connections**, and **Deterministic IP Hash**, complete with live explainability for every automated delegation decision.
2. **Defense-in-Depth Resilience**: Per-IP token-bucket **Edge Shield** rate limiting (`HTTP 429`), three-state per-backend **Circuit Breakers** (`Closed` / `Open` / `Half-Open`), proactive `/health` polling, and staged `retryRecorder` transparent failover across upstreams.
3. **Layer 7 Canary Routing**: Header, query parameter, and cookie-based canary steering with automatic fallback if canary instances go offline.
4. **Autonomous Real-Life Traffic & Chaos Simulator (`pkg/simulator`)**: Built-in background traffic generator producing diverse random public IPs, sticky sessions, slow queries, canary traffic, single-IP/botnet DDoS floods, and reversible cluster chaos drills with automatic baseline restoration.
5. **Embedded `data-terminal` Dashboard (`embed.FS` + SSE)**: Four-view (`Overview`, `Pool`, `Lab`, `Trace`) live telemetry console streamed at 1 Hz over Server-Sent Events with Chart.js sliding telemetry, per-node controls, and request ID tracing.

---

## Key Features

- **Pluggable 5-Strategy Routing Engine**:
  - **Smart Auto (`adaptive`)**: Autonomous meta-balancer that inspects live connection skew, upstream response latency skew, heterogeneous weights, and client IP locality every 250 ms, delegating with a 4-second anti-flap hysteresis cooldown (immediate bypass on load/capacity escalations) and emitting live explainability snapshots.
  - **Round Robin (`round_robin`)**: Monotonically advancing 64-bit atomic ring counter with zero allocations on the fast path.
  - **Smooth Weighted Round Robin (`weighted_round_robin`)**: NGINX SWRR capacity-weighted interleaving supporting dynamic 1x-100x backend weights without burst clustering.
  - **Least Connections (`least_connections`)**: Routes to the healthy upstream with minimum in-flight connections and uses an atomic rotating circular offset for fair sub-millisecond tie-breaking.
  - **IP Hash (`ip_hash`)**: 32-bit FNV-1a hashing over normalized client IPs (`X-Forwarded-For`, `X-Real-IP`, ephemeral port stripping) with sequential ring-walk failover.
  - **Zero-Downtime Hot-Swapping**: Switch algorithms live from the top strategy strip or `POST /api/strategy?name=...` protected by `sync.RWMutex`.
- **Layer 7 Canary Deployments & Traffic Splitting**:
  - Targets canary upstreams via `X-Canary: true`, `?canary=true`, or `canary=true` cookie, falling back transparently to stable upstreams when no healthy canary node is available.
  - Supports instant weighted canary rollouts (e.g., 90/10 split under Smooth Weighted Round Robin).
- **Edge Security & Anti-DDoS Rate Limiting**:
  - `sync.Map`-backed per-IP token bucket rate limiter with background stale-bucket eviction, rejecting floods with `HTTP 429 Too Many Requests` (`Retry-After`, `X-RateLimit-Limit`, `X-RateLimit-Remaining`).
  - Live runtime rate/burst tuning (`POST /api/shield/config`) and enable/bypass toggle without dropping legitimate isolated callers.
- **Fault Tolerance, Circuit Breaker & Transparent Retries**:
  - Three-state circuit breaker (`Closed` -> `Open` after configurable consecutive failures -> `Half-Open` probe after cooldown) isolates failing upstreams faster than passive health intervals alone.
  - Buffered `retryRecorder` intercepts upstream socket errors before committing headers to clients and retries alternate healthy backends (honoring `max_retries`, `max_retry_body_bytes`, `retry_non_idempotent`, and `Idempotency-Key` headers).
- **Autonomous Real-Life Traffic & Cluster Chaos Simulator**:
  - Streams Poisson-jittered multi-IP organic workloads, returning sticky caller sessions, canary requests, and slow queries alongside periodic or on-demand `single_ip_dos` and `botnet_ddos` attacks.
  - Executes reversible chaos drills (`latency_spike`, `canary_weighted_rollout`, `node_outage_failover`) and automatically restores pre-wave delays, weights, canary flags, node health, and touched circuit breakers when a wave finishes or stops.
- **Embedded Multi-View Observability Dashboard**:
  - Bundled into a single Go binary via `embed.FS` (`web/web.go`) with zero external asset runtime dependencies.
  - Four focused navigation views (`Overview`, `Pool`, `Lab`, `Trace`), 6-cell KPI bar (including p50/p95/p99 tail latency, in-flight queue depth, and blocked `429` count), 24-second sliding Chart.js area charts, interactive per-backend latency/weight/socket/canary controls, and searchable 50-entry request trace logs with click-to-copy `X-Request-ID`.

---

## System Architecture

```text
                       +------------------------------+
                       |   Clients / pkg/simulator    |
                       +--------------+---------------+
                                      | :8080 (HTTP Ingress)
                                      v
                       +------------------------------+
                       | Edge Shield (ratelimit)      |
                       | Per-IP Token Bucket -> 429   |
                       +--------------+---------------+
                                      |
                                      v
                       +------------------------------+
                       | Reverse Proxy Pipeline       |
                       | Canary Steering + Retries    |
                       +--------------+---------------+
                                      |
                                      v
                       +------------------------------+
                       | StrategyManager              |
                       | Smart Auto | RR | WRR | LC   |
                       | IP Hash    | Live Explain     |
                       +--------------+---------------+
                                      |
            +-------------------------+-------------------------+
            |                         |                         |
            v                         v                         v
   +-----------------+       +-----------------+       +-----------------+
   | Backend :8001   |       | Backend :8002   |       | Backend :8003   |
   | CB: Closed/Open |       | CB: Closed/Open |       | CB: Closed/Open |
   +--------+--------+       +--------+--------+       +--------+--------+
            |                         |                         |
            +-------------------------+-------------------------+
                                      |
            +-------------------------+-------------------------+
            |                                                   |
            v                                                   v
   +--------------------+                             +--------------------+
   | Active HealthCheck |                             | Metrics Collector  |
   | /health @ interval |                             | RPS, p50/p95/p99   |
   +--------------------+                             +---------+----------+
                                                                | :8081
                                                                v
                                                      +--------------------+
                                                      | Dashboard + API    |
                                                      | 1 Hz SSE + embedFS |
                                                      +--------------------+
```

---

## Repository Structure

```text
NexusLB/
├── backend/
│   └── server.go                  # Multi-server mock upstream cluster (:8001, :8002, :8003) with /chaos/* hooks
├── config/
│   ├── config.go                  # Configuration structs, defaults, validation & JSON loader
│   └── config_test.go             # Config loading & validation unit tests
├── journals/                      # Academic engineering journals (Groups/Authors 1024030313, 1024030316, 1024030320)
├── MST-report/                    # LaTeX Mid-Semester Technical Report & compiled PDF
├── project-proposal/              # Project proposal LaTeX source & PDF
├── pkg/
│   ├── backend/
│   │   ├── backend.go             # Thread-safe Backend state, simulated lag tracking & atomic counters
│   │   ├── backend_test.go        # Backend unit tests
│   │   └── circuit_breaker.go     # Closed/Open/Half-Open circuit breaker state machine
│   ├── balancer/
│   │   ├── adaptive.go            # Smart Auto meta-balancer, signal evaluator & explainability history
│   │   ├── adaptive_test.go       # Smart Auto strategy & hysteresis tests
│   │   ├── balancer.go            # Balancer interface & Round Robin
│   │   ├── balancer_test.go       # Round Robin tests
│   │   ├── canary_test.go         # Layer 7 canary steering & fallback tests
│   │   ├── ip_hash.go             # 32-bit FNV-1a IP Hash with normalized IP & failover walk
│   │   ├── ip_hash_test.go        # IP Hash tests
│   │   ├── least_conn.go          # Least Connections with circular rotating tie-breaker
│   │   ├── least_conn_test.go     # Least Connections tests
│   │   ├── manager.go             # StrategyManager hot-swapping & canary precedence
│   │   ├── manager_test.go        # Concurrency & hot-swap tests
│   │   ├── weighted_rr.go         # Smooth Weighted Round Robin (NGINX SWRR)
│   │   └── weighted_rr_test.go    # Weighted RR tests
│   ├── dashboard/
│   │   ├── server.go              # Admin HTTP server, 1 Hz SSE telemetry, chaos, shield & /api/sim/* endpoints
│   │   └── server_test.go         # Dashboard API & simulator endpoint tests
│   ├── health/
│   │   ├── checker.go             # Periodic active /health polling daemon
│   │   └── checker_test.go        # Active health transition tests
│   ├── metrics/
│   │   ├── metrics.go             # Thread-safe rolling throughput, p50/p95/p99 latency & 50-entry trace ring
│   │   └── metrics_test.go        # Metrics collector tests
│   ├── proxy/
│   │   ├── proxy.go               # Reverse proxy, canary detection, Edge Shield check & staged retryRecorder
│   │   └── proxy_test.go          # Retry, body limit, idempotency & shield proxy tests
│   ├── ratelimit/
│   │   ├── limiter.go             # Per-IP token-bucket rate limiter with background stale bucket eviction
│   │   └── limiter_test.go        # Rate limiter & concurrent eviction tests
│   └── simulator/
│       ├── simulator.go           # Real-life traffic & reversible chaos wave engine + worker pool
│       └── simulator_test.go      # IP diversity, DDoS 429, reversible chaos & default hook tests
├── web/
│   ├── app.js                     # SSE client, Smart Auto explainability UI, simulator studio & pool actions
│   ├── charts.js                  # Chart.js 24s sliding telemetry canvas renderer
│   ├── index.html                 # 4-view data-terminal console (Overview, Pool, Lab, Trace)
│   ├── style.css                  # Dark terminal palette (#171719/#131217) & responsive breakpoints
│   └── web.go                     # go:embed directive bundling frontend assets into binary
├── config.json                    # Runtime JSON configuration
├── go.mod                         # Go module definition (Go 1.27.1, stdlib only)
├── main.go                        # Ingress gateway entrypoint, CLI flags & graceful shutdown
└── README.md                      # Project documentation
```

---

## Configuration & CLI Flags

NexusLB loads settings from [`config.json`](./config.json) (falling back to [`config.DefaultConfig()`](./config/config.go) when absent):

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
  "strategy": "round_robin",
  "max_retries": 2,
  "max_retry_body_bytes": 1048576,
  "retry_non_idempotent": false,
  "rate_limit_enabled": true,
  "rate_limit_rate": 10.0,
  "rate_limit_burst": 10,
  "circuit_breaker_max_failures": 5,
  "circuit_breaker_cooldown": "10s"
}
```

### JSON Configuration Fields

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `proxy_port` | `string` | `":8080"` | Ingress port accepting proxied client traffic |
| `dashboard_port` | `string` | `":8081"` | Administrative dashboard and REST/SSE API port |
| `backends` | `[]string` | `3 local nodes` | Upstream HTTP backend URLs |
| `health_check_interval` | `string` | `"3s"` | Proactive background `/health` probe cadence |
| `health_check_path` | `string` | `"/health"` | Health endpoint path queried per upstream |
| `strategy` | `string` | `"round_robin"` | Initial algorithm (`adaptive`, `round_robin`, `weighted_round_robin`, `least_connections`, `ip_hash`) |
| `max_retries` | `int` | `2` | Maximum upstream failover retries per failed attempt |
| `max_retry_body_bytes` | `int64` | `1048576` | Max request body size (bytes) buffered for replay |
| `retry_non_idempotent` | `bool` | `false` | Whether `POST`/non-idempotent requests retry without `Idempotency-Key` |
| `rate_limit_enabled` | `bool` | `true` | Enables Edge Shield per-IP token bucket limiting |
| `rate_limit_rate` | `float64` | `10.0` | Token replenishment rate per client IP (req/s) |
| `rate_limit_burst` | `int` | `10` | Token bucket burst ceiling per client IP |
| `circuit_breaker_max_failures` | `int64` | `5` | Consecutive failures before tripping backend circuit breaker to `Open` |
| `circuit_breaker_cooldown` | `string` | `"10s"` | Cooldown before an `Open` circuit enters `Half-Open` probe state |
| `sim_enabled` | `bool` | `false` | Optional auto-start for the background traffic simulator on launch |
| `sim_intensity` | `string` | `"normal"` | Simulator throughput profile (`"low"`, `"normal"`, `"heavy"`) |
| `sim_chaos` | `bool` | `true` | Allow reversible cluster chaos drills (`latency_spike`, `canary_weighted_rollout`, `node_outage_failover`) |
| `sim_ddos` | `bool` | `true` | Allow periodic DDoS attack waves (`single_ip_dos`, `botnet_ddos`) |

### CLI Startup Flags (`main.go`)

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-config` | `"config.json"` | Path to JSON configuration file |
| `-simulate` | `false` | Auto-start the real-life traffic & chaos simulator on boot |
| `-sim-intensity` | `""` | Override simulation intensity (`low`, `normal`, `heavy`) |
| `-sim-chaos` | `""` | Override reversible chaos drills (`true` / `false`) |
| `-sim-ddos` | `""` | Override periodic DDoS attack waves (`true` / `false`) |

---

## Load Balancing Strategies & Smart Auto

NexusLB supports **five** runtime routing strategies selectable from the UI or `POST /api/strategy?name=...`:

| Strategy | Key | Selection Logic | Best Use Case |
| :--- | :--- | :--- | :--- |
| **Smart Auto** | `"adaptive"` | Live telemetry meta-balancer (`AdaptiveBalancer`) switching across all 4 core algorithms | Mixed or unpredictable real-world traffic with shifting bursts, weights, or upstream lag |
| **Round Robin** | `"round_robin"` | Lock-free atomic ring counter across healthy backends | Homogeneous backends and balanced traffic |
| **Weighted RR** | `"weighted_round_robin"` | Smooth Weighted Round Robin (NGINX SWRR) proportional interleaving | Asymmetric compute capacities or 90/10 canary splits |
| **Least Connections** | `"least_connections"` | Minimum `ActiveConnections` with atomic rotating circular tie-breaker | Long-lived requests or skewed upstream response times |
| **IP Hash** | `"ip_hash"` | 32-bit FNV-1a over normalized client IP with deterministic ring failover | Repeat clients needing stateful upstream affinity |

### Smart Auto Priority Engine & Explainability

When **Smart Auto (`adaptive`)** is active, `AdaptiveBalancer` evaluates four ordered priority rules every 250 ms across live cluster state and exposes the exact reason string, active rule badge, priority signal rows, and 12-entry transition timeline to the dashboard:

1. **P1 Least Connections (`connection_skew` / `latency_skew`)**: Selected when healthy active socket spread reaches `>= 2`, or when slowest healthy upstream latency reaches `>= 80 ms` with `>= 2.5x` skew over the fastest node.
2. **P2 Weighted Round Robin (`capacity_weights`)**: Selected when healthy nodes have unequal weights (e.g., `5:4:1` or `9:1`).
3. **P3 IP Hash (`client_affinity`)**: Selected across a 24-request sliding client window (min `6` samples) when repeat client IP ratio reaches `>= 70%`.
4. **P4 Round Robin (`uniform_baseline`)**: Default O(1) distribution when the pool is healthy and uniform.

Transitions honor a `4.0s` anti-flap stability cooldown, while immediate escalations to **P1 Least Connections** or **P2 Weighted RR** bypass cooldown to react without lag.

---

## Getting Started

### Prerequisites

- **Go**: Version 1.27+ recommended (`go.mod` targets `go 1.27.1`; stdlib-only dependencies).
- **Web Browser**: Modern Chromium, Firefox, Edge, or Safari for the dashboard.

### 1. Launch Simulated Backend Cluster

Start the three mock upstream servers (`:8001`, `:8002`, `:8003`) in terminal 1:

```bash
go run ./backend/server.go
```

### 2. Launch NexusLB Ingress Gateway

Start NexusLB in terminal 2 (starts with the simulator paused by default):

```bash
go run .
```

Or auto-start the real-life simulator immediately at heavy intensity:

```bash
go run . -simulate -sim-intensity heavy
```

Or compile a standalone binary:

```bash
go build -o nexuslb.exe .
./nexuslb.exe
```

Console startup output:

```text
==================================================
    NexusLB - Reverse Proxy & Load Balancer
==================================================
[INIT] Registered upstream backend: http://localhost:8001 (CB threshold=5, cooldown=10s)
[INIT] Registered upstream backend: http://localhost:8002 (CB threshold=5, cooldown=10s)
[INIT] Registered upstream backend: http://localhost:8003 (CB threshold=5, cooldown=10s)
[INIT] Active routing algorithm: Round Robin
[INIT] Edge Shield rate limiter active=true (10.0 req/s, burst capacity 10)
[INIT] Health checker active (probing every 3s on /health)
[INIT] Real-life traffic simulator standby (start via Dashboard or -simulate flag)

NexusLB is running!
Ingress Proxy:     http://localhost:8080
Admin Dashboard:   http://localhost:8081
```

---

## Real-Life Traffic & Chaos Simulator

The built-in simulator (`pkg/simulator`) runs inside NexusLB against the real `:8080` ingress path using a bounded 64-worker pool and two coordinated loops:

- **`trafficLoop`**: Generates Poisson-jittered client workloads across intensity profiles:
  - `low`: ~6 req/s organic baseline (~22 req/s attack waves)
  - `normal`: ~14 req/s organic baseline (~40 req/s attack waves)
  - `heavy`: ~28 req/s organic baseline (~70 req/s attack waves)
  - Includes unique public client IPs across multiple CIDRs, sticky session micro-bursts, `GET`/`POST`/`HEAD` routes with `Idempotency-Key`, canary requests (`X-Canary: true`, `?canary=true`, cookie), and occasional slow queries.
- **`directorLoop` & Reversible Chaos Drills**: Alternates organic baselines with randomized autonomous scenarios (or immediate manual wave triggers from UI/API):
  - `sticky_affinity`: Concentrates returning requests from a pinned client IP to demonstrate IP Hash & Smart Auto `P3` affinity switching.
  - `single_ip_dos`: High-rate flood from a single rogue IP blended with normal organic clients so you can watch Edge Shield block the attacker with `HTTP 429` without harming legitimate users.
  - `botnet_ddos`: Rotating swarm of 5-8 attacker IPs testing concurrent per-IP rate limiting.
  - `latency_spike`: Temporarily injects chaos delay onto one healthy node via `/chaos/delay`, triggering Smart Auto `P1 Least Connections` skew avoidance, then reverts delay back to baseline.
  - `canary_weighted_rollout`: Temporarily assigns asymmetric capacity weights (e.g., `5:4:1`), tags a canary node (`v2.0-canary`), streams canary traffic, and triggers `P2 Weighted RR` before restoring initial weights and canary flags.
  - `node_outage_failover`: Takes one healthy node down via `/chaos/toggle?state=down`, triggers health checks + peer failover + circuit breaker transitions, then brings the node back online and runs recovery health probes.

Stopping the simulator (`Stop` button, `POST /api/sim/stop`, or process shutdown) invokes `restoreChaos(true)` to guarantee every backend reverts to its exact pre-simulation delay, weight, canary flag, healthy state, and closed circuit breaker.

---

## Live Observability Dashboard

Open **[http://localhost:8081](http://localhost:8081)** after launching NexusLB. The console uses a compact dark `data-terminal` architecture organized into four views:

1. **Overview (`#overview`)**:
   - **Top Bar & Algorithm Tabs**: Cluster health pill, active algorithm / Smart Auto delegation indicator, quick traffic controls (`Send 1 req`, live `Sim` toggle pill, `Reset`, custom burst), and zero-downtime tabs (`Smart Auto`, `Round Robin`, `Weighted RR`, `Least Connections`, `IP Hash`).
   - **6-Cell KPI Bar**: Total requests, live Throughput (5s RPS), Avg latency & median p50, Tail p95 / p99 latency, Success rate & Canary request count, and In-flight queued sockets / Blocked `429` count.
   - **Overview Simulator Strip**: Live wave pill, summary with target RPS, `Low`/`Normal`/`Heavy` selector, `Chaos` switch, `Start`/`Stop`, and `Studio ->` shortcut.
   - **Adaptive Explainability Panel**: Visible when `Smart Auto` is active; renders live rule-by-rule evaluation matrix (`P1`-`P4`) and timestamped transition timeline.
   - **Cluster Telemetry & Traffic Controls**: 24-second Chart.js sliding chart (`Throughput`, `Avg latency`, `Tail p95`), proportional backend distribution bar, Edge Shield toggle, socket-hold tester (`Hold active`), and quick canary/flood actions.
2. **Upstream Pool (`#pool` / `#backends`)**:
   - Per-backend cards displaying health/circuit state badge (`Healthy`, `Half-open`, `Circuit open`, `Offline`), request/latency/error stats, `Take down` / `Bring up` action, `Simulated lag` presets (`0`, `100`, `300`, `600 ms` plus active custom chaos lag label), `WRR weight` stepper, `Pinned sockets` stepper, and `Canary pool` toggle.
3. **Defense, Simulator & Canary Lab (`#lab`)**:
   - **Traffic Simulator Studio**: Config bar (`Intensity`, `Reversible chaos drills`, `Periodic DDoS waves`), 4-cell live breakdown (`Unique IPs`, `Repeat IPs`, `Attack / 429`, `Canary / Chaos`), 6 on-demand wave buttons (`Single-IP DoS`, `Botnet DDoS`, `Sticky clients`, `Latency spike`, `Canary + WRR`, `Node outage drill`), and scrollable **Simulation Timeline**.
   - **DDoS Rate-Limiter Lab**: Live `Refill req/s` and `Burst limit` tuner (`Apply limits`), Edge Shield toggle, multi-step automated DDoS scenario banner, attacker flood (`35x`), and valid user burst (`15x`).
   - **Canary Rollout Lab**: Send single or `10x` canary traffic and one-click `Apply 90/10 split` under Weighted RR.
4. **Request Trace (`#trace`)**:
   - Filterable (`All`, `2xx OK`, `429`, `5xx`, `Canary`) and searchable live log table of recent requests showing timestamp, clickable `X-Request-ID` copy badge, HTTP method, client IP, target backend (with Canary tag), status pill, and upstream latency.

---

## Simulating & Testing Active Connections

To inspect **Least Connections** or **Smart Auto** connection-skew reactions:

1. Open `http://localhost:8081` (`Overview`) and select **Smart Auto** or **Least Connections**.
2. Under **Traffic controls -> Socket hold simulation**, configure `Conns` (e.g., `5`) and `Seconds` (e.g., `4`) and click **Hold active**, or adjust **Pinned sockets** (`-` / `+`) on individual cards in **Upstream Pool**.
3. Alternatively, use the REST API directly:

```bash
# Pin Server 1 active connections directly to 10
curl -X POST "http://localhost:8081/api/backend/connections?url=http://localhost:8001&count=10"

# Adjust Server 2 active connections by +3
curl -X POST "http://localhost:8081/api/backend/connections?url=http://localhost:8002&delta=3"

# Reset Server 1 active connections to 0
curl -X POST "http://localhost:8081/api/backend/connections?url=http://localhost:8001&count=0"
```

---

## Fault Tolerance & Load Rebalancing

You can trigger and verify zero-drop failover either autonomously (`Node outage drill` in Lab Studio) or manually:

1. Start the simulator from the Overview bar or trigger `node_outage_failover` in **Lab**.
2. Or go to **Pool** (`#backends`), click **Take down** on Node 2, and observe immediate traffic redistribution across remaining healthy nodes followed by automatic restoration when brought up:

```bash
# Mark Node 2 offline
curl -X POST "http://localhost:8081/api/backend/toggle?url=http://localhost:8002&state=down"

# Verify proxy traffic routes cleanly to remaining healthy nodes
curl http://localhost:8080/

# Restore Node 2 and allow health checks to reintegrate it
curl -X POST "http://localhost:8081/api/backend/toggle?url=http://localhost:8002&state=up"
```

---

## Academic Engineering Journals

As part of the UCS503P curriculum, each team member authored detailed engineering journals documenting challenging technical obstacles encountered during design, debugging, and benchmarking:

| Student | Journal | Topic | Summary |
| :--- | :--- | :--- | :--- |
| **Navjot Singh** (`1024030313`) | [Journal 01](./journals/1024030313-navjot/01-concurrent-slice-mutation.md) | Concurrent Slice Mutation Race Condition | Fixed a data race where concurrent requests mutating dynamic slice memory corrupted round-robin traversal, replaced with an atomic ring pointer. |
| **Navjot Singh** (`1024030313`) | [Journal 02](./journals/1024030313-navjot/02-least-connections-clustering-tiebreaker.md) | Least Connections Node Starvation & Tie-Breaking | Resolved a load clustering defect under fast endpoints where index 0 monopolized 99.4% of traffic; introduced rotating circular offsets for fair tie-breaking. |
| **Navjot Singh** (`1024030313`) | [Journal 03](./journals/1024030313-navjot/03-smooth-weighted-round-robin-burst-clustering.md) | Smooth Weighted Round-Robin (SWRR) & Burst Clustering | Solved severe traffic clustering and high latency under integer weighted round-robin; implemented NGINX SWRR smooth interleaving and failure weight normalization. |
| **Navjot Singh** (`1024030313`) | [Journal 04](./journals/1024030313-navjot/04-reverse-proxy-closure-race-condition.md) | Shared ReverseProxy Error Handler Data Race | Fixed a critical write-write race condition and closure state hijacking across concurrent goroutines in the reverse proxy error pipeline; decoupled handlers into static initialization with interface assertion. |
| **Shaina Gera** (`1024030316`) | [Journal 01](./journals/1024030316-shaina/01-reverse-proxy-telemetry-loss.md) | Telemetry Loss with `httputil.ReverseProxy` | Solved upstream status code loss caused by standard reverse proxy streaming directly to sockets; implemented zero-allocation `statusRecorder`. |
| **Shaina Gera** (`1024030316`) | [Journal 02](./journals/1024030316-shaina/02-ip-hash-ephemeral-port-affinity-skew.md) | Ephemeral Port Skew in IP Hashing | Fixed broken sticky sessions where browser parallel TCP ports scattered client traffic; implemented `ExtractClientIP` port stripping and sequential ring walk failover. |
| **Shaina Gera** (`1024030316`) | [Journal 03](./journals/1024030316-shaina/03-canary-steering-outage-fallback.md) | Canary Routing Degradation & Transparent Fallback | Resolved client-facing 503 outages when canary nodes fail; implemented multi-vector L7 routing with automatic self-healing fallback to stable nodes. |
| **Shaina Gera** (`1024030316`) | [Journal 04](./journals/1024030316-shaina/04-transparent-retry-staged-recorder.md) | Premature Socket Commitment & Transparent Failover | Eliminated hard client 502 drops on upstream socket resets; implemented staged memory recording (`retryRecorder`) with non-idempotent body replay and protocol passthrough (`Hijacker`/`Flusher`). |
| **Prabhgun Kaur** (`1024030320`) | [Journal 01](./journals/1024030320-prabhgun/01-asset-embedding-boundary-violation.md) | Compile-Time Encapsulation Failure in `go:embed` | Resolved `pattern ../../web/*: cannot use '..'` compile errors by co-locating `web.go` to hermetically bundle UI assets into the binary. |
| **Prabhgun Kaur** (`1024030320`) | [Journal 02](./journals/1024030320-prabhgun/02-sse-stream-goroutine-leak-timeout-conflict.md) | SSE Goroutine Leak & `WriteTimeout` Collision | Resolved stream disconnects every 15s caused by `WriteTimeout` conflicts and eliminated goroutine leakage on tab refreshes using context cancellation. |
| **Prabhgun Kaur** (`1024030320`) | [Journal 03](./journals/1024030320-prabhgun/03-token-bucket-ip-churn-memory-leak.md) | Memory Exhaustion Under IP Churn in Token Bucket | Eliminated unbounded memory growth during botnet flood simulations; implemented periodic background sweeper and lock-free token bucket eviction. |
| **Prabhgun Kaur** (`1024030320`) | Journal 04 *(not present in repo)* | Latent Upstream Blackholes & Circuit Breaking | Resolved synthetic `/health` polling blind spots during application degradation; implemented finite state machine Circuit Breaker (`Closed`/`Open`/`Half-Open`), outlier detection, and slice/map mutex protection. |

---

## API Reference

### NexusLB Ingress Gateway (`http://localhost:8080`)

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `ANY` | `/*` | Proxies requests through Edge Shield, L7 Canary Steering, Retry Pipeline, and active StrategyManager |

### NexusLB Admin & Telemetry API (`http://localhost:8081`)

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/` | Serves the embedded `data-terminal` observability dashboard |
| `GET` | `/api/stats` | JSON snapshot of cluster KPIs, strategy/Smart Auto state, backend health, recent trace logs, and `simulation` state |
| `GET` | `/api/stream` | Server-Sent Events (SSE) live telemetry stream (1 Hz) |
| `GET` | `/api/strategy` | Returns active strategy key/name and available strategies |
| `POST` | `/api/strategy?name=STRAT` | Hot-swaps strategy (`adaptive`, `round_robin`, `weighted_round_robin`, `least_connections`, `ip_hash`) |
| `POST` | `/api/test-request?count=N&delay=MS&client_ip=IP&canary=[true\|false]&sim_ips=[true\|false]` | Fires N concurrent synthetic requests held in-flight for D ms with optional spoofed IP or canary flag |
| `GET` | `/api/shield` | Returns Edge Shield status (`enabled`, `rate`, `burst`, `total_throttled`) |
| `POST` | `/api/shield/toggle?state=[on\|off]` | Enables or bypasses Edge Shield per-IP rate limiting |
| `POST` | `/api/shield/config?rate=N&burst=N` | Updates token bucket refill rate (req/s) and burst ceiling |
| `POST` | `/api/backend/toggle?url=URL&state=[up\|down]` | Marks upstream backend online or offline |
| `POST` | `/api/backend/delay?url=URL&ms=N` | Injects artificial simulated delay on target upstream (`mock /chaos/delay` + `simulated_delay_ms`) |
| `POST` | `/api/backend/connections?url=URL&count=N&delta=N` | Overrides or adjusts simulated active connection counter on target upstream |
| `POST` | `/api/backend/weight?url=URL&weight=N&delta=N` | Sets or adjusts SWRR backend weight (`>= 1`) |
| `POST` | `/api/backend/canary?url=URL&enabled=[true\|false]&tag=TAG` | Tags or untags target upstream as a Canary pool member |
| `POST` | `/api/stats/reset` | Resets metrics collector, rate limiter buckets, Smart Auto transition history, and simulator counters/events |
| `GET` | `/api/sim/status` | Returns real-life traffic simulator `Snapshot` (status, wave, intensity, attacker count, counters, events) |
| `POST` | `/api/sim/start` | Starts simulator; accepts optional query or JSON `intensity`, `allow_chaos`, `allow_ddos` |
| `POST` | `/api/sim/stop` | Stops simulator and restores pre-wave backend delays, weights, canary flags, health, and circuit breakers |
| `POST` | `/api/sim/config` | Live-updates simulator `intensity`, `allow_chaos`, or `allow_ddos` without interrupting stream |
| `POST` | `/api/sim/trigger?wave=WAVE` | Preempts to an immediate wave (`organic_baseline`, `sticky_affinity`, `single_ip_dos`, `botnet_ddos`, `latency_spike`, `canary_weighted_rollout`, `node_outage_failover`), auto-starting if paused |

### Mock Upstream Cluster (`http://localhost:8001`, `8002`, `8003`)

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/?delay=MS` | Responds with upstream server port and identity after optional delay |
| `GET` | `/health` | Returns `200 OK` when healthy or `503 Service Unavailable` when toggled down via `/chaos/toggle` |
| `GET/POST` | `/chaos/toggle?state=[up\|down]` | Simulates upstream crash/recovery on that mock node |
| `GET/POST` | `/chaos/delay?ms=MS` | Sets artificial processing delay in milliseconds on that mock node |

---

## Testing & Verification

### Go Tests & Static Checks

```bash
go vet ./...
go test -v -count=1 ./...
```

On toolchains with cgo enabled (Linux/macOS or Windows with gcc):

```bash
go test -race ./...
```

### Frontend Syntax Check

```bash
node --check web/app.js
```

### Test Coverage Breakdown

- `config/config_test.go`: JSON config loading, defaults, duration parsing, and `sim_intensity` validation.
- `pkg/backend/backend_test.go`: Atomic state/weight/canary/simulated delay mutations and `Closed` / `Open` / `Half-Open` circuit breaker transitions.
- `pkg/balancer/*_test.go`: Round Robin, Smooth Weighted Round Robin, Least Connections circular tie-breaker, FNV-1a IP Hash affinity + ring failover, Canary steering & fallback, `StrategyManager` concurrent hot-swaps, and `Smart Auto` priority rules + cooldown hysteresis (`adaptive_test.go`).
- `pkg/dashboard/server_test.go`: SSE telemetry streaming, strategy swaps, connection/weight/delay/canary mutations, Edge Shield APIs, stats reset, and `/api/sim/*` lifecycle routes.
- `pkg/health/checker_test.go`: Proactive `/health` failure detection and auto-reintegration.
- `pkg/metrics/metrics_test.go`: Rolling throughput, latency percentiles (`p50`/`p95`/`p99`), and log ring buffer tests.
- `pkg/proxy/proxy_test.go`: Transparent retry failover, payload size cap, idempotency enforcement, and Edge Shield `429` interception.
- `pkg/ratelimit/limiter_test.go`: Token bucket replenishment, per-IP isolation, config hot-updates, and stale bucket eviction.
- `pkg/simulator/simulator_test.go`: Unique/repeat IP diversity, `single_ip_dos` / `botnet_ddos` `429` validation, reversible chaos & `Stop()` baseline restore, configure/reset, and default `/chaos/*` hook wiring.

---

## Engineering Highlights & Concurrency

1. **Zero-Allocation Hot-Path Routing**: Pre-allocated backend slices paired with 64-bit atomic cursors and circular tie-breakers avoid slice filtering allocations on per-request selection.
2. **Staged Retry Buffering & Status Capture**: `retryRecorder` buffers response headers and bodies before socket commitment so failed upstream connections retry alternate backends transparently while capturing accurate upstream status codes and latencies.
3. **Hermetic Single-Binary Packaging**: [`web/web.go`](./web/web.go) embeds all dashboard UI files via `go:embed`, producing a self-contained binary with zero runtime template or asset path dependencies.
4. **Anti-Flap Meta-Balancing & Observable Explainability**: `AdaptiveBalancer` decouples periodic `250ms` cluster evaluation from fast-path selection while enforcing hysteresis cooldowns with immediate escalation overrides and full human-readable transition history.
5. **Deterministic Simulator Baseline Rollback**: Every chaos perturbation snapshots original per-node lag, WRR weights, canary flags/tags, node health, and touched circuit breakers so waves and shutdowns cleanly restore cluster baselines without orphaned faults.
6. **Context-Controlled Shutdown & Leak Prevention**: SSE streams, health pollers, bucket sweepers, simulator worker goroutines, and HTTP listeners terminate cleanly via context cancellation and bounded drain timeouts on `SIGINT`/`SIGTERM`.
