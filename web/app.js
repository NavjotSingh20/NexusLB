document.addEventListener("DOMContentLoaded", () => {
  const STRATEGY_META = {
    adaptive: "Evaluates live socket skew, node lag, weights, and repeat IPs to select the optimal strategy.",
    round_robin: "Distributes incoming requests sequentially across healthy nodes.",
    weighted_round_robin: "Routes requests proportionally according to configured server weights.",
    least_connections: "Targets the healthy node holding the fewest active in-flight sockets.",
    ip_hash: "Hashes the client IP via FNV-1a so returning clients stay pinned to one node.",
  };

  const RULE_LABELS = {
    connection_skew: "Rule: Socket skew",
    latency_skew: "Rule: Latency skew",
    capacity_weights: "Rule: Capacity weights",
    client_affinity: "Rule: Repeat client IPs",
    uniform_baseline: "Rule: Uniform baseline",
  };

  const MAX_HISTORY = 24;
  const TELEMETRY_INTERVAL_MS = 850;

  let currentStats = null;
  let currentStrategyKey = "round_robin";
  let activeView = "overview";
  let chartMetric = "rps";
  let logFilter = "all";
  let logSearchQuery = "";
  let shieldState = true;
  let autoTrafficTimer = null;
  let isScenarioRunning = false;
  let lastSampleTimestamp = 0;
  const backendDelays = {};

  const history = seedInitialHistory();

  // DOM References
  const appShell = document.getElementById("app-shell");
  const railButtons = document.querySelectorAll(".rail-item[data-nav]");
  const sseDot = document.getElementById("sse-dot");
  const sseIndicator = document.getElementById("sse-indicator");

  const elHealthyBadge = document.getElementById("healthy-count-badge");
  const elStrategyBadge = document.getElementById("active-strategy-display");
  const elClusterFinding = document.getElementById("cluster-finding");
  const elProxyUrl = document.getElementById("proxy-url-display");
  const elUptimeVal = document.getElementById("uptime-val");
  const elShieldStatusLabel = document.getElementById("shield-status-label");
  const elStrategyDesc = document.getElementById("strategy-desc");

  const kpiTotal = document.getElementById("kpi-total-requests");
  const kpiReqDelta = document.getElementById("kpi-requests-delta");
  const kpiRps = document.getElementById("kpi-rps");
  const kpiLatency = document.getElementById("kpi-latency");
  const kpiP50 = document.getElementById("kpi-p50");
  const kpiP95 = document.getElementById("kpi-p95");
  const kpiP99 = document.getElementById("kpi-p99");
  const kpiSuccess = document.getElementById("kpi-success-rate");
  const kpiCanarySub = document.getElementById("kpi-canary-sub");
  const kpiQueued = document.getElementById("kpi-queued");
  const kpiThrottled = document.getElementById("kpi-throttled");

  const chartHost = document.getElementById("telemetry-chart");
  const chartTabs = document.querySelectorAll("#chart-metric-tabs .tab");
  const elDistBar = document.getElementById("distribution-bar");
  const elDistLegend = document.getElementById("distribution-legend");
  const elDistTotal = document.getElementById("distribution-total-label");

  const btnTestSingle = document.getElementById("btn-test-single");
  const btnTestBurst = document.getElementById("btn-test-burst");
  const inputBurstCount = document.getElementById("burst-count-input");
  const btnTestSlow = document.getElementById("btn-test-slow");
  const inputHoldConns = document.getElementById("hold-conns-count");
  const inputHoldSec = document.getElementById("hold-conns-sec");
  const checkSimIps = document.getElementById("check-sim-ips");
  const btnAutoTraffic = document.getElementById("btn-auto-traffic");
  const autoTrafficDot = document.getElementById("auto-traffic-dot");
  const autoTrafficLabel = document.getElementById("auto-traffic-label");
  const btnResetMetrics = document.getElementById("btn-reset-metrics");

  const btnToggleShield = document.getElementById("btn-toggle-shield");
  const btnLabToggleShield = document.getElementById("btn-lab-toggle-shield");
  const shieldBtnText = document.getElementById("shield-btn-text");
  const labShieldPill = document.getElementById("lab-shield-pill");

  const btnQuickCanary = document.getElementById("btn-quick-canary");
  const btnQuickFlood = document.getElementById("btn-quick-flood");
  const btnRunDdosScenario = document.getElementById("btn-run-ddos-scenario");
  const btnSpamAttacker = document.getElementById("btn-spam-attacker");
  const btnSendNormalUsers = document.getElementById("btn-send-normal-users");
  const btnSendCanarySingle = document.getElementById("btn-send-canary-single");
  const btnSendCanaryBurst = document.getElementById("btn-send-canary-burst");
  const btnSetCanaryWeights = document.getElementById("btn-set-canary-weights");

  const storylineBanner = document.getElementById("storyline-banner");
  const storylinePhase = document.getElementById("storyline-phase-badge");
  const storylineTitle = document.getElementById("storyline-title");
  const storylineMessage = document.getElementById("storyline-message");
  const storylineProgress = document.getElementById("storyline-progress-fill");

  const backendsContainer = document.getElementById("backend-cards-container");
  const logsTbody = document.getElementById("logs-table-body");
  const elLogCountBadge = document.getElementById("log-count-badge");
  const traceFilterTabs = document.querySelectorAll("#trace-filter-tabs .tab");
  const traceSearchInput = document.getElementById("trace-search-input");
  const toastRegion = document.getElementById("toast-region");

  function seedInitialHistory() {
    const now = Date.now();
    const labels = [];
    const rps = [];
    const latency = [];
    const p95 = [];
    for (let i = MAX_HISTORY - 1; i >= 0; i -= 1) {
      labels.push(formatClock(new Date(now - i * 1000)));
      rps.push(0);
      latency.push(0);
      p95.push(0);
    }
    return { labels, rps, latency, p95 };
  }

  function formatClock(date) {
    const hh = String(date.getHours()).padStart(2, "0"); // taste-skill-ignore: clock timestamp
    const mm = String(date.getMinutes()).padStart(2, "0"); // taste-skill-ignore: clock timestamp
    const ss = String(date.getSeconds()).padStart(2, "0"); // taste-skill-ignore: clock timestamp
    return `${hh}:${mm}:${ss}`;
  }

  function formatUptime(sec) {
    const s = Math.max(0, Number(sec) || 0);
    const hrs = Math.floor(s / 3600);
    const mins = Math.floor((s % 3600) / 60);
    const rem = s % 60;
    return [hrs, mins, rem].map((v) => String(v).padStart(2, "0")).join(":"); // taste-skill-ignore: duration clock
  }

  function escapeHTML(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function showToast(msg) {
    if (!toastRegion) return;
    const item = document.createElement("div");
    item.className = "toast";
    item.textContent = msg;
    toastRegion.appendChild(item);
    setTimeout(() => {
      item.remove();
    }, 3200);
  }

  function setView(viewName, updateHash = true) {
    const allowed = new Set(["overview", "backends", "lab", "logs"]);
    const next = allowed.has(viewName) ? viewName : "overview";
    activeView = next;
    if (appShell) appShell.setAttribute("data-view", next);

    railButtons.forEach((btn) => {
      const match = btn.getAttribute("data-nav") === next;
      btn.classList.toggle("is-active", match);
      btn.setAttribute("aria-current", match ? "page" : "false");
    });

    if (updateHash) {
      const targetHash = `#${next}`;
      if (window.location.hash !== targetHash) {
        historyPushOrReplace(targetHash);
      }
    }

    if (currentStats) {
      renderLogs(currentStats.recent_logs || []);
      if (next === "overview") {
        renderTelemetryChart();
      }
    }
  }

  function historyPushOrReplace(hash) {
    try {
      window.history.replaceState(null, "", hash);
    } catch {
      window.location.hash = hash;
    }
  }

  function syncViewFromHash() {
    const hash = window.location.hash.replace("#", "").trim();
    if (hash) {
      setView(hash, false);
    }
  }

  function sampleTelemetry(data) {
    const now = Date.now();
    const label = formatClock(new Date(now));
    const nextRps = Number(data.rps || 0);
    const nextLatency = Number(data.avg_latency_ms || 0);
    const nextP95 = Number(data.p95_latency_ms || 0);

    if (now - lastSampleTimestamp < TELEMETRY_INTERVAL_MS && history.labels.length > 0) {
      const lastIdx = history.labels.length - 1;
      history.rps[lastIdx] = nextRps;
      history.latency[lastIdx] = nextLatency;
      history.p95[lastIdx] = nextP95;
    } else {
      lastSampleTimestamp = now;
      history.labels.push(label);
      history.rps.push(nextRps);
      history.latency.push(nextLatency);
      history.p95.push(nextP95);

      while (history.labels.length > MAX_HISTORY) {
        history.labels.shift();
        history.rps.shift();
        history.latency.shift();
        history.p95.shift();
      }
    }
  }

  function renderTelemetryChart() {
    if (!chartHost || activeView !== "overview" || typeof window.Charts === "undefined") return;
    if (chartHost.clientWidth <= 0) return;

    let seriesName = "Throughput";
    let values = history.rps;
    let formatter = (v) => `${Number(v).toFixed(1)} req/s`;
    let fallbackMax = 10;

    if (chartMetric === "latency") {
      seriesName = "Avg latency";
      values = history.latency;
      formatter = (v) => `${Number(v).toFixed(1)} ms`;
      fallbackMax = 50;
    } else if (chartMetric === "p95") {
      seriesName = "Tail p95";
      values = history.p95;
      formatter = (v) => `${Number(v).toFixed(1)} ms`;
      fallbackMax = 100;
    }

    const peak = Math.max(0, ...values);
    const mount = document.createElement("div");
    mount.className = "chart-mount";
    chartHost.replaceChildren(mount);

    const xTicks = chartHost.clientWidth < 520 ? [0, 11, 23] : [0, 7, 15, 23];

    window.Charts.line(mount, {
      labels: history.labels,
      series: [{ name: seriesName, values }],
      format: formatter,
      tickFormat: window.Charts.fmt.dec(1),
      height: 244,
      fill: true,
      min: 0,
      max: peak <= 0 ? fallbackMax : undefined,
      xAt: xTicks,
      endLabel: false,
    });
  }

  function computeFinding(data) {
    const backends = data.backends || [];
    const total = backends.length;
    const healthy = backends.filter((b) => b.alive).length;
    const offline = backends.filter((b) => !b.alive);
    const openCircuits = backends.filter((b) => {
      const state = (b.circuit_state || "closed").toLowerCase();
      return state !== "closed";
    });

    if (total === 0) {
      return "No upstream backends configured for this cluster.";
    }
    if (data.adaptive && data.strategy_key === "adaptive") {
      return `Smart Auto -> ${data.adaptive.active_name}: ${data.adaptive.reason}`;
    }
    if (offline.length > 0) {
      return `${offline.length} of ${total} backends offline (${offline.map((b) => b.url.replace("http://", "")).join(", ")}); healthy nodes absorb cluster load.`;
    }
    if (openCircuits.length > 0) {
      return `Circuit breaker open on ${openCircuits[0].url.replace("http://", "")}; failover active across ${healthy}/${total} nodes.`;
    }
    if (data.queued_requests > 0) {
      return `${data.queued_requests} in-flight socket${data.queued_requests === 1 ? "" : "s"} active across ${healthy}/${total} nodes under ${data.strategy}.`;
    }
    if ((data.total_throttled || 0) > 0 && data.shield_enabled) {
      return `Edge Shield rate limiter blocked ${data.total_throttled} flood requests (HTTP 429) while upstream nodes remained nominal.`;
    }
    if ((data.total_canary_requests || 0) > 0) {
      return `Canary isolation routed ${data.total_canary_requests} tagged requests; ${healthy}/${total} backends healthy at ${Number(data.avg_latency_ms || 0).toFixed(1)} ms avg.`;
    }
    if ((data.total_requests || 0) === 0) {
      return `All ${healthy}/${total} upstream backends healthy; waiting for ingress traffic under ${data.strategy}.`;
    }
    return `Nominal: ${healthy}/${total} backends healthy at ${Number(data.avg_latency_ms || 0).toFixed(1)} ms avg (${Number(data.p95_latency_ms || 0).toFixed(1)} ms p95).`;
  }

  function connectSSE() {
    const evtSource = new EventSource("/api/stream");

    evtSource.onopen = () => {
      if (sseDot) sseDot.className = "status-dot";
      if (sseIndicator) sseIndicator.title = "Live SSE telemetry connected";
    };

    evtSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        updateUI(data);
      } catch (err) {
        console.error("Failed to parse SSE telemetry", err);
      }
    };

    evtSource.onerror = () => {
      if (sseDot) sseDot.className = "status-dot down";
      if (sseIndicator) sseIndicator.title = "Telemetry stream disconnected, retrying";
    };
  }

  async function fetchStats() {
    try {
      const res = await fetch("/api/stats");
      if (res.ok) {
        const data = await res.json();
        updateUI(data);
      }
    } catch (err) {
      console.error("Stats fetch error", err);
    }
  }

  function updateUI(data) {
    currentStats = data;
    currentStrategyKey = data.strategy_key || "round_robin";
    shieldState = Boolean(data.shield_enabled);

    if (elUptimeVal) elUptimeVal.textContent = formatUptime(data.uptime_seconds || 0);
    if (elStrategyBadge) {
      if (data.adaptive && currentStrategyKey === "adaptive") {
        elStrategyBadge.textContent = `Smart Auto -> ${data.adaptive.active_name}`;
      } else {
        elStrategyBadge.textContent = data.strategy || "Round Robin";
      }
    }
    if (elStrategyDesc) {
      if (data.adaptive && currentStrategyKey === "adaptive") {
        const ruleName = RULE_LABELS[data.adaptive.rule_matched] || "Active policy";
        elStrategyDesc.textContent = `Delegating to ${data.adaptive.active_name} (${ruleName.replace("Rule: ", "")})`;
      } else {
        elStrategyDesc.textContent = STRATEGY_META[currentStrategyKey] || "";
      }
    }
    if (elClusterFinding) elClusterFinding.textContent = computeFinding(data);

    if (elProxyUrl) {
      elProxyUrl.textContent = `${window.location.protocol}//${window.location.hostname}:8080`;
    }

    // Strategy underline selection
    document.querySelectorAll(".strategy-pill[data-strategy]").forEach((btn) => {
      const isSelected = btn.getAttribute("data-strategy") === currentStrategyKey;
      btn.classList.toggle("is-active", isSelected);
      btn.setAttribute("aria-selected", isSelected ? "true" : "false");
    });

    // Shield readouts
    const elShieldDesc = document.getElementById("shield-desc");
    if (elShieldDesc && data.shield_rate != null && data.shield_burst != null) {
      elShieldDesc.textContent = `Token-bucket IP throttle at ${data.shield_rate} req/s (burst ${data.shield_burst})`;
    }
    if (elShieldStatusLabel) elShieldStatusLabel.textContent = shieldState ? "Active" : "Bypassed";
    if (shieldBtnText) shieldBtnText.textContent = shieldState ? "Disable" : "Enable";
    if (btnLabToggleShield) btnLabToggleShield.textContent = shieldState ? "Disable" : "Enable";
    if (labShieldPill) {
      labShieldPill.textContent = shieldState ? "Shield active" : "Shield bypassed";
      labShieldPill.className = shieldState ? "pill ok" : "pill warn";
    }

    // KPIs
    const total = Number(data.total_requests || 0);
    const success = Number(data.total_success || 0);
    const errCount = Number(data.total_client_errors || 0) + Number(data.total_server_errors || 0);
    const rate = total > 0 ? (success / total) * 100 : 100;

    if (kpiTotal) kpiTotal.textContent = total.toLocaleString();
    if (kpiReqDelta) kpiReqDelta.textContent = `${success.toLocaleString()} OK / ${errCount.toLocaleString()} err`;
    if (kpiRps) kpiRps.innerHTML = `${Number(data.rps || 0).toFixed(1)} <span class="kpi-unit">req/s</span>`;
    if (kpiLatency) kpiLatency.innerHTML = `${Number(data.avg_latency_ms || 0).toFixed(1)} <span class="kpi-unit">ms</span>`;
    if (kpiP50) kpiP50.textContent = `${Number(data.p50_latency_ms || 0).toFixed(1)} ms`;
    if (kpiP95) kpiP95.innerHTML = `${Number(data.p95_latency_ms || 0).toFixed(1)} <span class="kpi-unit">ms</span>`;
    if (kpiP99) kpiP99.textContent = `${Number(data.p99_latency_ms || 0).toFixed(1)} ms`;

    if (kpiSuccess) {
      kpiSuccess.textContent = `${rate.toFixed(1)}%`;
      kpiSuccess.classList.toggle("bad", rate < 90 && total > 0);
      kpiSuccess.classList.toggle("warn", rate >= 90 && rate < 98 && total > 0);
    }

    if (kpiCanarySub) {
      kpiCanarySub.textContent = `Canary ${Number(data.total_canary_requests || 0).toLocaleString()} reqs`;
    }

    if (kpiQueued) {
      const queued = Number(data.queued_requests || 0);
      kpiQueued.innerHTML = `${queued.toLocaleString()} <span class="kpi-unit">queued</span>`;
      kpiQueued.classList.toggle("warn", queued > 0);
    }
    if (kpiThrottled) {
      kpiThrottled.textContent = Number(data.total_throttled || 0).toLocaleString();
    }

    // Health badge
    const backends = data.backends || [];
    const aliveCount = backends.filter((b) => b.alive).length;
    if (elHealthyBadge) {
      elHealthyBadge.textContent = `${aliveCount} / ${backends.length} healthy`;
      elHealthyBadge.className = aliveCount === backends.length ? "pill ok" : aliveCount > 0 ? "pill warn" : "pill bad";
    }

    sampleTelemetry(data);
    renderAdaptiveExplainer(currentStrategyKey === "adaptive" ? data.adaptive : null);
    renderTelemetryChart();
    renderDistribution(backends);
    renderBackends(backends);
    renderLogs(data.recent_logs || []);
  }

  function renderAdaptiveExplainer(adaptive) {
    const elPanel = document.getElementById("adaptive-explainer");
    if (!elPanel) return;

    if (!adaptive) {
      elPanel.classList.add("hidden");
      return;
    }
    elPanel.classList.remove("hidden");

    const elDelegatePill = document.getElementById("adaptive-delegate-pill");
    const elRulePill = document.getElementById("adaptive-rule-pill");
    const elReason = document.getElementById("adaptive-reason-text");
    const elSwitchCount = document.getElementById("adaptive-switch-count");
    const elCooldown = document.getElementById("adaptive-cooldown-label");
    const elEvalTime = document.getElementById("adaptive-eval-time");
    const elRulesList = document.getElementById("adaptive-rules-list");
    const elHistoryList = document.getElementById("adaptive-history-list");

    const sig = adaptive.signals || {};
    const cdMs = Number(adaptive.cooldown_remaining_ms || 0);
    const swCount = Number(adaptive.switch_count || 0);

    if (elDelegatePill) elDelegatePill.textContent = `Active: ${adaptive.active_name || "Round Robin"}`;
    if (elRulePill) elRulePill.textContent = RULE_LABELS[adaptive.rule_matched] || adaptive.rule_matched || "Policy";
    if (elReason) elReason.textContent = adaptive.reason || "Evaluating live cluster telemetry.";
    if (elSwitchCount) elSwitchCount.textContent = `${swCount} transition${swCount === 1 ? "" : "s"}`;
    if (elCooldown) {
      elCooldown.textContent = cdMs > 0 ? `Cooldown ${(cdMs / 1000).toFixed(1)}s` : "Stable";
    }
    if (elEvalTime) elEvalTime.textContent = adaptive.evaluated_at ? `Updated ${adaptive.evaluated_at}` : "Live";

    if (elRulesList) {
      const sampleCount = Number(sig.recent_sample_size || 0);
      const uniqueIPs = Number(sig.unique_recent_ips || 0);
      const repeatPct = Math.round(Number(sig.repeat_ip_ratio || 0) * 100);
      const skewRatio = Number(sig.latency_skew_ratio || 1).toFixed(1);

      const rules = [
        {
          key: "least_connections",
          rank: "P1",
          name: "Least Connections",
          cond: "Socket spread >= 2 or max lag >= 80 ms (2.5x skew)",
          metric: `Spread ${Number(sig.conn_spread || 0)} / Max ${Number(sig.max_latency_ms || 0)} ms (${skewRatio}x)`,
        },
        {
          key: "weighted_round_robin",
          rank: "P2",
          name: "Weighted RR",
          cond: "Healthy node weights differ across the pool",
          metric: `Weights ${sig.weight_summary || "1:1:1"}`,
        },
        {
          key: "ip_hash",
          rank: "P3",
          name: "IP Hash",
          cond: "Repeat client IPs >= 70% across last 24 reqs (min 6)",
          metric: sampleCount > 0 ? `${repeatPct}% repeat (${uniqueIPs}/${sampleCount} IPs)` : "0 samples",
        },
        {
          key: "round_robin",
          rank: "P4",
          name: "Round Robin",
          cond: "Healthy balanced default with uniform weights",
          metric: `${Number(sig.healthy_nodes || 0)}/${Number(sig.total_nodes || 0)} healthy`,
        },
      ];

      elRulesList.innerHTML = rules
        .map((r) => {
          const isSelected = adaptive.active_key === r.key;
          const isPending = !isSelected && adaptive.candidate_key === r.key && cdMs > 0;
          const pillCls = isSelected ? "accent" : isPending ? "warn" : "neutral";
          const pillText = isSelected ? "Selected" : isPending ? "Pending" : "Standby";

          return `
            <div class="rule-item ${isSelected ? "is-selected" : ""}">
              <div class="rule-main">
                <div class="rule-name-row">
                  <span class="rule-rank">${r.rank}</span>
                  <span class="rule-name">${escapeHTML(r.name)}</span>
                </div>
                <span class="rule-cond">${escapeHTML(r.cond)}</span>
              </div>
              <div class="rule-right">
                <span class="rule-metric">${escapeHTML(r.metric)}</span>
                <span class="pill ${pillCls}">${pillText}</span>
              </div>
            </div>
          `;
        })
        .join("");
    }

    if (elHistoryList) {
      const items = Array.isArray(adaptive.history) ? adaptive.history.slice(0, 4) : [];
      if (items.length === 0) {
        elHistoryList.innerHTML = `
          <div class="transition-empty">
            No automated transitions recorded yet. Adjust simulated lag, backend weights, socket hold, or disable Diverse client IPs to observe and explain a live switch.
          </div>
        `;
      } else {
        elHistoryList.innerHTML = items
          .map(
            (ev) => `
              <div class="transition-item">
                <div class="transition-top">
                  <span class="transition-route">${escapeHTML(ev.from_name || ev.from_key)} &rarr; ${escapeHTML(ev.to_name || ev.to_key)}</span>
                  <span class="transition-time">${escapeHTML(ev.timestamp || "")}</span>
                </div>
                <span class="transition-why">${escapeHTML(ev.reason || "")}</span>
              </div>
            `
          )
          .join("");
      }
    }
  }

  function renderDistribution(backends) {
    if (!elDistBar || !elDistLegend) return;
    const total = backends.reduce((sum, b) => sum + (b.total_requests || 0), 0);
    if (elDistTotal) {
      elDistTotal.textContent = `${total.toLocaleString()} total requests routed`;
    }

    if (total === 0) {
      elDistBar.replaceChildren();
      const items = backends.map((b, idx) => {
        const cls = `dist-seg-${idx % 4}`;
        const label = b.url.replace("http://", "");
        return `<span class="dist-legend-item"><span class="dist-swatch ${cls}"></span><span><code>${escapeHTML(label)}</code> <strong>0%</strong></span></span>`;
      });
      elDistLegend.innerHTML = items.join("");
      return;
    }

    elDistBar.innerHTML = backends
      .map((b, idx) => {
        const pct = ((b.total_requests || 0) / total) * 100;
        if (pct <= 0) return "";
        return `<div class="dist-seg dist-seg-${idx % 4}" style="width:${pct.toFixed(2)}%" title="${escapeHTML(b.url)}: ${pct.toFixed(1)}%"></div>`;
      })
      .join("");

    elDistLegend.innerHTML = backends
      .map((b, idx) => {
        const pct = (((b.total_requests || 0) / total) * 100).toFixed(1);
        const host = b.url.replace("http://", "");
        return `<span class="dist-legend-item"><span class="dist-swatch dist-seg-${idx % 4}"></span><span><code>${escapeHTML(host)}</code> <strong>${pct}%</strong> (${(b.total_requests || 0).toLocaleString()})</span></span>`;
      })
      .join("");
  }

  function renderBackends(backends) {
    if (!backendsContainer) return;

    backendsContainer.innerHTML = backends
      .map((b, idx) => {
        const host = b.url.replace("http://", "");
        const isUp = Boolean(b.alive);
        const delayMs = Number(backendDelays[b.url] ?? b.simulated_delay_ms ?? 0);
        const errs = Number(b.total_errors || 0);
        const circuit = (b.circuit_state || "closed").toLowerCase();
        const isCanary = Boolean(b.is_canary);
        const weight = Number(b.weight || 1);

        let statusClass = "ok";
        let statusText = "Healthy";
        if (!isUp) {
          statusClass = "bad";
          statusText = "Offline";
        } else if (circuit === "open") {
          statusClass = "bad";
          statusText = "Circuit open";
        } else if (circuit.includes("half")) {
          statusClass = "warn";
          statusText = "Half-open";
        }

        const latPresets = [0, 100, 300, 600]
          .map(
            (ms) =>
              `<button type="button" class="latency-tab ${delayMs === ms ? "is-active" : ""}" data-action="delay" data-url="${escapeHTML(b.url)}" data-ms="${ms}">${ms} ms</button>`
          )
          .join("");

        return `
          <article class="backend-card ${isUp ? "" : "is-down"}">
            <div class="backend-card-head">
              <div class="backend-title">
                <div class="backend-id-line">
                  <span class="status-dot ${isUp ? "" : "down"}"></span>
                  <strong>Node ${idx + 1}</strong>
                  <span class="pill ${statusClass}">${escapeHTML(statusText)}</span>
                </div>
                <code class="backend-url">${escapeHTML(host)}</code>
              </div>
              <button type="button" class="btn small" data-action="toggle" data-url="${escapeHTML(b.url)}" data-state="${isUp ? "down" : "up"}">
                ${isUp ? "Take down" : "Bring up"}
              </button>
            </div>

            <div class="backend-stats">
              <div class="b-stat">
                <span>Requests</span>
                <strong>${Number(b.total_requests || 0).toLocaleString()}</strong>
              </div>
              <div class="b-stat">
                <span>Last latency</span>
                <strong>${Number(b.last_latency_ms || 0)} ms</strong>
              </div>
              <div class="b-stat">
                <span>Errors</span>
                <strong class="${errs > 0 ? "is-err" : ""}">${errs.toLocaleString()}</strong>
              </div>
            </div>

            <div class="backend-controls">
              <div class="control-line">
                <span class="control-label">Simulated lag</span>
                <div class="latency-tabs">${latPresets}</div>
              </div>

              <div class="control-line">
                <span class="control-label">WRR weight</span>
                <div class="stepper">
                  <button type="button" class="step-btn" data-action="weight" data-url="${escapeHTML(b.url)}" data-delta="-1" aria-label="Decrease weight">-</button>
                  <span class="step-val">${weight}</span>
                  <button type="button" class="step-btn" data-action="weight" data-url="${escapeHTML(b.url)}" data-delta="1" aria-label="Increase weight">+</button>
                </div>
              </div>

              <div class="control-line">
                <span class="control-label">Pinned sockets</span>
                <div class="stepper">
                  <button type="button" class="step-btn" data-action="conn" data-url="${escapeHTML(b.url)}" data-delta="-1" aria-label="Release pinned socket">-</button>
                  <span class="step-val">${Number(b.active_connections || 0)}</span>
                  <button type="button" class="step-btn" data-action="conn" data-url="${escapeHTML(b.url)}" data-delta="1" aria-label="Add pinned socket">+</button>
                </div>
              </div>

              <div class="canary-row">
                <div class="canary-meta">
                  <span class="control-label">Canary pool</span>
                  ${isCanary ? `<span class="pill accent">${escapeHTML(b.canary_tag || "canary")}</span>` : ""}
                </div>
                <label class="switch" aria-label="Toggle canary node">
                  <input type="checkbox" ${isCanary ? "checked" : ""} data-action="canary" data-url="${escapeHTML(b.url)}" />
                  <span class="slider"></span>
                </label>
              </div>
            </div>
          </article>
        `;
      })
      .join("");
  }

  function renderLogs(logs) {
    if (!logsTbody) return;
    const allLogs = Array.isArray(logs) ? logs : [];
    const q = logSearchQuery.trim().toLowerCase();

    const filtered = allLogs.filter((entry) => {
      if (logFilter === "2xx" && !(entry.status >= 200 && entry.status < 300)) return false;
      if (logFilter === "429" && entry.status !== 429) return false;
      if (logFilter === "5xx" && entry.status < 500) return false;
      if (logFilter === "canary" && !entry.is_canary) return false;

      if (q) {
        const hay = `${entry.id || ""} ${entry.client_ip || ""} ${entry.backend || ""} ${entry.path || ""} ${entry.method || ""}`.toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    });

    const visibleLimit = activeView === "overview" ? 6 : 50;
    const visibleRows = filtered.slice(0, visibleLimit);

    if (elLogCountBadge) {
      elLogCountBadge.textContent = `${filtered.length} ${filtered.length === 1 ? "entry" : "entries"}`;
    }

    if (visibleRows.length === 0) {
      logsTbody.innerHTML = `
        <tr>
          <td colspan="7" class="empty-cell">
            ${allLogs.length === 0 ? "No requests routed yet. Send a test request above to populate trace telemetry." : "No trace entries match the current filter."}
          </td>
        </tr>
      `;
      return;
    }

    logsTbody.innerHTML = visibleRows
      .map((l) => {
        const isCanary = Boolean(l.is_canary);
        const statusNum = Number(l.status || 0);
        let statusPillClass = "ok";
        if (statusNum === 429) statusPillClass = "warn";
        else if (statusNum >= 400) statusPillClass = "bad";

        const latMs = Number(l.latency_ms || 0);
        const latClass = latMs > 400 ? "latency-bad" : latMs > 150 ? "latency-warn" : "";
        const targetHost = (l.backend || "-").replace("http://", "");
        const fullId = String(l.id || "");
        const shortId = fullId.length > 8 ? `${fullId.slice(0, 8)}\u2026` : fullId || "n/a";

        return `
          <tr>
            <td class="col-time">${escapeHTML(l.timestamp || "")}</td>
            <td>
              <button type="button" class="req-id-btn" data-copy-id="${escapeHTML(fullId)}" title="Copy full X-Request-ID (${escapeHTML(fullId)})">
                <code>${escapeHTML(shortId)}</code>
              </button>
            </td>
            <td class="col-method"><code>${escapeHTML(l.method || "GET")}</code></td>
            <td class="col-ip"><code>${escapeHTML(l.client_ip || "-")}</code></td>
            <td>
              <div class="target-cell">
                <code>${escapeHTML(targetHost)}</code>
                ${isCanary ? '<span class="pill accent">Canary</span>' : ""}
              </div>
            </td>
            <td class="num"><span class="pill ${statusPillClass}">${statusNum}</span></td>
            <td class="num ${latClass}">${latMs.toFixed(1)} ms</td>
          </tr>
        `;
      })
      .join("");
  }

  // Control Handlers & API Calls

  async function setStrategy(key) {
    try {
      const res = await fetch(`/api/strategy?name=${encodeURIComponent(key)}`, { method: "POST" });
      if (res.ok) {
        await fetchStats();
        showToast(`Strategy set to ${STRATEGY_META[key] ? key.replace(/_/g, " ") : key}`);
      } else {
        showToast("Failed to update strategy");
      }
    } catch {
      showToast("Error updating strategy");
    }
  }

  async function fireTraffic({ count = 1, delay = 0, clientIp = "", canary = false } = {}) {
    const simIps = checkSimIps ? checkSimIps.checked : true;
    const params = new URLSearchParams({
      count: String(count),
      sim_ips: String(simIps),
    });
    if (delay > 0) params.set("delay", String(delay));
    if (clientIp) params.set("client_ip", clientIp);
    if (canary) params.set("canary", "true");

    const res = await fetch(`/api/test-request?${params.toString()}`, { method: "POST" });
    return res.ok;
  }

  async function toggleServer(url, state) {
    await fetch(`/api/backend/toggle?url=${encodeURIComponent(url)}&state=${encodeURIComponent(state)}`, { method: "POST" });
    await fetchStats();
  }

  async function setBackendDelay(url, ms) {
    backendDelays[url] = ms;
    await fetch(`/api/backend/delay?url=${encodeURIComponent(url)}&ms=${encodeURIComponent(ms)}`, { method: "POST" });
    await fetchStats();
  }

  async function adjustBackendConn(url, delta) {
    await fetch(`/api/backend/connections?url=${encodeURIComponent(url)}&delta=${encodeURIComponent(delta)}`, { method: "POST" });
    await fetchStats();
  }

  async function adjustBackendWeight(url, delta) {
    await fetch(`/api/backend/weight?url=${encodeURIComponent(url)}&delta=${encodeURIComponent(delta)}`, { method: "POST" });
    await fetchStats();
  }

  async function setBackendWeight(url, weight) {
    await fetch(`/api/backend/weight?url=${encodeURIComponent(url)}&weight=${encodeURIComponent(weight)}`, { method: "POST" });
  }

  async function toggleCanary(url, enabled) {
    await fetch(`/api/backend/canary?url=${encodeURIComponent(url)}&enabled=${enabled}&tag=v2.0-canary`, { method: "POST" });
    await fetchStats();
  }

  async function toggleShield(forceState) {
    const url = forceState ? `/api/shield/toggle?state=${forceState}` : "/api/shield/toggle";
    const res = await fetch(url, { method: "POST" });
    if (res.ok) {
      const data = await res.json();
      shieldState = Boolean(data.shield_enabled);
      await fetchStats();
      showToast(shieldState ? "Edge Shield rate limiter enabled" : "Edge Shield bypassed");
    }
  }

  function updateStoryline(phase, title, msg, pct) {
    if (!storylineBanner) return;
    storylineBanner.classList.remove("hidden");
    if (storylinePhase) storylinePhase.textContent = phase;
    if (storylineTitle) storylineTitle.textContent = title;
    if (storylineMessage) storylineMessage.textContent = msg;
    if (storylineProgress) storylineProgress.style.width = `${pct}%`;
  }

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  async function runDdosScenario() {
    if (isScenarioRunning) return;
    isScenarioRunning = true;
    setView("lab");

    try {
      if (btnRunDdosScenario) btnRunDdosScenario.disabled = true;

      updateStoryline("Phase 1 / 3", "Baseline traffic check", "Routing legitimate requests from varied client IPs under normal capacity.", 18);
      await fireTraffic({ count: 8 });
      await fetchStats();
      await sleep(2200);

      updateStoryline("Phase 2 / 3", "Unprotected IP flood", "Disabling Edge Shield and sending 40 rapid flood requests from attacker 203.0.113.66.", 55);
      await toggleShield("off");
      await fireTraffic({ count: 40, clientIp: "203.0.113.66" });
      await fetchStats();
      await sleep(2600);

      updateStoryline("Phase 3 / 3", "Mitigation engaged", "Re-enabling Edge Shield token buckets, blocking attacker 429 floods while serving valid clients.", 90);
      await toggleShield("on");
      await Promise.all([
        fireTraffic({ count: 40, clientIp: "203.0.113.66" }),
        fireTraffic({ count: 8 }),
      ]);
      await fetchStats();
      await sleep(1800);

      if (storylineProgress) storylineProgress.style.width = "100%";
      updateStoryline("Complete", "DDoS mitigation verified", "Attacker bursts were throttled with HTTP 429 while valid traffic continued cleanly.", 100);
    } finally {
      isScenarioRunning = false;
      if (btnRunDdosScenario) btnRunDdosScenario.disabled = false;
    }
  }

  // Event Wiring

  railButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      setView(btn.getAttribute("data-nav"));
    });
  });

  document.querySelectorAll("[data-jump-view]").forEach((btn) => {
    btn.addEventListener("click", () => {
      setView(btn.getAttribute("data-jump-view"));
    });
  });

  document.querySelectorAll(".strategy-pill[data-strategy]").forEach((btn) => {
    btn.addEventListener("click", () => {
      setStrategy(btn.getAttribute("data-strategy"));
    });
  });

  chartTabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      chartMetric = tab.getAttribute("data-chart-metric") || "rps";
      chartTabs.forEach((t) => {
        const active = t === tab;
        t.classList.toggle("is-active", active);
        t.setAttribute("aria-selected", active ? "true" : "false");
      });
      renderTelemetryChart();
    });
  });

  traceFilterTabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      logFilter = tab.getAttribute("data-log-filter") || "all";
      traceFilterTabs.forEach((t) => {
        const active = t === tab;
        t.classList.toggle("is-active", active);
        t.setAttribute("aria-selected", active ? "true" : "false");
      });
      renderLogs(currentStats?.recent_logs || []);
    });
  });

  if (traceSearchInput) {
    traceSearchInput.addEventListener("input", (e) => {
      logSearchQuery = e.target.value || "";
      renderLogs(currentStats?.recent_logs || []);
    });
  }

  if (btnTestSingle) {
    btnTestSingle.addEventListener("click", async () => {
      await fireTraffic({ count: 1 });
      await fetchStats();
    });
  }

  if (btnTestBurst) {
    btnTestBurst.addEventListener("click", async () => {
      const count = Math.max(1, Math.min(500, parseInt(inputBurstCount?.value || "10", 10) || 10));
      btnTestBurst.disabled = true;
      try {
        await fireTraffic({ count });
        await fetchStats();
        showToast(`Dispatched ${count} concurrent requests`);
      } finally {
        btnTestBurst.disabled = false;
      }
    });
  }

  if (btnTestSlow) {
    btnTestSlow.addEventListener("click", async () => {
      const count = Math.max(1, Math.min(50, parseInt(inputHoldConns?.value || "5", 10) || 5));
      const sec = Math.max(1, Math.min(15, parseInt(inputHoldSec?.value || "4", 10) || 4));
      fireTraffic({ count, delay: sec * 1000 });
      showToast(`Holding ${count} sockets open for ${sec}s`);
      setTimeout(fetchStats, 250);
    });
  }

  if (btnAutoTraffic) {
    btnAutoTraffic.addEventListener("click", () => {
      if (autoTrafficTimer) {
        clearInterval(autoTrafficTimer);
        autoTrafficTimer = null;
        btnAutoTraffic.classList.remove("is-active");
        if (autoTrafficDot) autoTrafficDot.className = "status-dot muted";
        if (autoTrafficLabel) autoTrafficLabel.textContent = "Continuous off";
      } else {
        autoTrafficTimer = setInterval(() => {
          fireTraffic({ count: 2 }).catch(() => {});
        }, 1000);
        btnAutoTraffic.classList.add("is-active");
        if (autoTrafficDot) autoTrafficDot.className = "status-dot accent";
        if (autoTrafficLabel) autoTrafficLabel.textContent = "Continuous on";
        showToast("Continuous stream active (2 req/s)");
      }
    });
  }

  if (btnResetMetrics) {
    btnResetMetrics.addEventListener("click", async () => {
      const res = await fetch("/api/stats/reset", { method: "POST" });
      if (res.ok) {
        const fresh = seedInitialHistory();
        history.labels.splice(0, history.labels.length, ...fresh.labels);
        history.rps.splice(0, history.rps.length, ...fresh.rps);
        history.latency.splice(0, history.latency.length, ...fresh.latency);
        history.p95.splice(0, history.p95.length, ...fresh.p95);
        await fetchStats();
        showToast("Cluster metrics and request logs reset");
      }
    });
  }

  if (btnToggleShield) btnToggleShield.addEventListener("click", () => toggleShield());
  if (btnLabToggleShield) btnLabToggleShield.addEventListener("click", () => toggleShield());

  if (btnQuickCanary) {
    btnQuickCanary.addEventListener("click", async () => {
      await fireTraffic({ count: 1, canary: true });
      await fetchStats();
      showToast("Sent canary request (X-Canary: true)");
    });
  }

  if (btnQuickFlood) {
    btnQuickFlood.addEventListener("click", async () => {
      await fireTraffic({ count: 25, clientIp: "203.0.113.66" });
      await fetchStats();
      showToast("Sent 25 flood requests from 203.0.113.66");
    });
  }

  if (btnRunDdosScenario) btnRunDdosScenario.addEventListener("click", runDdosScenario);

  if (btnSpamAttacker) {
    btnSpamAttacker.addEventListener("click", async () => {
      await fireTraffic({ count: 35, clientIp: "203.0.113.66" });
      await fetchStats();
      showToast("Dispatched 35 flood requests from 203.0.113.66");
    });
  }

  if (btnSendNormalUsers) {
    btnSendNormalUsers.addEventListener("click", async () => {
      await fireTraffic({ count: 15 });
      await fetchStats();
      showToast("Dispatched 15 legitimate client requests");
    });
  }

  if (btnSendCanarySingle) {
    btnSendCanarySingle.addEventListener("click", async () => {
      await fireTraffic({ count: 1, canary: true });
      await fetchStats();
      showToast("Sent 1 canary request (X-Canary: true)");
    });
  }

  if (btnSendCanaryBurst) {
    btnSendCanaryBurst.addEventListener("click", async () => {
      await fireTraffic({ count: 10, canary: true });
      await fetchStats();
      showToast("Sent 10 canary requests");
    });
  }

  if (btnSetCanaryWeights) {
    btnSetCanaryWeights.addEventListener("click", async () => {
      const backends = currentStats?.backends || [];
      if (backends.length === 0) return;

      for (let i = 0; i < backends.length; i += 1) {
        const target = backends[i];
        const isLast = i === backends.length - 1;
        await fetch(`/api/backend/canary?url=${encodeURIComponent(target.url)}&enabled=${isLast}&tag=v2.0-canary`, { method: "POST" });
        await setBackendWeight(target.url, isLast ? 1 : 9);
      }
      await setStrategy("weighted_round_robin");
      await fetchStats();
      showToast("Applied 90/10 canary rollout split under Weighted RR");
    });
  }

  if (backendsContainer) {
    backendsContainer.addEventListener("click", (e) => {
      const target = e.target.closest("[data-action]");
      if (!target || target.tagName === "INPUT") return;

      const action = target.getAttribute("data-action");
      const url = target.getAttribute("data-url");
      if (!url) return;

      if (action === "toggle") toggleServer(url, target.getAttribute("data-state"));
      else if (action === "delay") setBackendDelay(url, Number(target.getAttribute("data-ms") || 0));
      else if (action === "weight") adjustBackendWeight(url, Number(target.getAttribute("data-delta") || 0));
      else if (action === "conn") adjustBackendConn(url, Number(target.getAttribute("data-delta") || 0));
    });

    backendsContainer.addEventListener("change", (e) => {
      const input = e.target;
      if (input.getAttribute("data-action") === "canary") {
        toggleCanary(input.getAttribute("data-url"), input.checked);
      }
    });
  }

  if (logsTbody) {
    logsTbody.addEventListener("click", (e) => {
      const btn = e.target.closest(".req-id-btn");
      if (!btn) return;
      const reqId = btn.getAttribute("data-copy-id");
      if (!reqId) return;
      if (navigator.clipboard) {
        navigator.clipboard.writeText(reqId).then(() => {
          showToast(`Copied Request ID: ${reqId}`);
        });
      }
    });
  }

  window.addEventListener("hashchange", syncViewFromHash);

  // Backwards-compatible globals
  window.toggleServer = toggleServer;
  window.setBackendDelay = setBackendDelay;
  window.adjustBackendConn = adjustBackendConn;
  window.adjustBackendWeight = adjustBackendWeight;
  window.toggleCanary = toggleCanary;

  syncViewFromHash();
  fetchStats();
  connectSSE();
});
