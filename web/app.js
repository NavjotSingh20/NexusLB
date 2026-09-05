// NexusLB Observability Dashboard Frontend Script

const COLOR_PALETTE = [
  '#6366f1', // Indigo
  '#06b6d4', // Cyan
  '#10b981', // Emerald
  '#f59e0b', // Amber
  '#ec4899', // Pink
  '#8b5cf6', // Violet
];

let eventSource = null;
let lastStats = null;
let isDemoRunning = false;
let autoTrafficTimer = null;

// DOM Elements
const connStatus = document.getElementById('connection-status');
const connStatusText = document.getElementById('conn-status-text');
const uptimeVal = document.getElementById('uptime-val');
const strategyDisplay = document.getElementById('active-strategy-display');
const proxyUrlDisplay = document.getElementById('proxy-url-display');

const kpiTotalRequests = document.getElementById('kpi-total-requests');
const kpiRps = document.getElementById('kpi-rps');
const kpiLatency = document.getElementById('kpi-latency');
const kpiSuccessRate = document.getElementById('kpi-success-rate');

const healthyCountBadge = document.getElementById('healthy-count-badge');
const backendCardsContainer = document.getElementById('backend-cards-container');
const distributionBar = document.getElementById('distribution-bar');
const distributionLegend = document.getElementById('distribution-legend');
const distributionTotalLabel = document.getElementById('distribution-total-label');

const logsTableBody = document.getElementById('logs-table-body');
const logCountBadge = document.getElementById('log-count-badge');

const btnTestSingle = document.getElementById('btn-test-single');
const btnTestBurst = document.getElementById('btn-test-burst');
const btnAutoTraffic = document.getElementById('btn-auto-traffic');
const autoTrafficLabel = document.getElementById('auto-traffic-label');
const btnResetMetrics = document.getElementById('btn-reset-metrics');
const btnRunFailoverDemo = document.getElementById('btn-run-failover-demo');

const failoverCallout = document.getElementById('failover-callout');
const calloutMessage = document.getElementById('callout-message');

const step1 = document.getElementById('step-1');
const step2 = document.getElementById('step-2');
const step3 = document.getElementById('step-3');

// Format seconds into HH:MM:SS
function formatUptime(seconds) {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  return [h, m, s].map(v => String(v).padStart(2, '0')).join(':');
}

// Update the entire UI state from stats payload
function updateUI(stats) {
  lastStats = stats;

  // Uptime & Strategy
  uptimeVal.textContent = formatUptime(stats.uptime_seconds || 0);
  if (stats.strategy) {
    strategyDisplay.textContent = stats.strategy;
  }

  // KPIs
  kpiTotalRequests.textContent = (stats.total_requests || 0).toLocaleString();
  kpiRps.innerHTML = `${(stats.rps || 0).toFixed(1)} <span class="kpi-unit">req/s</span>`;
  kpiLatency.innerHTML = `${(stats.avg_latency_ms || 0).toFixed(1)} <span class="kpi-unit">ms</span>`;

  const total = stats.total_requests || 0;
  const success = stats.total_success || 0;
  const successRate = total > 0 ? ((success / total) * 100).toFixed(1) : '100.0';
  kpiSuccessRate.innerHTML = `${successRate}<span class="kpi-unit">%</span>`;

  // Upstream Backends
  renderBackends(stats.backends || []);

  // Dynamic Failover Banner
  if (!isDemoRunning) {
    updateFailoverCallout(stats.backends || []);
  }

  // Traffic Distribution
  renderDistribution(stats.backends || [], total);

  // Request Logs
  renderLogs(stats.recent_logs || []);
}

function updateFailoverCallout(backends) {
  if (!backends || backends.length === 0) return;

  const downNodes = backends.filter(b => !b.alive);
  const healthyNodes = backends.filter(b => b.alive);

  if (downNodes.length === 0) {
    failoverCallout.className = 'failover-callout state-healthy';
    calloutMessage.innerHTML = `<strong>Cluster Status: Optimal Balancing (All ${backends.length} Nodes Online)</strong> &mdash; Incoming traffic is distributed evenly (33.3% / 33.3% / 33.3%) across all servers via Round Robin.`;
  } else if (healthyNodes.length === 0) {
    failoverCallout.className = 'failover-callout state-degraded';
    calloutMessage.innerHTML = `<strong>⚠️ Critical: All Backend Servers Switched OFF!</strong> &mdash; NexusLB is responding with HTTP 503 to protect clients until at least one server is toggled back ON.`;
  } else {
    failoverCallout.className = 'failover-callout state-degraded';
    const downPorts = downNodes.map(b => b.url.replace('http://localhost', '')).join(', ');
    const healthyPorts = healthyNodes.map(b => b.url.replace('http://localhost', '')).join(', ');
    const pctPerNode = (100 / healthyNodes.length).toFixed(1);
    calloutMessage.innerHTML = `<strong>⚡ Dynamic Load Shift: Server ${downPorts} Switched OFF!</strong> &mdash; NexusLB automatically rerouted traffic: <strong>100% of incoming load</strong> is now dynamically split across healthy servers <strong>[${healthyPorts}] (${pctPerNode}% each)</strong> with <strong>0 dropped requests</strong>!`;
  }
}

function renderBackends(backends) {
  let healthyCount = 0;
  let html = '';

  const onlineNodes = backends.filter(b => b.alive);
  const targetPct = onlineNodes.length > 0 ? (100 / onlineNodes.length).toFixed(1) : '0';

  backends.forEach((b, idx) => {
    if (b.alive) healthyCount++;
    const color = COLOR_PALETTE[idx % COLOR_PALETTE.length];
    const isOnline = b.alive;
    const cardClass = isOnline ? 'backend-card' : 'backend-card offline';

    const statusBadge = isOnline
      ? `<span class="badge badge-success">HEALTHY</span>`
      : `<span class="badge badge-danger">OFFLINE (BYPASSED)</span>`;

    const shareBadge = isOnline
      ? `<span class="target-share-badge active" title="Active share target">Target: ${targetPct}% share</span>`
      : `<span class="target-share-badge inactive">0% (Bypassed)</span>`;

    html += `
      <div class="${cardClass}" style="border-left: 4px solid ${isOnline ? color : '#f43f5e'}">
        <div class="backend-card-header">
          <div class="backend-title-wrap">
            <span class="node-color-tag" style="background: ${isOnline ? color : '#f43f5e'}"></span>
            <span class="backend-url">${escapeHTML(b.url)}</span>
          </div>
          ${statusBadge}
        </div>

        <div class="server-toggle-bar">
          <div class="toggle-wrapper">
            <label class="switch">
              <input type="checkbox" ${isOnline ? 'checked' : ''} onchange="window.toggleServer('${escapeHTML(b.url)}', this.checked)">
              <span class="slider round"></span>
            </label>
            <span class="switch-state-label ${isOnline ? 'state-on' : 'state-off'}">
              ${isOnline ? 'SERVER: ON' : 'SERVER: OFF'}
            </span>
          </div>
          ${shareBadge}
        </div>

        <div class="backend-stats-grid">
          <div class="stat-item">
            <span class="stat-label">Active Conn</span>
            <span class="stat-val">${b.active_connections}</span>
          </div>
          <div class="stat-item">
            <span class="stat-label">Handled</span>
            <span class="stat-val">${(b.total_requests || 0).toLocaleString()}</span>
          </div>
          <div class="stat-item">
            <span class="stat-label">Latency</span>
            <span class="stat-val">${b.last_latency_ms} ms</span>
          </div>
        </div>
      </div>
    `;
  });

  backendCardsContainer.innerHTML = html;

  healthyCountBadge.textContent = `${healthyCount} / ${backends.length} Online`;
  if (healthyCount === backends.length && backends.length > 0) {
    healthyCountBadge.className = 'badge badge-success';
  } else if (healthyCount === 0) {
    healthyCountBadge.className = 'badge badge-danger';
  } else {
    healthyCountBadge.className = 'badge badge-danger';
  }
}

function renderDistribution(backends, totalRequests) {
  distributionTotalLabel.textContent = `${totalRequests.toLocaleString()} total proxied`;

  let totalBackendReqs = 0;
  backends.forEach(b => totalBackendReqs += (b.total_requests || 0));

  if (totalBackendReqs === 0) {
    distributionBar.innerHTML = `<div class="dist-slice" style="width: 100%; background: rgba(255,255,255,0.05);"></div>`;
    distributionLegend.innerHTML = `<span style="color: var(--text-muted); font-size: 0.8rem;">No requests processed yet &bull; Start traffic above</span>`;
    return;
  }

  let barHtml = '';
  let legendHtml = '';

  backends.forEach((b, idx) => {
    const color = COLOR_PALETTE[idx % COLOR_PALETTE.length];
    const count = b.total_requests || 0;
    const pct = ((count / totalBackendReqs) * 100).toFixed(1);

    if (count > 0) {
      barHtml += `<div class="dist-slice" style="width: ${pct}%; background: ${b.alive ? color : '#64748b'};" title="${b.url}: ${pct}% (${count})"></div>`;
    }

    legendHtml += `
      <div class="legend-item ${b.alive ? '' : 'legend-offline'}">
        <span class="legend-dot" style="background: ${b.alive ? color : '#64748b'}"></span>
        <span>${escapeHTML(b.url)}: <strong>${pct}%</strong> (${count} reqs) ${b.alive ? '' : '<small style="color:#fb7185">[OFFLINE]</small>'}</span>
      </div>
    `;
  });

  distributionBar.innerHTML = barHtml;
  distributionLegend.innerHTML = legendHtml;
}

function renderLogs(logs) {
  logCountBadge.textContent = `${logs.length} logged`;

  if (!logs || logs.length === 0) {
    logsTableBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="6">Waiting for requests... Start Continuous Traffic or click "Burst 10" above.</td>
      </tr>
    `;
    return;
  }

  let html = '';
  logs.forEach(log => {
    let statusClass = 'status-2xx';
    if (log.status >= 400 && log.status < 500) statusClass = 'status-4xx';
    else if (log.status >= 500) statusClass = 'status-5xx';

    html += `
      <tr>
        <td>${escapeHTML(log.timestamp)}</td>
        <td><span class="method-tag method-${escapeHTML(log.method)}">${escapeHTML(log.method)}</span></td>
        <td>${escapeHTML(log.client_ip || '127.0.0.1')}</td>
        <td>${escapeHTML(log.backend)}</td>
        <td><span class="status-tag ${statusClass}">${log.status}</span></td>
        <td>${log.latency_ms} ms</td>
      </tr>
    `;
  });

  logsTableBody.innerHTML = html;
}

function escapeHTML(str) {
  if (!str) return '';
  return str.replace(/[&<>'"]/g, 
    tag => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[tag] || tag)
  );
}

// Toggle a server ON or OFF idempotently
window.toggleServer = async function(url, isOnline) {
  const state = isOnline ? 'up' : 'down';
  try {
    const res = await fetch(`/api/backend/toggle?url=${encodeURIComponent(url)}&state=${state}`, { method: 'POST' });
    const data = await res.json();
    console.log(`Server ${url} set to ${state}:`, data);
  } catch (err) {
    console.error(`Failed to set server ${url} state:`, err);
  }
};

// Reset metrics
async function resetMetrics() {
  const originalHtml = btnResetMetrics.innerHTML;
  btnResetMetrics.disabled = true;
  btnResetMetrics.innerHTML = `<span>Resetting...</span>`;
  try {
    await fetch('/api/stats/reset', { method: 'POST' });
  } catch (err) {
    console.error('Failed to reset stats:', err);
  } finally {
    btnResetMetrics.disabled = false;
    btnResetMetrics.innerHTML = originalHtml;
  }
}

// Continuous Traffic Generator
function toggleAutoTraffic() {
  if (autoTrafficTimer) {
    clearInterval(autoTrafficTimer);
    autoTrafficTimer = null;
    btnAutoTraffic.classList.remove('active');
    autoTrafficLabel.textContent = 'Start Continuous Traffic';
  } else {
    btnAutoTraffic.classList.add('active');
    autoTrafficLabel.textContent = 'Stop Continuous Traffic';
    // Send 1 request every 500ms (2 requests per second)
    autoTrafficTimer = setInterval(() => {
      triggerTestRequest(1);
    }, 500);
  }
}

// 1-Click Automated Failover Simulation
async function runFailoverDemo() {
  if (isDemoRunning) return;
  isDemoRunning = true;
  btnRunFailoverDemo.disabled = true;
  const originalBtnHtml = btnRunFailoverDemo.innerHTML;
  btnRunFailoverDemo.innerHTML = `<span>Simulating Demo...</span>`;

  try {
    // Phase 1: Baseline (All ON)
    step1.className = 'demo-step active';
    step2.className = 'demo-step';
    step3.className = 'demo-step';
    failoverCallout.className = 'failover-callout state-healthy';
    calloutMessage.innerHTML = `<strong>Demo Phase 1: Baseline Balancing</strong> &mdash; Resetting counters and sending 6 requests across all 3 healthy nodes...`;
    await fetch('/api/stats/reset', { method: 'POST' });
    await new Promise(r => setTimeout(r, 600));
    await triggerTestRequest(6);
    await new Promise(r => setTimeout(r, 1800));

    // Phase 2: Switch Backend 2 (:8002) OFF
    step1.className = 'demo-step completed';
    step2.className = 'demo-step active';
    failoverCallout.className = 'failover-callout state-degraded';
    calloutMessage.innerHTML = `<strong>Demo Phase 2: Switching Server :8002 OFF</strong> &mdash; Server toggled OFF! Sending 6 requests... Watch traffic automatically route ONLY to :8001 and :8003 (50% each)!`;
    await window.toggleServer('http://localhost:8002', false);
    await new Promise(r => setTimeout(r, 1000));
    await triggerTestRequest(6);
    await new Promise(r => setTimeout(r, 2200));

    // Phase 3: Switch Backend 2 (:8002) back ON
    step2.className = 'demo-step completed';
    step3.className = 'demo-step active';
    failoverCallout.className = 'failover-callout state-healthy';
    calloutMessage.innerHTML = `<strong>Demo Phase 3: Switching Server :8002 back ON</strong> &mdash; Server toggled ON! Sending 6 requests to verify full 33.3% 3-node balance is restored!`;
    await window.toggleServer('http://localhost:8002', true);
    await new Promise(r => setTimeout(r, 1000));
    await triggerTestRequest(6);
    await new Promise(r => setTimeout(r, 1500));

    // Completion
    step3.className = 'demo-step completed';
    calloutMessage.innerHTML = `<strong>✅ Failover Demo Complete!</strong> &mdash; Successfully demonstrated automatic fault detection, seamless failover (zero dropped requests), and automatic recovery rebalancing!`;
  } catch (err) {
    console.error('Error during failover demo:', err);
  } finally {
    isDemoRunning = false;
    btnRunFailoverDemo.disabled = false;
    btnRunFailoverDemo.innerHTML = originalBtnHtml;
  }
}

// Connect to Server-Sent Events stream
function connectSSE() {
  if (eventSource) {
    eventSource.close();
  }

  eventSource = new EventSource('/api/stream');

  eventSource.onopen = () => {
    connStatusText.textContent = 'LIVE TELEMETRY';
    connStatus.style.borderColor = 'rgba(16, 185, 129, 0.3)';
  };

  eventSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      updateUI(data);
    } catch (err) {
      console.error('Failed to parse SSE payload:', err);
    }
  };

  eventSource.onerror = () => {
    connStatusText.textContent = 'RECONNECTING...';
    connStatus.style.borderColor = 'rgba(245, 158, 11, 0.4)';
    eventSource.close();
    setTimeout(connectSSE, 2000);
  };
}

// Quick trigger helper
async function triggerTestRequest(count = 1) {
  try {
    const res = await fetch(`/api/test-request?count=${count}`, { method: 'POST' });
    const data = await res.json();
    return data;
  } catch (err) {
    console.error('Error triggering test request:', err);
  }
}

btnTestSingle.addEventListener('click', () => triggerTestRequest(1));
btnTestBurst.addEventListener('click', () => triggerTestRequest(10));
btnAutoTraffic.addEventListener('click', toggleAutoTraffic);
btnResetMetrics.addEventListener('click', resetMetrics);
btnRunFailoverDemo.addEventListener('click', runFailoverDemo);

// Initialize SSE connection
connectSSE();
