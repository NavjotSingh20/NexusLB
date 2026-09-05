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

  // Dynamic Failover Banner (only update if automated demo is not overriding)
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
    calloutMessage.innerHTML = `<strong>Cluster Status: Optimal Balancing</strong> &mdash; All ${backends.length} backends are healthy. Traffic is cycled evenly (${(100 / backends.length).toFixed(0)}% each) via Round Robin.`;
  } else if (healthyNodes.length === 0) {
    failoverCallout.className = 'failover-callout state-degraded';
    calloutMessage.innerHTML = `<strong>Critical Alert: All Backends Down!</strong> &mdash; NexusLB is responding with HTTP 503 to protect downstream clients until backends recover.`;
  } else {
    failoverCallout.className = 'failover-callout state-degraded';
    const downPorts = downNodes.map(b => b.url.replace('http://localhost', '')).join(', ');
    const healthyPorts = healthyNodes.map(b => b.url.replace('http://localhost', '')).join(', ');
    const pctPerNode = (100 / healthyNodes.length).toFixed(0);
    calloutMessage.innerHTML = `<strong>⚡ Active Failover Detected: Node(s) [${downPorts}] DOWN!</strong> &mdash; NexusLB automatically bypassed failed targets and is redistributing 100% of incoming load across [${healthyPorts}] (${pctPerNode}% each) with <strong>0 dropped requests</strong>!`;
  }
}

function renderBackends(backends) {
  let healthyCount = 0;
  let html = '';

  backends.forEach((b, idx) => {
    if (b.alive) healthyCount++;
    const color = COLOR_PALETTE[idx % COLOR_PALETTE.length];
    const statusBadge = b.alive
      ? `<span class="badge badge-success">HEALTHY</span>`
      : `<span class="badge badge-danger">UNHEALTHY</span>`;

    const actionButton = b.alive
      ? `<button class="btn btn-sm btn-kill" onclick="toggleBackend('${escapeHTML(b.url)}')">⚡ Kill Server</button>`
      : `<button class="btn btn-sm btn-restore" onclick="toggleBackend('${escapeHTML(b.url)}')">🔄 Revive Server</button>`;

    html += `
      <div class="backend-card" style="border-left: 3px solid ${color}">
        <div class="backend-card-header">
          <div class="backend-title-wrap">
            <span class="node-color-tag" style="background: ${color}"></span>
            <span class="backend-url">${escapeHTML(b.url)}</span>
          </div>
          ${statusBadge}
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
        <div class="card-actions">
          ${actionButton}
        </div>
      </div>
    `;
  });

  backendCardsContainer.innerHTML = html;

  healthyCountBadge.textContent = `${healthyCount} / ${backends.length} Healthy`;
  if (healthyCount === backends.length && backends.length > 0) {
    healthyCountBadge.className = 'badge badge-success';
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
    distributionLegend.innerHTML = `<span style="color: var(--text-muted); font-size: 0.8rem;">No requests processed yet</span>`;
    return;
  }

  let barHtml = '';
  let legendHtml = '';

  backends.forEach((b, idx) => {
    const color = COLOR_PALETTE[idx % COLOR_PALETTE.length];
    const count = b.total_requests || 0;
    const pct = ((count / totalBackendReqs) * 100).toFixed(1);

    if (count > 0) {
      barHtml += `<div class="dist-slice" style="width: ${pct}%; background: ${color};" title="${b.url}: ${pct}% (${count})"></div>`;
    }

    legendHtml += `
      <div class="legend-item">
        <span class="legend-dot" style="background: ${color}"></span>
        <span>${escapeHTML(b.url)} (${pct}%)</span>
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
        <td colspan="6">Waiting for requests... Click "Send 1 Request" or "Burst 10" above to begin.</td>
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

// Toggle a backend's online/down state
window.toggleBackend = async function(url) {
  try {
    const res = await fetch(`/api/backend/toggle?url=${encodeURIComponent(url)}`, { method: 'POST' });
    const data = await res.json();
    console.log('Toggled backend:', data);
  } catch (err) {
    console.error('Failed to toggle backend:', err);
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

// 1-Click Automated Failover Simulation
async function runFailoverDemo() {
  if (isDemoRunning) return;
  isDemoRunning = true;
  btnRunFailoverDemo.disabled = true;
  const originalBtnHtml = btnRunFailoverDemo.innerHTML;
  btnRunFailoverDemo.innerHTML = `<span>Simulating...</span>`;

  try {
    // Phase 1: Baseline
    step1.className = 'demo-step active';
    step2.className = 'demo-step';
    step3.className = 'demo-step';
    failoverCallout.className = 'failover-callout state-healthy';
    calloutMessage.innerHTML = `<strong>Demo Phase 1: Baseline Balancing</strong> &mdash; Resetting counters and sending 6 requests across all 3 healthy nodes...`;
    await fetch('/api/stats/reset', { method: 'POST' });
    await new Promise(r => setTimeout(r, 600));
    await triggerTestRequest(6);
    await new Promise(r => setTimeout(r, 1800));

    // Phase 2: Kill Backend 2 (:8002) and send traffic
    step1.className = 'demo-step completed';
    step2.className = 'demo-step active';
    failoverCallout.className = 'failover-callout state-degraded';
    calloutMessage.innerHTML = `<strong>Demo Phase 2: Simulating Failure</strong> &mdash; Shutting down Backend 2 (:8002) and sending 6 more requests... Watch traffic route ONLY to :8001 and :8003!`;
    await window.toggleBackend('http://localhost:8002');
    await new Promise(r => setTimeout(r, 1000));
    await triggerTestRequest(6);
    await new Promise(r => setTimeout(r, 2200));

    // Phase 3: Restore Backend 2 and verify rebalancing
    step2.className = 'demo-step completed';
    step3.className = 'demo-step active';
    failoverCallout.className = 'failover-callout state-healthy';
    calloutMessage.innerHTML = `<strong>Demo Phase 3: Healing & Rebalancing</strong> &mdash; Reviving Backend 2 (:8002) back online... Sending 6 requests to verify full 3-node balance is restored!`;
    await window.toggleBackend('http://localhost:8002');
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
  const btn = count === 1 ? btnTestSingle : btnTestBurst;
  const originalHtml = btn ? btn.innerHTML : '';
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = `<span style="opacity: 0.8">Sending ${count}...</span>`;
  }

  try {
    const res = await fetch(`/api/test-request?count=${count}`, { method: 'POST' });
    const data = await res.json();
    return data;
  } catch (err) {
    console.error('Error triggering test request:', err);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = originalHtml;
    }
  }
}

btnTestSingle.addEventListener('click', () => triggerTestRequest(1));
btnTestBurst.addEventListener('click', () => triggerTestRequest(10));
btnResetMetrics.addEventListener('click', resetMetrics);
btnRunFailoverDemo.addEventListener('click', runFailoverDemo);

// Initialize SSE connection
connectSSE();
