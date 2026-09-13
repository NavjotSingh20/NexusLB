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
let autoTrafficTimer = null;
const backendDelays = {};

// DOM Elements
const uptimeVal = document.getElementById('uptime-val');
const strategyDisplay = document.getElementById('active-strategy-display');
const proxyUrlDisplay = document.getElementById('proxy-url-display');

const kpiTotalRequests = document.getElementById('kpi-total-requests');
const kpiRps = document.getElementById('kpi-rps');
const kpiLatency = document.getElementById('kpi-latency');
const kpiSuccessRate = document.getElementById('kpi-success-rate');
const kpiQueued = document.getElementById('kpi-queued');
const kpiCardQueue = document.getElementById('kpi-card-queue');

const healthyCountBadge = document.getElementById('healthy-count-badge');
const backendCardsContainer = document.getElementById('backend-cards-container');
const distributionBar = document.getElementById('distribution-bar');
const distributionLegend = document.getElementById('distribution-legend');
const distributionTotalLabel = document.getElementById('distribution-total-label');

const logsTableBody = document.getElementById('logs-table-body');
const logCountBadge = document.getElementById('log-count-badge');

const btnTestSingle = document.getElementById('btn-test-single');
const btnTestBurst = document.getElementById('btn-test-burst');
const burstCountInput = document.getElementById('burst-count-input');
const checkSimIPs = document.getElementById('check-sim-ips');
const btnAutoTraffic = document.getElementById('btn-auto-traffic');
const autoTrafficLabel = document.getElementById('auto-traffic-label');
const btnResetMetrics = document.getElementById('btn-reset-metrics');

// Update active strategy pill styling
function updateStrategyPills(key) {
  if (!key) return;
  const pills = document.querySelectorAll('.strategy-pill');
  pills.forEach(pill => {
    if (pill.getAttribute('data-strategy') === key) {
      pill.classList.add('active');
    } else {
      pill.classList.remove('active');
    }
  });
}

// Hook strategy switcher buttons
document.querySelectorAll('.strategy-pill').forEach(pill => {
  pill.addEventListener('click', async () => {
    const strat = pill.getAttribute('data-strategy');
    if (!strat) return;
    updateStrategyPills(strat);
    try {
      const res = await fetch(`/api/strategy?name=${encodeURIComponent(strat)}`, { method: 'POST' });
      const data = await res.json();
      if (data.key) {
        updateStrategyPills(data.key);
      }
    } catch (err) {
      console.error('Failed to change strategy:', err);
    }
  });
});

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
  if (stats.strategy_key) {
    updateStrategyPills(stats.strategy_key);
  }
  if (stats.strategy && strategyDisplay) {
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

  // Queued Requests
  const queued = stats.queued_requests || 0;
  if (kpiQueued) {
    kpiQueued.textContent = queued.toLocaleString();
    if (kpiCardQueue) {
      if (queued > 0) {
        kpiCardQueue.classList.add('queue-active');
      } else {
        kpiCardQueue.classList.remove('queue-active');
      }
    }
  }

  // Upstream Backends
  renderBackends(stats.backends || []);

  // Traffic Distribution
  renderDistribution(stats.backends || [], total);

  // Request Logs
  renderLogs(stats.recent_logs || []);
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
          <div class="stat-item active-conn-stat">
            <div class="stat-header">
              <span class="stat-label">Active Conn</span>
              <div class="conn-steppers">
                <button class="btn-stepper" onclick="window.adjustBackendConn('${escapeHTML(b.url)}', -1)" title="Decrease active connections by 1">-</button>
                <button class="btn-stepper" onclick="window.adjustBackendConn('${escapeHTML(b.url)}', 1)" title="Increase active connections by 1">+</button>
                <button class="btn-stepper btn-stepper-reset" onclick="window.setBackendConn('${escapeHTML(b.url)}', 0)" title="Reset active connections to 0">0</button>
              </div>
            </div>
            <span class="stat-val ${b.active_connections > 0 ? 'highlight-conn' : ''}">${b.active_connections}</span>
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

        <div class="backend-delay-bar">
          <span class="delay-label">Latency Sim:</span>
          <div class="delay-buttons">
            <button class="btn-delay-preset ${(!backendDelays[b.url] || backendDelays[b.url] === 0) ? 'active' : ''}" onclick="window.setBackendDelay('${escapeHTML(b.url)}', 0)">0ms</button>
            <button class="btn-delay-preset ${backendDelays[b.url] === 100 ? 'active' : ''}" onclick="window.setBackendDelay('${escapeHTML(b.url)}', 100)">100ms</button>
            <button class="btn-delay-preset ${backendDelays[b.url] === 300 ? 'active' : ''}" onclick="window.setBackendDelay('${escapeHTML(b.url)}', 300)">300ms</button>
            <button class="btn-delay-preset ${backendDelays[b.url] === 600 ? 'active' : ''}" onclick="window.setBackendDelay('${escapeHTML(b.url)}', 600)">600ms</button>
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
        <td colspan="6">Waiting for requests... Start Continuous Traffic or click "Send Burst" above.</td>
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

// Set simulated latency on a backend server
window.setBackendDelay = async function(url, ms) {
  backendDelays[url] = ms;
  try {
    const res = await fetch(`/api/backend/delay?url=${encodeURIComponent(url)}&ms=${ms}`, { method: 'POST' });
    const data = await res.json();
    console.log(`Server ${url} latency set to ${ms}ms:`, data);
    if (lastStats && lastStats.backends) {
      renderBackends(lastStats.backends);
    }
  } catch (err) {
    console.error(`Failed to set latency for ${url}:`, err);
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

// Connect to Server-Sent Events stream
function connectSSE() {
  if (eventSource) {
    eventSource.close();
  }

  eventSource = new EventSource('/api/stream');

  eventSource.onopen = () => {
    // Connected to stream
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
    eventSource.close();
    setTimeout(connectSSE, 2000);
  };
}

// Quick trigger helper
async function triggerTestRequest(count = 1) {
  // Optimistically reflect queued request count immediately
  if (kpiQueued) {
    const current = parseInt(kpiQueued.textContent.replace(/,/g, ''), 10) || 0;
    const newQueued = current + count;
    kpiQueued.textContent = newQueued.toLocaleString();
    if (kpiCardQueue) kpiCardQueue.classList.add('queue-active');
  }

  const simIPs = checkSimIPs ? checkSimIPs.checked : true;
  try {
    const res = await fetch(`/api/test-request?count=${count}&sim_ips=${simIPs}`, { method: 'POST' });
    const data = await res.json();
    return data;
  } catch (err) {
    console.error('Error triggering test request:', err);
  }
}

btnTestSingle.addEventListener('click', () => triggerTestRequest(1));
btnTestBurst.addEventListener('click', () => {
  let count = parseInt(burstCountInput ? burstCountInput.value : 10, 10);
  if (isNaN(count) || count <= 0) {
    count = 10;
  }
  if (count > 2000) {
    count = 2000;
  }
  triggerTestRequest(count);
});

// Adjust active connections manually on a backend server
window.adjustBackendConn = async function(url, delta) {
  try {
    await fetch(`/api/backend/connections?url=${encodeURIComponent(url)}&delta=${delta}`, { method: 'POST' });
  } catch (err) {
    console.error(`Failed to adjust connections for ${url}:`, err);
  }
};

window.setBackendConn = async function(url, count) {
  try {
    await fetch(`/api/backend/connections?url=${encodeURIComponent(url)}&count=${count}`, { method: 'POST' });
  } catch (err) {
    console.error(`Failed to set connections for ${url}:`, err);
  }
};

const btnTestSlow = document.getElementById('btn-test-slow');
const holdConnsCount = document.getElementById('hold-conns-count');
const holdConnsSec = document.getElementById('hold-conns-sec');

if (btnTestSlow) {
  btnTestSlow.addEventListener('click', async () => {
    let count = parseInt(holdConnsCount ? holdConnsCount.value : 15, 10) || 15;
    let sec = parseInt(holdConnsSec ? holdConnsSec.value : 4, 10) || 4;
    if (count > 150) count = 150;
    if (count < 1) count = 1;
    if (sec > 30) sec = 30;
    if (sec < 1) sec = 1;

    const delayMs = sec * 1000;
    btnTestSlow.disabled = true;
    const origHtml = btnTestSlow.innerHTML;
    btnTestSlow.innerHTML = `<span>Holding ${count} Conns (${sec}s)...</span>`;
    try {
      await fetch(`/api/test-request?count=${count}&delay=${delayMs}`, { method: 'POST' });
    } catch (err) {
      console.error('Error triggering custom hold requests:', err);
    } finally {
      setTimeout(() => {
        btnTestSlow.disabled = false;
        btnTestSlow.innerHTML = origHtml;
      }, delayMs);
    }
  });
}

btnAutoTraffic.addEventListener('click', toggleAutoTraffic);
btnResetMetrics.addEventListener('click', resetMetrics);

// Initialize SSE connection
connectSSE();
