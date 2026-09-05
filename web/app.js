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

  // Traffic Distribution
  renderDistribution(stats.backends || [], total);

  // Request Logs
  renderLogs(stats.recent_logs || []);
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
      </div>
    `;
  });

  backendCardsContainer.innerHTML = html;

  healthyCountBadge.textContent = `${healthyCount} / ${backends.length} Healthy`;
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
  const originalHtml = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = `<span style="opacity: 0.8">Sending ${count}...</span>`;

  try {
    const res = await fetch(`/api/test-request?count=${count}`, { method: 'POST' });
    const data = await res.json();
    console.log('Test requests result:', data);
  } catch (err) {
    console.error('Error triggering test request:', err);
  } finally {
    btn.disabled = false;
    btn.innerHTML = originalHtml;
  }
}

btnTestSingle.addEventListener('click', () => triggerTestRequest(1));
btnTestBurst.addEventListener('click', () => triggerTestRequest(10));

// Initialize SSE connection
connectSSE();
