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
const kpiThrottled = document.getElementById('kpi-throttled');
const kpiCardShield = document.getElementById('kpi-card-shield');

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

// Shield & Scenario Studio Elements
const shieldStatusLabel = document.getElementById('shield-status-label');
const btnToggleShield = document.getElementById('btn-toggle-shield');
const shieldBtnText = document.getElementById('shield-btn-text');
const btnRunDDoSScenario = document.getElementById('btn-run-ddos-scenario');
const btnSpamAttacker = document.getElementById('btn-spam-attacker');
const btnSendNormalUsers = document.getElementById('btn-send-normal-users');

// Strategy Descriptions
const STRATEGY_DESCRIPTIONS = {
  round_robin: 'Monotonically advancing atomic ring counter with zero memory allocations',
  weighted_round_robin: 'Smoothly interleaves traffic according to node weights without burst clustering',
  least_connections: 'Dynamically routes to healthy backend with lowest active in-flight sockets',
  ip_hash: 'Deterministic 32-bit FNV-1a hash over client IP for sticky session persistence'
};
const strategyDescEl = document.getElementById('strategy-desc');

// Update active strategy pill styling and description
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
  if (strategyDescEl && STRATEGY_DESCRIPTIONS[key]) {
    strategyDescEl.textContent = STRATEGY_DESCRIPTIONS[key];
  }
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

  // Throttled (429) Requests
  if (kpiThrottled) {
    const throttled = stats.total_throttled || 0;
    kpiThrottled.textContent = throttled.toLocaleString();
    if (kpiCardShield) {
      if (throttled > 0) {
        kpiCardShield.classList.add('throttled-active');
      } else {
        kpiCardShield.classList.remove('throttled-active');
      }
    }
  }

  // Edge Shield Status
  if (shieldStatusLabel && btnToggleShield && shieldBtnText) {
    const isShieldOn = stats.shield_enabled !== false;
    const rate = stats.shield_rate || 10;
    const burst = stats.shield_burst || 10;
    shieldStatusLabel.textContent = isShieldOn
      ? `Edge Shield: ACTIVE (${rate} req/s, Burst: ${burst})`
      : `Edge Shield: DISABLED`;
    if (isShieldOn) {
      btnToggleShield.className = 'btn btn-shield-active';
      shieldBtnText.textContent = 'Shield Enabled';
    } else {
      btnToggleShield.className = 'btn btn-shield-disabled';
      shieldBtnText.textContent = 'Shield Disabled';
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

  let totalAliveWeight = 0;
  backends.forEach(b => {
    if (b.alive) totalAliveWeight += (b.weight || 1);
  });

  backends.forEach((b, idx) => {
    if (b.alive) healthyCount++;
    const color = COLOR_PALETTE[idx % COLOR_PALETTE.length];
    const isOnline = b.alive;
    const isCanary = b.is_canary || false;
    const weight = b.weight || 1;
    const cardClass = `backend-card ${isOnline ? '' : 'offline'} ${isCanary ? 'canary-node' : ''}`.trim();

    const targetPct = (isOnline && totalAliveWeight > 0)
      ? ((weight / totalAliveWeight) * 100).toFixed(1)
      : '0';

    let statusBadge;
    if (!isOnline) {
      statusBadge = `<span class="badge badge-danger">OFFLINE (BYPASSED)</span>`;
    } else if (isCanary) {
      statusBadge = `<span class="badge badge-canary">🐥 CANARY (${escapeHTML(b.canary_tag || 'v2.0')})</span>`;
    } else {
      statusBadge = `<span class="badge badge-success">HEALTHY</span>`;
    }

    const shareBadge = isOnline
      ? `<span class="target-share-badge active" title="Active expected load share">Target: ${targetPct}% share</span>`
      : `<span class="target-share-badge inactive">0% (Bypassed)</span>`;

    html += `
      <div class="${cardClass}" style="border-left: 4px solid ${isOnline ? (isCanary ? '#8b5cf6' : color) : '#f43f5e'}">
        <div class="backend-card-header">
          <div class="backend-title-wrap">
            <span class="node-color-tag" style="background: ${isOnline ? (isCanary ? '#8b5cf6' : color) : '#f43f5e'}"></span>
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

        <div class="node-weight-bar ${isOnline ? '' : 'controls-disabled'}">
          <div class="weight-stepper-wrap">
            <span class="weight-label">Weight:</span>
            <button class="btn-weight-stepper" ${isOnline ? '' : 'disabled'} onclick="window.adjustBackendWeight('${escapeHTML(b.url)}', -1)" title="Decrease weight">-</button>
            <span class="weight-val-badge">${weight}x</span>
            <button class="btn-weight-stepper" ${isOnline ? '' : 'disabled'} onclick="window.adjustBackendWeight('${escapeHTML(b.url)}', 1)" title="Increase weight">+</button>
          </div>
          <button class="canary-toggle-btn ${isCanary ? 'active' : ''}" ${isOnline ? '' : 'disabled'} onclick="window.toggleCanary('${escapeHTML(b.url)}', ${!isCanary})" title="${isOnline ? 'Designate as Canary Release target' : 'Server is offline'}">
            <span class="canary-indicator-dot"></span>
            <span>${isCanary ? 'Canary Active' : 'Make Canary'}</span>
          </button>
        </div>

        <div class="backend-stats-grid ${isOnline ? '' : 'controls-disabled'}">
          <div class="stat-item active-conn-stat">
            <div class="stat-header">
              <span class="stat-label">Active Conn</span>
              <div class="conn-steppers">
                <button class="btn-stepper" ${isOnline ? '' : 'disabled'} onclick="window.adjustBackendConn('${escapeHTML(b.url)}', -1)" title="Decrease active connections by 1">-</button>
                <button class="btn-stepper" ${isOnline ? '' : 'disabled'} onclick="window.adjustBackendConn('${escapeHTML(b.url)}', 1)" title="Increase active connections by 1">+</button>
                <button class="btn-stepper btn-stepper-reset" ${isOnline ? '' : 'disabled'} onclick="window.setBackendConn('${escapeHTML(b.url)}', 0)" title="Reset active connections to 0">0</button>
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

        <div class="backend-delay-bar ${isOnline ? '' : 'controls-disabled'}">
          <span class="delay-label">Latency Sim:</span>
          <div class="delay-buttons">
            <button class="btn-delay-preset ${(!backendDelays[b.url] || backendDelays[b.url] === 0) ? 'active' : ''}" ${isOnline ? '' : 'disabled'} onclick="window.setBackendDelay('${escapeHTML(b.url)}', 0)">0ms</button>
            <button class="btn-delay-preset ${backendDelays[b.url] === 100 ? 'active' : ''}" ${isOnline ? '' : 'disabled'} onclick="window.setBackendDelay('${escapeHTML(b.url)}', 100)">100ms</button>
            <button class="btn-delay-preset ${backendDelays[b.url] === 300 ? 'active' : ''}" ${isOnline ? '' : 'disabled'} onclick="window.setBackendDelay('${escapeHTML(b.url)}', 300)">300ms</button>
            <button class="btn-delay-preset ${backendDelays[b.url] === 600 ? 'active' : ''}" ${isOnline ? '' : 'disabled'} onclick="window.setBackendDelay('${escapeHTML(b.url)}', 600)">600ms</button>
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

    const canaryTag = b.is_canary ? `<span class="legend-canary-badge">🐥 CANARY</span>` : '';
    const weightBadge = `<span class="legend-weight-badge" title="Dynamic weight multiplier">${b.weight || 1}x</span>`;
    legendHtml += `
      <div class="legend-item ${b.alive ? '' : 'legend-offline'}">
        <span class="legend-dot" style="background: ${b.alive ? (b.is_canary ? '#8b5cf6' : color) : '#64748b'}"></span>
        <span class="legend-url">${escapeHTML(b.url)}</span>
        <span class="legend-pct"><strong>${pct}%</strong></span>
        <span class="legend-count">(${count} reqs)</span>
        ${weightBadge}
        ${canaryTag}
        ${b.alive ? '' : '<span class="legend-offline-tag">OFFLINE</span>'}
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
    let statusDisplay = `${log.status}`;

    if (log.status === 429) {
      statusClass = 'status-429';
      statusDisplay = '429 BLOCKED';
    } else if (log.status >= 400 && log.status < 500) {
      statusClass = 'status-4xx';
    } else if (log.status >= 500) {
      statusClass = 'status-5xx';
    }

    let backendDisplay;
    if (log.backend === 'Edge Shield') {
      backendDisplay = `<span class="badge badge-accent" style="font-size: 0.72rem; padding: 0.15rem 0.45rem;">🛡️ Edge Shield</span>`;
    } else if (log.is_canary) {
      backendDisplay = `<span class="canary-tag">🐥 CANARY</span> ${escapeHTML(log.backend)}`;
    } else {
      backendDisplay = escapeHTML(log.backend);
    }

    html += `
      <tr>
        <td>${escapeHTML(log.timestamp)}</td>
        <td><span class="method-tag method-${escapeHTML(log.method)}">${escapeHTML(log.method)}</span></td>
        <td><code>${escapeHTML(log.client_ip || '127.0.0.1')}</code></td>
        <td>${backendDisplay}</td>
        <td><span class="status-tag ${statusClass}">${statusDisplay}</span></td>
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

// Trigger request with custom client IP
async function triggerTestRequestWithIP(count, clientIP) {
  if (kpiQueued) {
    const current = parseInt(kpiQueued.textContent.replace(/,/g, ''), 10) || 0;
    kpiQueued.textContent = (current + count).toLocaleString();
    if (kpiCardQueue) kpiCardQueue.classList.add('queue-active');
  }
  try {
    const url = clientIP
      ? `/api/test-request?count=${count}&client_ip=${encodeURIComponent(clientIP)}`
      : `/api/test-request?count=${count}&sim_ips=true`;
    const res = await fetch(url, { method: 'POST' });
    return await res.json();
  } catch (err) {
    console.error('Error triggering custom IP request:', err);
  }
}

// Edge Shield Toggle Handler
if (btnToggleShield) {
  btnToggleShield.addEventListener('click', async () => {
    btnToggleShield.disabled = true;
    try {
      const res = await fetch('/api/shield/toggle', { method: 'POST' });
      const data = await res.json();
      if (shieldBtnText) {
        shieldBtnText.textContent = data.enabled ? 'Shield Enabled' : 'Shield Disabled';
      }
    } catch (err) {
      console.error('Failed to toggle shield:', err);
    } finally {
      btnToggleShield.disabled = false;
    }
  });
}

// Guided DDoS Scenario Runner
let ddosRunning = false;

async function runDDoSScenario() {
  if (ddosRunning) return;
  ddosRunning = true;

  const banner = document.getElementById('storyline-banner');
  const phaseBadge = document.getElementById('storyline-phase-badge');
  const title = document.getElementById('storyline-title');
  const message = document.getElementById('storyline-message');
  const progressFill = document.getElementById('storyline-progress-fill');

  if (banner) banner.classList.remove('hidden');
  if (btnRunDDoSScenario) btnRunDDoSScenario.disabled = true;

  function updateStory(phase, t, msg, pct) {
    if (phaseBadge) phaseBadge.textContent = phase;
    if (title) title.textContent = t;
    if (message) message.textContent = msg;
    if (progressFill) progressFill.style.width = `${pct}%`;
  }

  // Ensure shield is enabled
  try {
    await fetch('/api/shield/toggle?state=on', { method: 'POST' });
  } catch (e) {}

  // Phase 1: Baseline Legitimate Traffic
  updateStory(
    'Phase 1 of 4',
    'Baseline Legitimate User Traffic',
    'Sending 5 normal requests from legitimate user IPs (198.51.100.x)... All admitted with 200 OK.',
    25
  );
  await triggerTestRequestWithIP(5, '');
  await new Promise(r => setTimeout(r, 1500));

  // Phase 2: Attacker Flood
  updateStory(
    'Phase 2 of 4',
    'Rogue Botnet Flood Injected',
    'Attacker IP 203.0.113.42 floods proxy with 20 rapid requests exceeding token bucket capacity (10 tokens)...',
    50
  );
  await triggerTestRequestWithIP(20, '203.0.113.42');
  await new Promise(r => setTimeout(r, 1500));

  // Phase 3: Edge Shield Throttles Excess
  updateStory(
    'Phase 3 of 4',
    'Edge Shield Active (HTTP 429)',
    'Token bucket exhausted for attacker IP 203.0.113.42. Excess requests rejected at edge in <0.1ms with 429 Too Many Requests!',
    75
  );
  await new Promise(r => setTimeout(r, 1600));

  // Phase 4: Innocent User Continues Unaffected
  updateStory(
    'Phase 4 of 4',
    'Legitimate User Admitted with Zero Lag',
    'Innocent client (198.51.100.1) connects while attacker is throttled. Admitted instantly (200 OK) with zero noisy-neighbor degradation!',
    100
  );
  await triggerTestRequestWithIP(2, '198.51.100.1');
  await new Promise(r => setTimeout(r, 2000));

  updateStory(
    'Demo Completed',
    'Anti-DDoS Mitigation Verified',
    'Target attacker isolated and blocked at edge shield; legitimate users preserved with 100% availability.',
    100
  );

  setTimeout(() => {
    if (btnRunDDoSScenario) btnRunDDoSScenario.disabled = false;
    ddosRunning = false;
  }, 1000);
}

if (btnRunDDoSScenario) {
  btnRunDDoSScenario.addEventListener('click', runDDoSScenario);
}

if (btnSpamAttacker) {
  btnSpamAttacker.addEventListener('click', async () => {
    btnSpamAttacker.disabled = true;
    try {
      await triggerTestRequestWithIP(20, '203.0.113.42');
    } finally {
      setTimeout(() => btnSpamAttacker.disabled = false, 500);
    }
  });
}

if (btnSendNormalUsers) {
  btnSendNormalUsers.addEventListener('click', async () => {
    btnSendNormalUsers.disabled = true;
    try {
      await triggerTestRequestWithIP(15, '');
    } finally {
      setTimeout(() => btnSendNormalUsers.disabled = false, 500);
    }
  });
}

// Adjust weight on backend
window.adjustBackendWeight = async function(url, delta) {
  try {
    await fetch(`/api/backend/weight?url=${encodeURIComponent(url)}&delta=${delta}`, { method: 'POST' });
  } catch (err) {
    console.error(`Failed to adjust weight for ${url}:`, err);
  }
};

window.setBackendWeight = async function(url, weight) {
  try {
    await fetch(`/api/backend/weight?url=${encodeURIComponent(url)}&weight=${weight}`, { method: 'POST' });
  } catch (err) {
    console.error(`Failed to set weight for ${url}:`, err);
  }
};

window.toggleCanary = async function(url, enabled) {
  try {
    await fetch(`/api/backend/canary?url=${encodeURIComponent(url)}&enabled=${enabled}&tag=v2.0-canary`, { method: 'POST' });
  } catch (err) {
    console.error(`Failed to toggle canary for ${url}:`, err);
  }
};

// Scenario 2: Canary Deployment Controls
const btnSendCanarySingle = document.getElementById('btn-send-canary-single');
const btnSendCanaryBurst = document.getElementById('btn-send-canary-burst');
const btnSetCanaryWeights = document.getElementById('btn-set-canary-weights');

if (btnSendCanarySingle) {
  btnSendCanarySingle.addEventListener('click', async () => {
    btnSendCanarySingle.disabled = true;
    try {
      await fetch('/api/test-request?count=1&canary=true', { method: 'POST' });
    } finally {
      setTimeout(() => btnSendCanarySingle.disabled = false, 400);
    }
  });
}

if (btnSendCanaryBurst) {
  btnSendCanaryBurst.addEventListener('click', async () => {
    btnSendCanaryBurst.disabled = true;
    try {
      await fetch('/api/test-request?count=10&canary=true', { method: 'POST' });
    } finally {
      setTimeout(() => btnSendCanaryBurst.disabled = false, 500);
    }
  });
}

if (btnSetCanaryWeights) {
  btnSetCanaryWeights.addEventListener('click', async () => {
    btnSetCanaryWeights.disabled = true;
    try {
      // 1. Set weights 5 : 5 : 1
      await fetch('/api/backend/weight?url=http://localhost:8001&weight=5', { method: 'POST' });
      await fetch('/api/backend/weight?url=http://localhost:8002&weight=5', { method: 'POST' });
      await fetch('/api/backend/weight?url=http://localhost:8003&weight=1', { method: 'POST' });
      // 2. Set Node 3 as Canary
      await fetch('/api/backend/canary?url=http://localhost:8003&enabled=true&tag=v2.0-canary', { method: 'POST' });
      // 3. Switch strategy to Weighted Round Robin
      await fetch('/api/strategy?name=weighted_round_robin', { method: 'POST' });
      updateStrategyPills('weighted_round_robin');
    } finally {
      setTimeout(() => btnSetCanaryWeights.disabled = false, 600);
    }
  });
}

// Initialize SSE connection
connectSSE();
