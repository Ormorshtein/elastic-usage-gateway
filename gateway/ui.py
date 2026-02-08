"""
Control panel UI — single HTML page with inline CSS/JS.
Served at /_gateway/ui by the gateway.

Two tabs:
  - Generator: scenario selection, weight sliders, traffic generation controls
  - Monitor: live health/stats, query body config, heat report links
"""

HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ES Usage Gateway — Control Panel</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0f1117; color: #e0e0e0; padding: 24px; }
  h1 { font-size: 22px; margin-bottom: 4px; color: #fff; }
  .subtitle { color: #888; font-size: 13px; margin-bottom: 20px; }

  /* Main navigation tabs */
  .main-tabs { display: flex; gap: 0; margin-bottom: 24px; border-bottom: 2px solid #2a2d37; }
  .main-tab { padding: 12px 24px; font-size: 14px; font-weight: 600; cursor: pointer;
              color: #888; border-bottom: 2px solid transparent; margin-bottom: -2px;
              transition: all .15s; user-select: none; }
  .main-tab:hover { color: #ccc; }
  .main-tab.active { color: #4f8ff7; border-bottom-color: #4f8ff7; }
  .tab-content { display: none; }
  .tab-content.active { display: block; }

  .card { background: #1a1d27; border: 1px solid #2a2d37; border-radius: 8px;
          padding: 20px; margin-bottom: 16px; }
  .card h2 { font-size: 14px; text-transform: uppercase; letter-spacing: 1px;
             color: #888; margin-bottom: 16px; }

  /* Scenario tabs */
  .tabs { display: flex; gap: 4px; margin-bottom: 16px; }
  .tab { padding: 8px 16px; border-radius: 6px; border: 1px solid #3a3d47;
         background: #0f1117; color: #888; font-size: 13px; font-weight: 600;
         cursor: pointer; transition: all .15s; }
  .tab:hover { border-color: #4f8ff7; color: #ccc; }
  .tab.active { background: #1e2a3a; border-color: #4f8ff7; color: #4f8ff7; }

  .slider-row { display: flex; align-items: center; margin-bottom: 10px; gap: 12px; }
  .slider-label { width: 220px; font-size: 13px; color: #ccc; }
  .slider-row input[type=range] { flex: 1; accent-color: #4f8ff7; }
  .slider-value { width: 36px; text-align: right; font-size: 14px; font-weight: 600;
                  color: #4f8ff7; font-variant-numeric: tabular-nums; }
  .count-row { display: flex; align-items: center; gap: 12px; margin-bottom: 8px; }
  .count-row label { font-size: 14px; color: #ccc; }
  .count-row input[type=number] { width: 120px; padding: 6px 10px; border-radius: 6px;
    border: 1px solid #3a3d47; background: #0f1117; color: #fff; font-size: 15px; }
  .actions { display: flex; gap: 12px; margin-top: 8px; flex-wrap: wrap; }
  button { padding: 10px 20px; border: none; border-radius: 6px; font-size: 14px;
           font-weight: 600; cursor: pointer; transition: opacity .15s; }
  button:hover { opacity: 0.85; }
  button:disabled { opacity: 0.4; cursor: not-allowed; }
  .btn-run { background: #4f8ff7; color: #fff; }
  .btn-run-all { background: #27ae60; color: #fff; }
  .btn-clear { background: #e74c3c; color: #fff; }
  .btn-reset { background: #3a3d47; color: #ccc; }
  .status { margin-top: 16px; padding: 12px; border-radius: 6px; font-size: 13px;
            display: none; white-space: pre-wrap; }
  .status.info { display: block; background: #1e2a3a; border: 1px solid #2a4a6a; color: #7ab8f5; }
  .status.ok { display: block; background: #1a2e1a; border: 1px solid #2a5a2a; color: #6fcf6f; }
  .status.err { display: block; background: #2e1a1a; border: 1px solid #5a2a2a; color: #cf6f6f; }
  .progress { margin-top: 8px; height: 4px; background: #2a2d37; border-radius: 2px;
              overflow: hidden; display: none; }
  .progress.active { display: block; }
  .progress-bar { height: 100%; background: #4f8ff7; transition: width .3s;
                  animation: pulse 1.5s ease-in-out infinite; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.6; } }

  /* Monitor tab styles */
  .health-banner { padding: 12px 16px; border-radius: 6px; font-size: 14px;
                   font-weight: 600; margin-bottom: 16px; display: flex;
                   align-items: center; gap: 10px; }
  .health-banner.healthy { background: #1a2e1a; border: 1px solid #2a5a2a; color: #6fcf6f; }
  .health-banner.unhealthy { background: #2e1a1a; border: 1px solid #5a2a2a; color: #cf6f6f; }
  .health-banner.loading { background: #1e2a3a; border: 1px solid #2a4a6a; color: #7ab8f5; }
  .health-dot { width: 10px; height: 10px; border-radius: 50%; }
  .healthy .health-dot { background: #6fcf6f; }
  .unhealthy .health-dot { background: #cf6f6f; }
  .loading .health-dot { background: #7ab8f5; }

  .stats-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
                gap: 12px; }
  .stat-box { background: #0f1117; border: 1px solid #2a2d37; border-radius: 6px;
              padding: 12px 16px; }
  .stat-label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px;
                color: #888; margin-bottom: 4px; }
  .stat-value { font-size: 22px; font-weight: 700; color: #fff;
                font-variant-numeric: tabular-nums; }
  .stat-value.warn { color: #f0ad4e; }
  .stat-value.err { color: #cf6f6f; }

  .links { display: flex; gap: 16px; flex-wrap: wrap; }
  .links a { color: #4f8ff7; text-decoration: none; font-size: 13px; padding: 8px 14px;
             background: #1e2a3a; border-radius: 6px; border: 1px solid #2a4a6a; }
  .links a:hover { background: #2a3a5a; }

  .refresh-hint { font-size: 11px; color: #555; margin-top: 8px; }
</style>
</head>
<body>

<h1>ES Usage Gateway</h1>
<p class="subtitle">Control Panel</p>

<!-- Main navigation -->
<div class="main-tabs">
  <div class="main-tab active" onclick="switchMainTab('generator')">Generator</div>
  <div class="main-tab" onclick="switchMainTab('monitor')">Monitor</div>
</div>

<!-- ==================== GENERATOR TAB ==================== -->
<div class="tab-content active" id="tab-generator">

<div class="card">
  <h2>Scenario</h2>
  <div class="tabs" id="scenario-tabs"></div>
  <div id="sliders"></div>
  <div class="count-row" style="margin-top: 16px;">
    <label for="count">Query count:</label>
    <input type="number" id="count" value="200" min="1" max="10000">
  </div>
  <div class="count-row" style="margin-top: 8px;">
    <label for="lookback">Lookback:</label>
    <input type="text" id="lookback" placeholder="e.g. 6h, 30d, 15m" style="width:160px; padding:6px 10px; border-radius:6px; border:1px solid #3a3d47; background:#0f1117; color:#fff; font-size:15px;">
    <span style="color:#666; font-size:12px;" id="lookback-hint">empty = random</span>
  </div>
  <div id="time-range-info" style="margin-top:8px; font-size:12px; color:#666;"></div>
</div>

<div class="card">
  <h2>Actions</h2>
  <div class="actions">
    <button class="btn-run" id="btn-run" onclick="runScenario()">Run Scenario</button>
    <button class="btn-run-all" id="btn-run-all" onclick="runAllScenarios()">Run All Scenarios</button>
    <button class="btn-clear" id="btn-clear" onclick="clearEvents()">Clear Events</button>
    <button class="btn-reset" onclick="resetWeights()">Reset Weights</button>
  </div>
  <div class="progress" id="progress"><div class="progress-bar" style="width:100%"></div></div>
  <div class="status" id="status"></div>
</div>

</div>

<!-- ==================== MONITOR TAB ==================== -->
<div class="tab-content" id="tab-monitor">

<div id="health-banner" class="health-banner loading">
  <div class="health-dot"></div>
  <span id="health-text">Checking...</span>
</div>

<div class="card">
  <h2>Gateway Stats</h2>
  <div class="stats-grid" id="stats-grid">
    <div class="stat-box"><div class="stat-label">Requests proxied</div><div class="stat-value" id="st-requests-proxied">-</div></div>
    <div class="stat-box"><div class="stat-label">Requests failed</div><div class="stat-value" id="st-requests-failed">-</div></div>
    <div class="stat-box"><div class="stat-label">Events emitted</div><div class="stat-value" id="st-events-emitted">-</div></div>
    <div class="stat-box"><div class="stat-label">Events failed</div><div class="stat-value" id="st-events-failed">-</div></div>
    <div class="stat-box"><div class="stat-label">Events skipped</div><div class="stat-value" id="st-events-skipped">-</div></div>
    <div class="stat-box"><div class="stat-label">Events sampled out</div><div class="stat-value" id="st-events-sampled-out">-</div></div>
    <div class="stat-box"><div class="stat-label">Extraction errors</div><div class="stat-value" id="st-extraction-errors">-</div></div>
    <div class="stat-box"><div class="stat-label">Metadata refreshes</div><div class="stat-value" id="st-metadata-refresh-ok">-</div></div>
    <div class="stat-box"><div class="stat-label">Metadata failures</div><div class="stat-value" id="st-metadata-refresh-failed">-</div></div>
    <div class="stat-box"><div class="stat-label">ES avg response</div><div class="stat-value" id="st-es-time-avg">-</div></div>
    <div class="stat-box"><div class="stat-label">ES max response</div><div class="stat-value" id="st-es-time-max">-</div></div>
    <div class="stat-box"><div class="stat-label">Avg request time</div><div class="stat-value" id="st-request-time-avg">-</div></div>
    <div class="stat-box"><div class="stat-label">Max request time</div><div class="stat-value" id="st-request-time-max">-</div></div>
    <div class="stat-box"><div class="stat-label">Uptime</div><div class="stat-value" id="st-uptime">-</div></div>
    <div class="stat-box"><div class="stat-label">Index groups</div><div class="stat-value" id="st-groups">-</div></div>
  </div>
  <div class="refresh-hint">Auto-refreshes every 5 seconds</div>
</div>

<div class="card">
  <h2>Event Sampling</h2>
  <div style="display:flex; align-items:center; gap:16px; flex-wrap:wrap;">
    <label style="font-size:13px; color:#ccc; display:flex; align-items:center; gap:8px;">
      Sample rate:
      <input type="range" id="es-rate" min="0" max="100" value="100" style="width:160px; accent-color:#4f8ff7;" oninput="document.getElementById('es-rate-val').textContent=this.value+'%'; updateESConfig()">
      <span id="es-rate-val" style="font-size:14px; font-weight:600; color:#4f8ff7; width:40px;">100%</span>
    </label>
    <span style="font-size:12px; color:#666;">Controls what fraction of proxied requests emit usage events</span>
  </div>
</div>

<div class="card">
  <h2>Query Body Storage</h2>
  <div style="display:flex; align-items:center; gap:16px; flex-wrap:wrap;">
    <label style="font-size:13px; color:#ccc; display:flex; align-items:center; gap:8px; cursor:pointer;">
      <input type="checkbox" id="qb-enabled" onchange="updateQBConfig()" style="accent-color:#4f8ff7;">
      Store query bodies
    </label>
    <label style="font-size:13px; color:#ccc; display:flex; align-items:center; gap:8px;">
      Sample rate:
      <input type="range" id="qb-rate" min="0" max="100" value="100" style="width:120px; accent-color:#4f8ff7;" oninput="document.getElementById('qb-rate-val').textContent=this.value+'%'; updateQBConfig()">
      <span id="qb-rate-val" style="font-size:14px; font-weight:600; color:#4f8ff7; width:40px;">100%</span>
    </label>
  </div>
</div>

<div class="card">
  <h2>Links</h2>
  <div class="links">
    <a href="/_gateway/heat" target="_blank">Heat Report (JSON)</a>
    <a href="/_gateway/stats" target="_blank">Stats (JSON)</a>
    <a href="/_gateway/health" target="_blank">Health (JSON)</a>
    <a href="/_gateway/sample-events?count=10" target="_blank">Sample Events</a>
    <a href="/_gateway/scenarios" target="_blank">Scenarios (JSON)</a>
  </div>
</div>

</div>

<script>
/* ==================== MAIN TAB SWITCHING ==================== */

function switchMainTab(tab) {
  document.querySelectorAll('.main-tab').forEach((el, i) => {
    el.classList.toggle('active', el.textContent.toLowerCase() === tab);
  });
  document.querySelectorAll('.tab-content').forEach(el => {
    el.classList.toggle('active', el.id === 'tab-' + tab);
  });
  if (tab === 'monitor') refreshStats();
}

/* ==================== GENERATOR TAB ==================== */

let scenarios = {};
let activeScenario = 'products';

async function loadScenarios() {
  try {
    const resp = await fetch('/_gateway/scenarios');
    scenarios = await resp.json();
    buildTabs();
    switchScenario('products');
  } catch (e) {
    setStatus('Failed to load scenarios: ' + e.message, 'err');
  }
}

function buildTabs() {
  const container = document.getElementById('scenario-tabs');
  container.innerHTML = '';
  for (const [key, s] of Object.entries(scenarios)) {
    const tab = document.createElement('div');
    tab.className = 'tab' + (key === activeScenario ? ' active' : '');
    tab.textContent = s.label;
    tab.dataset.scenario = key;
    tab.onclick = () => switchScenario(key);
    container.appendChild(tab);
  }
}

function switchScenario(key) {
  activeScenario = key;
  document.querySelectorAll('#scenario-tabs .tab').forEach(t => {
    t.className = 'tab' + (t.dataset.scenario === key ? ' active' : '');
  });
  buildSliders();
  updateTimeRangeInfo();
}

function updateTimeRangeInfo() {
  const s = scenarios[activeScenario];
  const el = document.getElementById('time-range-info');
  if (!s || !s.time_range_queries || s.time_range_queries.length === 0) {
    el.textContent = 'No time-range queries in this scenario';
    return;
  }
  const names = s.time_range_queries.map(q => s.labels[q] || q).join(', ');
  el.textContent = 'Lookback affects: ' + names;
}

function buildSliders() {
  const s = scenarios[activeScenario];
  if (!s) return;
  const container = document.getElementById('sliders');
  container.innerHTML = '';
  for (const [key, def] of Object.entries(s.weights)) {
    const storageKey = 'w_' + activeScenario + '_' + key;
    const saved = localStorage.getItem(storageKey);
    const val = saved !== null ? parseInt(saved) : def;
    const label = s.labels[key] || key;

    const row = document.createElement('div');
    row.className = 'slider-row';

    const labelEl = document.createElement('span');
    labelEl.className = 'slider-label';
    labelEl.textContent = label;

    const slider = document.createElement('input');
    slider.type = 'range';
    slider.min = '0';
    slider.max = '100';
    slider.value = val;
    slider.id = 's_' + key;

    const valueEl = document.createElement('span');
    valueEl.className = 'slider-value';
    valueEl.id = 'v_' + key;
    valueEl.textContent = val;

    slider.oninput = function() {
      valueEl.textContent = this.value;
      localStorage.setItem(storageKey, this.value);
    };

    row.appendChild(labelEl);
    row.appendChild(slider);
    row.appendChild(valueEl);
    container.appendChild(row);
  }
}

function getWeights() {
  const s = scenarios[activeScenario];
  if (!s) return {};
  const w = {};
  for (const key of Object.keys(s.weights)) {
    const el = document.getElementById('s_' + key);
    w[key] = el ? parseInt(el.value) : s.weights[key];
  }
  return w;
}

function resetWeights() {
  const s = scenarios[activeScenario];
  if (!s) return;
  for (const [key, def] of Object.entries(s.weights)) {
    const el = document.getElementById('s_' + key);
    const vel = document.getElementById('v_' + key);
    if (el) el.value = def;
    if (vel) vel.textContent = def;
    localStorage.removeItem('w_' + activeScenario + '_' + key);
  }
}

function setStatus(msg, type) {
  const el = document.getElementById('status');
  el.className = 'status ' + type;
  el.textContent = msg;
}

function setProgress(active) {
  document.getElementById('progress').className = active ? 'progress active' : 'progress';
}

function setButtonsDisabled(disabled) {
  document.getElementById('btn-run').disabled = disabled;
  document.getElementById('btn-run-all').disabled = disabled;
}

function getLookback() {
  const val = document.getElementById('lookback').value.trim();
  return val || null;
}

async function runScenario(scenarioKey, count) {
  const key = scenarioKey || activeScenario;
  const cnt = count || parseInt(document.getElementById('count').value);
  const weights = scenarioKey ? scenarios[key].weights : getWeights();
  const lookback = getLookback();

  setButtonsDisabled(true);
  const lbText = lookback ? ' (lookback: ' + lookback + ')' : '';
  setStatus('Running ' + (scenarios[key]?.label || key) + lbText + '...', 'info');
  setProgress(true);
  try {
    const payload = { count: cnt, scenario: key, weights: weights };
    if (lookback) payload.lookback = lookback;
    const resp = await fetch('/_gateway/generate', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });
    const data = await resp.json();
    if (resp.ok) {
      const s = scenarios[key];
      const lbInfo = data.lookback ? ' [lookback: ' + data.lookback + ']' : '';
      let msg = (s?.label || key) + ': sent ' + data.sent + ' queries in ' + data.elapsed_seconds + 's (' + data.ok + ' ok, ' + data.errors + ' errors)' + lbInfo;
      if (data.breakdown) {
        const labels = s?.labels || {};
        const lines = Object.entries(data.breakdown)
          .sort((a, b) => b[1] - a[1])
          .map(([k, v]) => (labels[k] || k) + ': ' + v)
          .join(', ');
        msg += '\\n' + lines;
      }
      setStatus(msg, 'ok');
      setProgress(false);
      setButtonsDisabled(false);
      return data;
    } else {
      setStatus('Error: ' + (data.detail || JSON.stringify(data)), 'err');
    }
  } catch (e) {
    setStatus('Network error: ' + e.message, 'err');
  }
  setProgress(false);
  setButtonsDisabled(false);
  return null;
}

async function runAllScenarios() {
  setButtonsDisabled(true);
  setProgress(true);
  const count = parseInt(document.getElementById('count').value);
  const lookback = getLookback();
  const results = [];

  for (const key of Object.keys(scenarios)) {
    setStatus('Running ' + scenarios[key].label + '...', 'info');
    try {
      const payload = { count: count, scenario: key, weights: scenarios[key].weights };
      if (lookback) payload.lookback = lookback;
      const resp = await fetch('/_gateway/generate', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
      });
      const data = await resp.json();
      if (resp.ok) {
        results.push(scenarios[key].label + ': ' + data.sent + ' queries (' + data.ok + ' ok, ' + data.errors + ' errors) in ' + data.elapsed_seconds + 's');
      } else {
        results.push(scenarios[key].label + ': ERROR');
      }
    } catch (e) {
      results.push(scenarios[key].label + ': NETWORK ERROR');
    }
  }

  setStatus('All scenarios complete:\\n' + results.join('\\n'), 'ok');
  setProgress(false);
  setButtonsDisabled(false);
}

async function clearEvents() {
  if (!confirm('Delete all usage events? This cannot be undone.')) return;
  const btn = document.getElementById('btn-clear');
  btn.disabled = true;
  setStatus('Clearing events...', 'info');
  try {
    const resp = await fetch('/_gateway/events', { method: 'DELETE' });
    const data = await resp.json();
    if (resp.ok) {
      setStatus('Deleted ' + data.deleted + ' events', 'ok');
    } else {
      setStatus('Error: ' + (data.detail || JSON.stringify(data)), 'err');
    }
  } catch (e) {
    setStatus('Network error: ' + e.message, 'err');
  }
  btn.disabled = false;
}

/* ==================== MONITOR TAB ==================== */

function formatUptime(seconds) {
  if (seconds < 60) return Math.round(seconds) + 's';
  if (seconds < 3600) return Math.round(seconds / 60) + 'm';
  const h = Math.floor(seconds / 3600);
  const m = Math.round((seconds % 3600) / 60);
  return h + 'h ' + m + 'm';
}

async function refreshStats() {
  // Fetch health
  try {
    const resp = await fetch('/_gateway/health');
    const data = await resp.json();
    const banner = document.getElementById('health-banner');
    const text = document.getElementById('health-text');
    if (data.status === 'healthy') {
      banner.className = 'health-banner healthy';
      text.textContent = 'Elasticsearch reachable — Gateway healthy';
    } else {
      banner.className = 'health-banner unhealthy';
      text.textContent = 'Elasticsearch: ' + (data.elasticsearch || 'unreachable');
    }
  } catch (e) {
    const banner = document.getElementById('health-banner');
    const text = document.getElementById('health-text');
    banner.className = 'health-banner unhealthy';
    text.textContent = 'Gateway unreachable: ' + e.message;
  }

  // Fetch stats
  try {
    const resp = await fetch('/_gateway/stats');
    const data = await resp.json();

    const set = (id, val, warnIf) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = typeof val === 'number' ? val.toLocaleString() : val;
      el.className = 'stat-value';
      if (warnIf && val > 0) el.className = 'stat-value ' + (val > 10 ? 'err' : 'warn');
    };

    set('st-requests-proxied', data.requests_proxied || 0);
    set('st-requests-failed', data.requests_failed || 0, true);
    set('st-events-emitted', data.events_emitted || 0);
    set('st-events-failed', data.events_failed || 0, true);
    set('st-events-skipped', data.events_skipped || 0);
    set('st-events-sampled-out', data.events_sampled_out || 0);
    set('st-extraction-errors', data.extraction_errors || 0, true);
    set('st-metadata-refresh-ok', data.metadata_refresh_ok || 0);
    set('st-metadata-refresh-failed', data.metadata_refresh_failed || 0, true);
    set('st-es-time-avg', (data.es_time_avg_ms || 0) + 'ms');
    set('st-es-time-max', (data.es_time_max_ms || 0) + 'ms');
    set('st-request-time-avg', (data.request_time_avg_ms || 0) + 'ms');
    set('st-request-time-max', (data.request_time_max_ms || 0) + 'ms');
    set('st-uptime', formatUptime(data.uptime_seconds || 0));
    const groups = data.metadata_cache ? data.metadata_cache.groups : '-';
    set('st-groups', groups);
  } catch (e) {
    console.warn('Failed to fetch stats:', e);
  }
}

async function loadConfig() {
  try {
    const resp = await fetch('/_gateway/config');
    const data = await resp.json();
    const es = data.event_sampling || {};
    const esPct = Math.round((es.sample_rate || 0) * 100);
    document.getElementById('es-rate').value = esPct;
    document.getElementById('es-rate-val').textContent = esPct + '%';
    const qb = data.query_body || {};
    document.getElementById('qb-enabled').checked = qb.enabled !== false;
    const pct = Math.round((qb.sample_rate || 0) * 100);
    document.getElementById('qb-rate').value = pct;
    document.getElementById('qb-rate-val').textContent = pct + '%';
  } catch (e) {
    console.warn('Failed to load config:', e);
  }
}

let _esTimer = null;
function updateESConfig() {
  clearTimeout(_esTimer);
  _esTimer = setTimeout(async () => {
    const rate = parseInt(document.getElementById('es-rate').value) / 100;
    try {
      await fetch('/_gateway/config', {
        method: 'PATCH',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ event_sampling: { sample_rate: rate } })
      });
    } catch (e) {
      console.warn('Failed to update event sampling config:', e);
    }
  }, 300);
}

let _qbTimer = null;
function updateQBConfig() {
  clearTimeout(_qbTimer);
  _qbTimer = setTimeout(async () => {
    const enabled = document.getElementById('qb-enabled').checked;
    const rate = parseInt(document.getElementById('qb-rate').value) / 100;
    try {
      await fetch('/_gateway/config', {
        method: 'PATCH',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ query_body: { enabled, sample_rate: rate } })
      });
    } catch (e) {
      console.warn('Failed to update query body config:', e);
    }
  }, 300);
}

/* ==================== INIT ==================== */

loadScenarios();
loadConfig();
refreshStats();

// Auto-refresh stats every 5 seconds
setInterval(refreshStats, 5000);
</script>
</body>
</html>
"""
