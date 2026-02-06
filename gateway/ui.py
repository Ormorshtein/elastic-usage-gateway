"""
Control panel UI — single HTML page with inline CSS/JS.
Served at /_gateway/ui by the gateway.
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
  .subtitle { color: #888; font-size: 13px; margin-bottom: 24px; }
  .card { background: #1a1d27; border: 1px solid #2a2d37; border-radius: 8px;
          padding: 20px; margin-bottom: 16px; }
  .card h2 { font-size: 14px; text-transform: uppercase; letter-spacing: 1px;
             color: #888; margin-bottom: 16px; }
  .slider-row { display: flex; align-items: center; margin-bottom: 10px; gap: 12px; }
  .slider-label { width: 220px; font-size: 13px; color: #ccc; }
  .slider-row input[type=range] { flex: 1; accent-color: #4f8ff7; }
  .slider-value { width: 36px; text-align: right; font-size: 14px; font-weight: 600;
                  color: #4f8ff7; font-variant-numeric: tabular-nums; }
  .count-row { display: flex; align-items: center; gap: 12px; margin-bottom: 8px; }
  .count-row label { font-size: 14px; color: #ccc; }
  .count-row input[type=number] { width: 120px; padding: 6px 10px; border-radius: 6px;
    border: 1px solid #3a3d47; background: #0f1117; color: #fff; font-size: 15px; }
  .actions { display: flex; gap: 12px; margin-top: 8px; }
  button { padding: 10px 20px; border: none; border-radius: 6px; font-size: 14px;
           font-weight: 600; cursor: pointer; transition: opacity .15s; }
  button:hover { opacity: 0.85; }
  button:disabled { opacity: 0.4; cursor: not-allowed; }
  .btn-run { background: #4f8ff7; color: #fff; }
  .btn-clear { background: #e74c3c; color: #fff; }
  .btn-reset { background: #3a3d47; color: #ccc; }
  .status { margin-top: 16px; padding: 12px; border-radius: 6px; font-size: 13px;
            display: none; white-space: pre-wrap; }
  .status.info { display: block; background: #1e2a3a; border: 1px solid #2a4a6a; color: #7ab8f5; }
  .status.ok { display: block; background: #1a2e1a; border: 1px solid #2a5a2a; color: #6fcf6f; }
  .status.err { display: block; background: #2e1a1a; border: 1px solid #5a2a2a; color: #cf6f6f; }
  .links { display: flex; gap: 16px; flex-wrap: wrap; }
  .links a { color: #4f8ff7; text-decoration: none; font-size: 13px; padding: 8px 14px;
             background: #1e2a3a; border-radius: 6px; border: 1px solid #2a4a6a; }
  .links a:hover { background: #2a3a5a; }
  .progress { margin-top: 8px; height: 4px; background: #2a2d37; border-radius: 2px;
              overflow: hidden; display: none; }
  .progress.active { display: block; }
  .progress-bar { height: 100%; background: #4f8ff7; transition: width .3s;
                  animation: pulse 1.5s ease-in-out infinite; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.6; } }
</style>
</head>
<body>

<h1>ES Usage Gateway</h1>
<p class="subtitle">Control Panel &mdash; generate traffic, observe heat</p>

<div class="card">
  <h2>Query Distribution</h2>
  <div id="sliders"></div>
  <div class="count-row" style="margin-top: 16px;">
    <label for="count">Query count:</label>
    <input type="number" id="count" value="200" min="1" max="10000">
  </div>
</div>

<div class="card">
  <h2>Actions</h2>
  <div class="actions">
    <button class="btn-run" id="btn-run" onclick="runGenerator()">Run Generator</button>
    <button class="btn-clear" id="btn-clear" onclick="clearStats()">Clear Stats</button>
    <button class="btn-reset" onclick="resetWeights()">Reset Weights</button>
  </div>
  <div class="progress" id="progress"><div class="progress-bar" style="width:100%"></div></div>
  <div class="status" id="status"></div>
</div>

<div class="card">
  <h2>Links</h2>
  <div class="links">
    <a href="http://localhost:5601/app/dashboards#/view/usage-heat" target="_blank">Usage &amp; Heat Dashboard</a>
    <a href="http://localhost:5601/app/dashboards#/view/products-explorer" target="_blank">Products Explorer</a>
    <a href="/_gateway/heat" target="_blank">Heat Report (JSON)</a>
    <a href="http://localhost:5601/app/discover" target="_blank">Discover (raw events)</a>
  </div>
</div>

<script>
const DEFAULTS = {
  search_by_title: 40,
  filter_by_category_sort_by_price: 25,
  aggregate_by_brand: 15,
  range_on_rating: 10,
  search_by_description: 5,
  get_by_id: 3,
  match_all: 2
};

const LABELS = {
  search_by_title: "Search by title",
  filter_by_category_sort_by_price: "Filter category + sort price",
  aggregate_by_brand: "Aggregate by brand",
  range_on_rating: "Range on rating",
  search_by_description: "Search by description",
  get_by_id: "Get by ID",
  match_all: "Match all"
};

function buildSliders() {
  const container = document.getElementById('sliders');
  container.innerHTML = '';
  for (const [key, def] of Object.entries(DEFAULTS)) {
    const saved = localStorage.getItem('w_' + key);
    const val = saved !== null ? parseInt(saved) : def;
    container.innerHTML += `
      <div class="slider-row">
        <span class="slider-label">${LABELS[key]}</span>
        <input type="range" min="0" max="100" value="${val}" id="s_${key}"
               oninput="document.getElementById('v_${key}').textContent=this.value; localStorage.setItem('w_${key}',this.value)">
        <span class="slider-value" id="v_${key}">${val}</span>
      </div>`;
  }
}

function getWeights() {
  const w = {};
  for (const key of Object.keys(DEFAULTS)) {
    w[key] = parseInt(document.getElementById('s_' + key).value);
  }
  return w;
}

function resetWeights() {
  for (const [key, def] of Object.entries(DEFAULTS)) {
    document.getElementById('s_' + key).value = def;
    document.getElementById('v_' + key).textContent = def;
    localStorage.removeItem('w_' + key);
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

async function runGenerator() {
  const btn = document.getElementById('btn-run');
  btn.disabled = true;
  setStatus('Generating queries...', 'info');
  setProgress(true);
  try {
    const resp = await fetch('/_gateway/generate', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        count: parseInt(document.getElementById('count').value),
        weights: getWeights()
      })
    });
    const data = await resp.json();
    if (resp.ok) {
      let msg = `Sent ${data.sent} queries in ${data.elapsed_seconds}s (${data.ok} ok, ${data.errors} errors)`;
      if (data.breakdown) {
        const lines = Object.entries(data.breakdown)
          .sort((a, b) => b[1] - a[1])
          .map(([k, v]) => `${LABELS[k] || k}: ${v}`)
          .join(', ');
        msg += '\\n' + lines;
      }
      setStatus(msg, 'ok');
    } else {
      setStatus('Error: ' + (data.detail || JSON.stringify(data)), 'err');
    }
  } catch (e) {
    setStatus('Network error: ' + e.message, 'err');
  }
  setProgress(false);
  btn.disabled = false;
}

async function clearStats() {
  if (!confirm('Delete all usage events? This cannot be undone.')) return;
  const btn = document.getElementById('btn-clear');
  btn.disabled = true;
  setStatus('Clearing events...', 'info');
  try {
    const resp = await fetch('/_gateway/events', { method: 'DELETE' });
    const data = await resp.json();
    if (resp.ok) {
      setStatus(`Deleted ${data.deleted} events`, 'ok');
    } else {
      setStatus('Error: ' + (data.detail || JSON.stringify(data)), 'err');
    }
  } catch (e) {
    setStatus('Network error: ' + e.message, 'err');
  }
  btn.disabled = false;
}

buildSliders();
</script>
</body>
</html>
"""
