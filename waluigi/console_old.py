import socket
import configargparse
import uvicorn
import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse

app = FastAPI()

p = configargparse.ArgParser(auto_env_var_prefix='WALUIGI_CONSOLE_')
p.add('--port', type=int, default=8080)
p.add('--host', default=socket.gethostname())
p.add('--bind-address', default='0.0.0.0')
p.add('--boss-url', default='http://localhost:8082')
p.add('--catalog-url', default='http://localhost:9000')

args = p.parse_args()

BOSS_URL = args.boss_url.rstrip('/')
CATALOG_URL = args.catalog_url.rstrip('/')


def log(msg):
    print(f"[Console 🖥️] {msg}", flush=True)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

async def _boss_get(path):
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{BOSS_URL}{path}")
        r.raise_for_status()
        return r.json()


async def _boss_post(path, json=None):
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"{BOSS_URL}{path}", json=json)
        r.raise_for_status()
        return r.json()


# ---------------------------------------------------------------------------
# API proxy routes
# ---------------------------------------------------------------------------

@app.get('/api/jobs')
async def api_jobs():
    return JSONResponse(await _boss_get('/api/jobs'))

@app.get('/api/tasks')
async def api_tasks():
    return JSONResponse(await _boss_get('/api/tasks'))

@app.get('/api/workers')
async def api_workers():
    return JSONResponse(await _boss_get('/api/workers'))

@app.get('/api/resources')
async def api_resources():
    return JSONResponse(await _boss_get('/api/resources'))

@app.get('/api/logs/{task_id}')
async def api_logs(task_id: str, limit: int = 100):
    return JSONResponse(await _boss_get(f'/api/logs/{task_id}?limit={limit}'))

@app.post('/api/reset/task/{id}')
async def api_reset_task(id: str):
    return JSONResponse(await _boss_post(f'/api/reset/task/{id}'))

@app.post('/api/reset/namespace/{namespace}')
async def api_reset_namespace(namespace: str):
    return JSONResponse(await _boss_post(f'/api/reset/namespace/{namespace}'))

@app.post('/api/delete/task/{id}')
async def api_delete_task(id: str):
    return JSONResponse(await _boss_post(f'/api/delete/task/{id}'))

@app.post('/api/delete/namespace/{namespace}')
async def api_delete_namespace(namespace: str):
    return JSONResponse(await _boss_post(f'/api/delete/namespace/{namespace}'))


# ---------------------------------------------------------------------------
# Main HTML
# ---------------------------------------------------------------------------

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Waluigi Console</title>
  <link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Source+Sans+Pro:300,400,400i,700&display=fallback">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/admin-lte@3.2/dist/css/adminlte.min.css">
  <style>
    :root { --purple-dark: #1a0026; --purple-mid: #2b0040; --purple-accent: #4b0082; --purple-light: #d080ff; }
    body, .content-wrapper, .main-footer { background: #12001e !important; }
    .main-sidebar, .brand-link { background: var(--purple-dark) !important; }
    .brand-link { border-bottom: 1px solid var(--purple-accent) !important; }
    .main-header.navbar { background: var(--purple-dark) !important; border-bottom: 1px solid var(--purple-accent) !important; }
    .main-footer { border-top: 1px solid var(--purple-accent) !important; color: #888 !important; }
    .nav-sidebar .nav-link { color: #ccc !important; }
    .nav-sidebar .nav-link.active, .nav-sidebar .nav-link:hover { background: var(--purple-accent) !important; color: #fff !important; }
    .card { background: var(--purple-mid) !important; border-color: var(--purple-accent) !important; }
    .card-header { background: transparent !important; border-bottom: 1px solid var(--purple-accent) !important; }
    .card-title { color: var(--purple-light) !important; }
    .table { color: #e0e0e0 !important; }
    .table thead th { background: var(--purple-dark) !important; color: #ccc !important; border-color: var(--purple-accent) !important; }
    .table td, .table tr { border-color: #3a005a !important; }
    .table-hover tbody tr:hover { background: rgba(107,66,193,0.15) !important; }
    .info-box { background: var(--purple-mid) !important; border: 1px solid var(--purple-accent) !important; }
    .info-box-text { color: #aaa !important; }
    .info-box-number { color: #e0e0e0 !important; }
    .modal-content { background: var(--purple-dark) !important; border: 1px solid var(--purple-accent) !important; color: #e0e0e0 !important; }
    .modal-header { border-bottom: 1px solid var(--purple-accent) !important; }
    .modal-title { color: var(--purple-light) !important; }
    .close { color: #e0e0e0 !important; }
    .form-control { background: var(--purple-dark) !important; color: #e0e0e0 !important; border-color: var(--purple-accent) !important; }

    .badge-RUNNING   { background: #ffc107 !important; color: #1a1a1a !important; }
    .badge-SUCCESS   { background: #28a745 !important; }
    .badge-FAILED    { background: #dc3545 !important; }
    .badge-PENDING   { background: #6c757d !important; }
    .badge-READY     { background: #17a2b8 !important; }
    .badge-ALIVE     { background: #28a745 !important; }
    .badge-DEAD      { background: #dc3545 !important; }

    .blink { animation: blink 1.4s infinite; }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.25} }

    pre.log-line { margin:0; font-size:0.8em; white-space:pre-wrap; word-break:break-all; display:inline; color:#e0e0e0; }
    .log-entry { padding: 3px 10px; border-bottom: 1px solid #1a1a2e; }
    .log-ts { color: #6f42c1; font-size: 0.75em; }
    .log-worker { color: #888; font-size: 0.75em; margin: 0 6px; }
  </style>
</head>

<body class="hold-transition sidebar-mini layout-fixed">
<div class="wrapper">

  <!-- Navbar -->
  <nav class="main-header navbar navbar-expand navbar-dark">
    <ul class="navbar-nav">
      <li class="nav-item">
        <a class="nav-link" data-widget="pushmenu" href="#" role="button"><i class="fas fa-bars"></i></a>
      </li>
    </ul>
    <ul class="navbar-nav ml-auto align-items-center">
      <li class="nav-item mr-3">
        <span id="clock" style="color:var(--purple-light); font-size:0.85em;"></span>
      </li>
      <li class="nav-item">
        <a class="nav-link" href="#" onclick="refreshAll()" title="Refresh">
          <i class="fas fa-sync-alt" style="color:var(--purple-light);"></i>
        </a>
      </li>
    </ul>
  </nav>

  <!-- Sidebar -->
  <aside class="main-sidebar elevation-4">
    <a href="/" class="brand-link text-center">
      <span class="brand-text font-weight-bold" style="color:var(--purple-light); font-size:1.1em;">🟣 Waluigi</span>
    </a>
    <div class="sidebar">
      <nav class="mt-2">
        <ul class="nav nav-pills nav-sidebar flex-column" data-widget="treeview" role="menu" data-accordion="false">
          <li class="nav-item">
            <a href="#" class="nav-link active" onclick="showSection('jobs', this)">
              <i class="nav-icon fas fa-briefcase"></i>
              <p>Jobs <span class="badge badge-secondary right" id="badge-jobs">-</span></p>
            </a>
          </li>
          <li class="nav-item">
            <a href="#" class="nav-link" onclick="showSection('tasks', this)">
              <i class="nav-icon fas fa-tasks"></i>
              <p>Tasks <span class="badge badge-secondary right" id="badge-tasks">-</span></p>
            </a>
          </li>
          <li class="nav-item">
            <a href="#" class="nav-link" onclick="showSection('workers', this)">
              <i class="nav-icon fas fa-server"></i>
              <p>Workers <span class="badge badge-secondary right" id="badge-workers">-</span></p>
            </a>
          </li>
          <li class="nav-item">
            <a href="#" class="nav-link" onclick="showSection('resources', this)">
              <i class="nav-icon fas fa-chart-bar"></i>
              <p>Resources</p>
            </a>
          </li>
        </ul>
      </nav>
    </div>
  </aside>

  <!-- Content Wrapper -->
  <div class="content-wrapper">
    <div class="content-header">
      <div class="container-fluid">
        <h1 class="m-0" style="color:var(--purple-light);" id="section-title">Jobs</h1>
      </div>
    </div>

    <section class="content">
      <div class="container-fluid">

        <!-- ===== JOBS ===== -->
        <div id="section-jobs">
          <div class="row mb-3" id="job-stats"></div>
          <div class="card card-outline">
            <div class="card-header">
              <h3 class="card-title"><i class="fas fa-briefcase mr-2"></i>Jobs</h3>
            </div>
            <div class="card-body p-0">
              <div class="table-responsive">
              <table class="table table-sm table-hover mb-0">
                <thead>
                  <tr>
                    <th>Job ID</th>
                    <th>Status</th>
                    <th>Locked By</th>
                    <th>Locked Until</th>
                  </tr>
                </thead>
                <tbody id="jobs-tbody"></tbody>
              </table></div>
            </div>
          </div>
        </div>

        <!-- ===== TASKS ===== -->
        <div id="section-tasks" style="display:none;">
          <div id="tasks-namespaces"></div>
        </div>

        <!-- ===== WORKERS ===== -->
        <div id="section-workers" style="display:none;">
          <div class="row mb-3" id="worker-stats"></div>
          <div class="card card-outline">
            <div class="card-header">
              <h3 class="card-title"><i class="fas fa-server mr-2"></i>Workers</h3>
            </div>
            <div class="card-body p-0">
              <div class="table-responsive">
              <table class="table table-sm table-hover mb-0">
                <thead>
                  <tr>
                    <th>URL</th>
                    <th>Status</th>
                    <th>Max Slots</th>
                    <th>Free Slots</th>
                    <th>Last Seen</th>
                  </tr>
                </thead>
                <tbody id="workers-tbody"></tbody>
              </table></div>
            </div>
          </div>
        </div>

        <!-- ===== RESOURCES ===== -->
        <div id="section-resources" style="display:none;">
          <div class="row" id="resource-cards"></div>
        </div>

      </div>
    </section>
  </div><!-- /.content-wrapper -->

  <!-- Log Modal -->
  <div class="modal fade" id="logModal" tabindex="-1" role="dialog">
    <div class="modal-dialog modal-xl" role="document">
      <div class="modal-content">
        <div class="modal-header">
          <h5 class="modal-title" id="log-modal-title">Logs</h5>
          <button type="button" class="close" data-dismiss="modal"><span>&times;</span></button>
        </div>
        <div class="modal-body p-0" id="log-modal-body"
             style="max-height:65vh; overflow-y:auto; background:#0d001a; font-family:monospace;">
        </div>
      </div>
    </div>
  </div>

  <footer class="main-footer text-sm">
    <strong>Waluigi Console</strong> &mdash;
    auto-refresh every 10s &mdash;
    Boss: <code id="boss-url-label"></code>
  </footer>

</div><!-- ./wrapper -->

<script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.6.0/jquery.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/4.6.2/js/bootstrap.bundle.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/admin-lte@3.2/dist/js/adminlte.min.js"></script>

<script>
// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let _jobs = [], _tasks = [], _workers = [], _resources = [];
let _currentSection = 'jobs';

document.getElementById('boss-url-label').textContent = window.location.host;

// Clock
setInterval(() => {
  document.getElementById('clock').textContent = new Date().toLocaleTimeString();
}, 1000);

// ---------------------------------------------------------------------------
// Data fetching
// ---------------------------------------------------------------------------
async function _get(url) {
  try { return await fetch(url).then(r => r.json()); }
  catch(e) { console.warn('Fetch failed:', url, e); return []; }
}

async function fetchAll() {
  [_jobs, _tasks, _workers, _resources] = await Promise.all([
    _get('/api/jobs'),
    _get('/api/tasks'),
    _get('/api/workers'),
    _get('/api/resources'),
  ]);
  updateBadges();
  renderCurrent();
}

function refreshAll() { fetchAll(); }

// ---------------------------------------------------------------------------
// Badges
// ---------------------------------------------------------------------------
function updateBadges() {
  document.getElementById('badge-jobs').textContent    = _jobs.length;
  document.getElementById('badge-tasks').textContent   = _tasks.length;
  document.getElementById('badge-workers').textContent = _workers.length;
}

// ---------------------------------------------------------------------------
// Navigation
// ---------------------------------------------------------------------------
function showSection(name, el) {
  ['jobs','tasks','workers','resources'].forEach(s => {
    document.getElementById('section-' + s).style.display = (s === name) ? '' : 'none';
  });
  document.querySelectorAll('.nav-sidebar .nav-link').forEach(l => l.classList.remove('active'));
  if (el) el.classList.add('active');
  document.getElementById('section-title').textContent = name.charAt(0).toUpperCase() + name.slice(1);
  _currentSection = name;
  renderCurrent();
  return false;
}

function renderCurrent() {
  if (_currentSection === 'jobs')      renderJobs();
  if (_currentSection === 'tasks')     renderTasks();
  if (_currentSection === 'workers')   renderWorkers();
  if (_currentSection === 'resources') renderResources();
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function badge(status) {
  const blink = (status === 'RUNNING') ? ' blink' : '';
  return `<span class="badge badge-${status}${blink}">${status}</span>`;
}

function infoBox(icon, color, label, value) {
  return `
    <div class="col-sm-3">
      <div class="info-box">
        <span class="info-box-icon bg-${color}"><i class="${icon}"></i></span>
        <div class="info-box-content">
          <span class="info-box-text">${label}</span>
          <span class="info-box-number">${value}</span>
        </div>
      </div>
    </div>`;
}

function emptyRow(cols, msg) {
  return `<tr><td colspan="${cols}" class="text-center text-muted py-3">${msg}</td></tr>`;
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------
function renderJobs() {
  const c = {RUNNING:0, SUCCESS:0, FAILED:0, PENDING:0};
  _jobs.forEach(j => { if(c[j.status]!==undefined) c[j.status]++; });

  document.getElementById('job-stats').innerHTML =
    infoBox('fas fa-spinner fa-spin', 'warning',   'Running', c.RUNNING) +
    infoBox('fas fa-check',           'success',   'Success', c.SUCCESS) +
    infoBox('fas fa-times',           'danger',    'Failed',  c.FAILED)  +
    infoBox('fas fa-clock',           'secondary', 'Pending', c.PENDING);

  document.getElementById('jobs-tbody').innerHTML = _jobs.length
    ? _jobs.map(j => `
        <tr>
          <td style="font-family:monospace;font-size:0.8em;">${j.job_id}</td>
          <td>${badge(j.status)}</td>
          <td style="font-size:0.8em;">${j.locked_by || '—'}</td>
          <td style="font-size:0.8em;">${j.locked_until || '—'}</td>
        </tr>`).join('')
    : emptyRow(4, 'No jobs found');
}

// ---------------------------------------------------------------------------
// Tasks — tree view grouped by namespace
// ---------------------------------------------------------------------------
function renderTasks() {
  // group tasks by namespace
  const byNs = {};
  _tasks.forEach(t => {
    const ns = t.namespace || '(none)';
    if (!byNs[ns]) byNs[ns] = {};
    byNs[ns][t.id] = {
      id: t.id,
      params: t.params,
      status: t.status,
      update: t.last_update,
      parent: t.parent_id
    };
  });

  const container = document.getElementById('tasks-namespaces');
  if (!Object.keys(byNs).length) {
    container.innerHTML = '<p class="text-muted mt-3">No tasks found.</p>';
    return;
  }

  container.innerHTML = Object.entries(byNs).map(([ns, tasks]) => {
    // find roots — tasks whose parent is null or not in this namespace
    const roots = Object.keys(tasks).filter(tid =>
      !tasks[tid].parent || String(tasks[tid].parent) === 'None' || !(tasks[tid].parent in tasks)
    );

    const rows = roots.map(rid => renderTaskRow(rid, tasks, 0)).join('');

    return `
      <div class="card card-outline mb-3">
        <div class="card-header d-flex justify-content-between align-items-center">
          <h3 class="card-title">
            <i class="fas fa-layer-group mr-2"></i>
            <span style="color:#ffcc00;">📦 ${ns}</span>
          </h3>
          <div>
            <button class="btn btn-xs btn-outline-warning mr-1"
                    onclick="resetNs('${esc(ns)}')">Reset</button>
            <button class="btn btn-xs btn-outline-danger"
                    onclick="deleteNs('${esc(ns)}')">Delete</button>
          </div>
        </div>
        <div class="card-body p-0">
          <div class="table-responsive">
          <table class="table table-sm table-hover mb-0">
            <thead>
              <tr>
                <th>Task ID</th>
                <th>Params</th>
                <th>Status</th>
                <th>Last Update</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table></div>
        </div>
      </div>`;
  }).join('');
}

function renderTaskRow(tid, allTasks, level) {
  if (!(tid in allTasks)) return '';
  const t = allTasks[tid];
  const indent = level > 0
    ? `<span style="font-family:monospace;color:#6f42c1;">${'&nbsp;'.repeat(level * 4)}└─ </span>`
    : '';

  let row = `
    <tr>
      <td style="font-size:0.8em;">
        ${indent}
        <a href="#" onclick="showLogs('${esc(t.id)}')" style="color:#00d4ff;">${t.id}</a>
      </td>
      <td style="font-size:0.78em;">${t.params||'—'}</td>
      <td>${badge(t.status)}</td>
      <td style="font-size:0.78em;">${t.update||'—'}</td>
      <td>
        <button class="btn btn-xs btn-outline-warning mr-1"
                onclick="resetTask('${esc(t.id)}')">Reset</button>
        <button class="btn btn-xs btn-outline-danger"
                onclick="deleteTask('${esc(t.id)}')">Delete</button>
      </td>
    </tr>`;

  // render children
  const children = Object.keys(allTasks).filter(cid =>
    String(allTasks[cid].parent) === String(tid)
  );
  children.forEach(cid => { row += renderTaskRow(cid, allTasks, level + 1); });
  return row;
}

async function resetNs(ns) {
  if (!confirm(`Reset all tasks in namespace "${ns}"?`)) return;
  await fetch(`/api/reset/namespace/${encodeURIComponent(ns)}`, {method:'POST'});
  fetchAll();
}

async function deleteNs(ns) {
  if (!confirm(`Delete all tasks in namespace "${ns}"?`)) return;
  await fetch(`/api/delete/namespace/${encodeURIComponent(ns)}`, {method:'POST'});
  fetchAll();
}

// ---------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------
function renderWorkers() {
  const totalSlots = _workers.reduce((s,w) => s + (w.max_slots||0), 0);
  const freeSlots  = _workers.reduce((s,w) => s + (w.free_slots||0), 0);

  document.getElementById('worker-stats').innerHTML =
    infoBox('fas fa-server',       'success', 'Workers',    _workers.length) +
    infoBox('fas fa-puzzle-piece', 'info',    'Total Slots', totalSlots)     +
    infoBox('fas fa-circle',       'warning', 'Free Slots',  freeSlots)      +
    infoBox('fas fa-minus-circle', 'danger',  'Busy Slots',  totalSlots - freeSlots);

  document.getElementById('workers-tbody').innerHTML = _workers.length
    ? _workers.map(w => `
        <tr>
          <td style="font-family:monospace;font-size:0.85em;">${w.url}</td>
          <td>${badge(w.status||'ALIVE')}</td>
          <td>${w.max_slots}</td>
          <td>${w.free_slots}</td>
          <td style="font-size:0.8em;">${w.last_seen||'—'}</td>
        </tr>`).join('')
    : emptyRow(5, 'No workers registered');
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------
function renderResources() {
  document.getElementById('resource-cards').innerHTML = _resources.length
    ? _resources.map(r => {
        const pct   = r.amount > 0 ? Math.round(r.usage / r.amount * 100) : 0;
        const color = pct > 80 ? 'danger' : pct > 50 ? 'warning' : 'success';
        return `
          <div class="col-sm-4">
            <div class="card card-outline">
              <div class="card-header">
                <h3 class="card-title">${r.name.toUpperCase()}</h3>
                <div class="card-tools">
                  <span class="badge bg-${color}">${pct}%</span>
                </div>
              </div>
              <div class="card-body">
                <div class="d-flex justify-content-between mb-1" style="color:#ccc;">
                  <span>Usage: <b>${r.usage}</b> / ${r.amount}</span>
                  <span>Available: <b>${r.amount - r.usage}</b></span>
                </div>
                <div class="progress progress-sm">
                  <div class="progress-bar bg-${color}" style="width:${pct}%"></div>
                </div>
              </div>
            </div>
          </div>`;
      }).join('')
    : '<div class="col-12"><p class="text-muted mt-3">No resources configured.</p></div>';
}

// ---------------------------------------------------------------------------
// Logs modal
// ---------------------------------------------------------------------------
async function showLogs(taskId) {
  document.getElementById('log-modal-title').textContent = 'Logs: ' + taskId;
  document.getElementById('log-modal-body').innerHTML =
    '<p class="text-muted p-3">Loading...</p>';
  $('#logModal').modal('show');

  try {
    const logs = await fetch(`/api/logs/${encodeURIComponent(taskId)}?limit=200`).then(r => r.json());
    if (!logs.length) {
      document.getElementById('log-modal-body').innerHTML =
        '<p class="text-muted p-3">No logs found.</p>';
      return;
    }
    document.getElementById('log-modal-body').innerHTML = logs.map(e => `
      <div class="log-entry">
        <span class="log-ts">${e.timestamp||''}</span>
        <span class="log-worker">[${e.worker_id||'?'}]</span>
        <pre class="log-line">${escHtml(e.message||'')}</pre>
      </div>`).join('');
  } catch(e) {
    document.getElementById('log-modal-body').innerHTML =
      `<p class="text-danger p-3">Error: ${e}</p>`;
  }
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------
async function resetTask(id) {
  if (!confirm(`Reset task "${id}"?`)) return;
  await fetch(`/api/reset/task/${encodeURIComponent(id)}`, {method:'POST'});
  fetchAll();
}

async function deleteTask(id) {
  if (!confirm(`Delete task "${id}"?`)) return;
  await fetch(`/api/delete/task/${encodeURIComponent(id)}`, {method:'POST'});
  fetchAll();
}

// ---------------------------------------------------------------------------
// Utils
// ---------------------------------------------------------------------------
function esc(s) { return String(s).replace(/'/g, "\\'"); }
function escHtml(s) {
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
fetchAll();
setInterval(fetchAll, 10000);
</script>
</body>
</html>
"""


@app.get('/', response_class=HTMLResponse)
async def console():
    return HTMLResponse(HTML)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log(f"Waluigi Console:")
    log(f"    Binding: {args.bind_address}:{args.port}")
    log(f"    Boss URL: {args.boss_url}")
    log(f"    Catalog URL: {args.catalog_url}")
    uvicorn.run(app, host=args.bind_address, port=args.port)


if __name__ == "__main__":
    main()
