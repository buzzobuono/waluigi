
const AUTH_KEY = 'waluigi_auth_token';

export function getToken()    { return localStorage.getItem(AUTH_KEY); }
export function setToken(t)   { localStorage.setItem(AUTH_KEY, t); }
export function clearToken()  { localStorage.removeItem(AUTH_KEY); }

export function getUserNamespaces() {
  try {
    const p = JSON.parse(atob(getToken().split('.')[1]));
    return p.namespaces;   // "*" or string[]
  } catch { return []; }
}

function _authHeaders(extra = {}) {
  const token = getToken();
  return token
    ? { 'Authorization': `Bearer ${token}`, ...extra }
    : { ...extra };
}

async function _handle(r, url) {
  if (r.status === 401) {
    clearToken();
    window.location.href = '/login';
    throw new Error(`Unauthorized`);
  }
  if (!r.ok) {
    let msg = `${url} → ${r.status}`;
    try {
      const body = await r.json();
      if (body?.diagnostic?.messages?.length) msg = body.diagnostic.messages[0];
      else if (body?.detail) msg = body.detail;
    } catch {}
    const err = new Error(msg);
    err.status = r.status;
    throw err;
  }
  return r.json();
}

async function _get(url, params) {
  const qs = params ? '?' + new URLSearchParams(params).toString() : '';
  const r = await fetch(url + qs, { headers: _authHeaders() });
  return _handle(r, url);
}

async function _post(url) {
  const r = await fetch(url, { method: 'POST', headers: _authHeaders() });
  return _handle(r, url);
}

async function _postJson(url, data) {
  const r = await fetch(url, {
    method: 'POST',
    headers: _authHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify(data),
  });
  return _handle(r, url);
}

async function _patchJson(url, data) {
  const r = await fetch(url, {
    method: 'PATCH',
    headers: _authHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify(data),
  });
  return _handle(r, url);
}

async function _putJson(url, data) {
  const r = await fetch(url, {
    method: 'PUT',
    headers: _authHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify(data),
  });
  return _handle(r, url);
}

async function _delete(url) {
  const r = await fetch(url, { method: 'DELETE', headers: _authHeaders() });
  return _handle(r, url);
}

function _enc(s)    { return encodeURIComponent(s); }
function _unwrap(r) { return r?.data !== undefined ? r.data : r; }

export const api = {
  // ── Auth / Users ──────────────────────────────────────────────────────────
  adminUsers:      ()              => _get('/auth/users'),
  adminCreateUser: (body)          => _postJson('/auth/users', body),
  adminUpdateUser: (userid, body)  => _patchJson(`/auth/users/${_enc(userid)}`, body),
  adminUpsertUser: (userid, body)  => _putJson(`/auth/users/${_enc(userid)}`, body),
  adminDeleteUser: (userid)        => _delete(`/auth/users/${_enc(userid)}`),

  login: async (username, password) => {
    const r = await fetch('/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });
    if (!r.ok) throw new Error('Invalid credentials');
    return r.json();
  },

  // ── Boss — Namespaces ─────────────────────────────────────────────────────
  namespaces:        ()   => _get('/boss/namespaces').then(_unwrap),
  namespace:         (ns) => _get(`/boss/namespaces/${_enc(ns)}`).then(_unwrap),
  resetNamespace:    (ns) => _post(`/boss/namespaces/${_enc(ns)}/_reset`).then(_unwrap),
  deleteNamespace:   (ns) => _delete(`/boss/namespaces/${_enc(ns)}`).then(_unwrap),

  // ── Boss — Jobs (namespace-scoped) ────────────────────────────────────────
  jobs:      (ns)        => _get(`/boss/namespaces/${_enc(ns)}/jobs`).then(_unwrap),
  job:       (ns, jobId) => _get(`/boss/namespaces/${_enc(ns)}/jobs/${_enc(jobId)}`).then(_unwrap),
  pauseJob:  (ns, jobId) => _post(`/boss/namespaces/${_enc(ns)}/jobs/${_enc(jobId)}/_pause`).then(_unwrap),
  resumeJob: (ns, jobId) => _post(`/boss/namespaces/${_enc(ns)}/jobs/${_enc(jobId)}/_resume`).then(_unwrap),
  resetJob:  (ns, jobId) => _post(`/boss/namespaces/${_enc(ns)}/jobs/${_enc(jobId)}/_reset`).then(_unwrap),
  cancelJob: (ns, jobId) => _post(`/boss/namespaces/${_enc(ns)}/jobs/${_enc(jobId)}/_cancel`).then(_unwrap),
  deleteJob: (ns, jobId) => _delete(`/boss/namespaces/${_enc(ns)}/jobs/${_enc(jobId)}`).then(_unwrap),

  // ── Boss — Tasks (namespace-scoped) ───────────────────────────────────────
  tasks:     (ns)        => _get(`/boss/namespaces/${_enc(ns)}/tasks`).then(_unwrap),
  jobTasks:  (ns, jobId) => _get(`/boss/namespaces/${_enc(ns)}/tasks`, { job_id: jobId }).then(_unwrap),
  resetTask: (ns, id)    => _post(`/boss/namespaces/${_enc(ns)}/tasks/${_enc(id)}/_reset`).then(_unwrap),
  logs:      (ns, taskId, limit = 100) =>
    _get(`/boss/namespaces/${_enc(ns)}/tasks/${_enc(taskId)}/logs`, { limit }).then(_unwrap),

  // ── Boss — Task Definitions (namespace-scoped) ───────────────────────────
  taskDefinitions:      (ns)       => _get(`/boss/namespaces/${_enc(ns)}/task-definitions`).then(_unwrap),
  upsertTaskDefinition: (ns, body) => _postJson(`/boss/namespaces/${_enc(ns)}/task-definitions`, body).then(_unwrap),
  deleteTaskDefinition: (ns, id)   => _delete(`/boss/namespaces/${_enc(ns)}/task-definitions/${_enc(id)}`).then(_unwrap),

  // ── Boss — Job Definitions (namespace-scoped) ────────────────────────────
  jobDefinitions:      (ns)       => _get(`/boss/namespaces/${_enc(ns)}/job-definitions`).then(_unwrap),
  deleteJobDefinition: (ns, id)   => _delete(`/boss/namespaces/${_enc(ns)}/job-definitions/${_enc(id)}`).then(_unwrap),

  // ── Boss — Cron Jobs (namespace-scoped) ──────────────────────────────────
  cronJobs:       (ns)        => _get(`/boss/namespaces/${_enc(ns)}/cron-jobs`).then(_unwrap),
  upsertCronJob:  (ns, body)  => _postJson(`/boss/namespaces/${_enc(ns)}/cron-jobs`, body).then(_unwrap),
  deleteCronJob:  (ns, id)    => _delete(`/boss/namespaces/${_enc(ns)}/cron-jobs/${_enc(id)}`).then(_unwrap),
  enableCronJob:  (ns, id)    => _post(`/boss/namespaces/${_enc(ns)}/cron-jobs/${_enc(id)}/_enable`).then(_unwrap),
  disableCronJob: (ns, id)    => _post(`/boss/namespaces/${_enc(ns)}/cron-jobs/${_enc(id)}/_disable`).then(_unwrap),

  // ── Boss — Cluster ────────────────────────────────────────────────────────
  workers:        ()          => _get('/boss/workers').then(_unwrap),
  resources:      (ns)        => _get(`/boss/namespaces/${_enc(ns)}/resources`).then(_unwrap),
  applyResources: (ns, spec)  => _postJson(`/boss/namespaces/${_enc(ns)}/resources`, { kind: 'NamespaceResources', spec }).then(_unwrap),

  // ── Boss — Secrets (namespace-scoped) ────────────────────────────────────
  secrets:       (ns)              => _get(`/boss/namespaces/${_enc(ns)}/secrets`).then(_unwrap),
  secretKeys:    (ns, name)        => _get(`/boss/namespaces/${_enc(ns)}/secrets/${_enc(name)}`).then(_unwrap),
  upsertSecret:  (ns, name, data)  => _postJson(`/boss/namespaces/${_enc(ns)}/secrets/${_enc(name)}`, data).then(_unwrap),
  deleteSecret:  (ns, name)        => _delete(`/boss/namespaces/${_enc(ns)}/secrets/${_enc(name)}`).then(_unwrap),

  // ── Catalog ───────────────────────────────────────────────────────────────
  // All namespace-scoped catalog APIs take `ns` (namespace) as first argument.
  // Global APIs (DQ rules/suite) remain namespace-free.

  catalogSources:       (ns)      => _get(`/catalog/namespaces/${_enc(ns)}/sources`),
  catalogCreateSource:  (ns, body)  => _postJson(`/catalog/namespaces/${_enc(ns)}/sources`, body),
  catalogUpdateSource:  (ns, id, body) => _patchJson(`/catalog/namespaces/${_enc(ns)}/sources/${_enc(id)}`, body),
  catalogDeleteSource:  (ns, id)    => _delete(`/catalog/namespaces/${_enc(ns)}/sources/${_enc(id)}`),

  catalogFolders:  (ns, prefix) => _get(`/catalog/namespaces/${_enc(ns)}/folders/${encodeURIComponent(prefix)}/`),
  catalogCreateDataset: (ns, body) => _postJson(`/catalog/namespaces/${_enc(ns)}/datasets`, body),
  catalogDataset:          (ns, id) => _get(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}`),
  catalogDatasetUpdate:    (ns, id, body) => _patchJson(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}`, body),
  catalogDatasetVersions:  (ns, id) => _get(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/versions`),
  catalogDatasetMetadata: (ns, id, ver) => _get(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/versions/${_enc(ver)}/metadata`),
  catalogDatasetPreview: (ns, id, ver, limit = 10, offset = 0) => _get(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/_preview/${_enc(ver)}`, { limit, offset }),
  catalogDatasetSchema:  (ns, id) => _get(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/schema`),
  catalogSchemaUpdateColumn: (ns, id, col, body) => _patchJson(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/schema/${_enc(col)}`, body),
  catalogSchemaApproveColumn: (ns, id, col, publisher = 'admin') => _post(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/schema/${_enc(col)}/approve?publisher=${_enc(publisher)}`),
  catalogSchemaDeleteColumn:  (ns, id, col) => _delete(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/schema/${_enc(col)}`),
  catalogSchemaPublish:  (ns, id, body) => _postJson(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/schema/publish`, body),
  catalogDatasetLineage:   (ns, id, ver) => _get(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/lineage/${_enc(ver)}`),

  dqRules: ()       => _get('/catalog/dq/rules'),
  dqSuite: (path)   => _get('/catalog/dq/suite', { path }),

  datasetCharts:       (ns, id)              => _get(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/charts`),
  addChart:            (ns, id, body)        => _postJson(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/charts`, body),
  updateChart:         (ns, id, cid, body)   => _patchJson(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/charts/${cid}`, body),
  deleteChart:         (ns, id, cid)         => _delete(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/charts/${cid}`),
  renderChart:         (ns, id, cid, ver)    => _get(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/charts/${cid}/render`, ver ? { version: ver } : {}),
  renderChartByKey:    (ns, id, key, ver)    => _get(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/charts/_render`, { key, ...(ver ? { version: ver } : {}) }),

  datasetDQResults:    (ns, id)              => _get(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/dq`),
  datasetDQResult:     (ns, id, ver)         => _get(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/dq/${_enc(ver)}`),

  catalogMaterialize:  (ns, id, body)        => _postJson(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/_materialize`, body),

  datasetExpectations: (ns, id)              => _get(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/expectations`),
  addExpectation:      (ns, id, body)        => _postJson(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/expectations`, body),
  updateExpectation:   (ns, id, expId, body) => _patchJson(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/expectations/${expId}`, body),
  deleteExpectation:   (ns, id, expId)       => _delete(`/catalog/namespaces/${_enc(ns)}/datasets/${_enc(id)}/expectations/${expId}`),
};
