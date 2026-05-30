
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
    throw new Error(msg);
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
  namespaces:      ()   => _get('/boss/namespaces').then(_unwrap),
  resetNamespace:  (ns) => _post(`/boss/namespaces/${_enc(ns)}/_reset`).then(_unwrap),
  deleteNamespace: (ns) => _delete(`/boss/namespaces/${_enc(ns)}`).then(_unwrap),

  // ── Boss — Jobs (namespace-scoped) ────────────────────────────────────────
  jobs:      (ns)        => _get(`/boss/namespaces/${_enc(ns)}/jobs`).then(_unwrap),
  pauseJob:  (ns, jobId) => _post(`/boss/namespaces/${_enc(ns)}/jobs/${_enc(jobId)}/_pause`).then(_unwrap),
  resumeJob: (ns, jobId) => _post(`/boss/namespaces/${_enc(ns)}/jobs/${_enc(jobId)}/_resume`).then(_unwrap),
  resetJob:  (ns, jobId) => _post(`/boss/namespaces/${_enc(ns)}/jobs/${_enc(jobId)}/_reset`).then(_unwrap),
  cancelJob: (ns, jobId) => _post(`/boss/namespaces/${_enc(ns)}/jobs/${_enc(jobId)}/_cancel`).then(_unwrap),
  deleteJob: (ns, jobId) => _delete(`/boss/namespaces/${_enc(ns)}/jobs/${_enc(jobId)}`).then(_unwrap),

  // ── Boss — Tasks (namespace-scoped) ───────────────────────────────────────
  tasks:     (ns)        => _get(`/boss/namespaces/${_enc(ns)}/tasks`).then(_unwrap),
  jobTasks:  (ns, jobId) => _get(`/boss/namespaces/${_enc(ns)}/tasks`, { job_id: jobId }).then(_unwrap),
  resetTask: (ns, id)    => _post(`/boss/namespaces/${_enc(ns)}/tasks/${_enc(id)}/_reset`).then(_unwrap),
  deleteTask: (ns, id)   => _delete(`/boss/namespaces/${_enc(ns)}/tasks/${_enc(id)}`).then(_unwrap),
  logs:      (ns, taskId, limit = 100) =>
    _get(`/boss/namespaces/${_enc(ns)}/tasks/${_enc(taskId)}/logs`, { limit }).then(_unwrap),

  // ── Boss — Cluster ────────────────────────────────────────────────────────
  workers:   () => _get('/boss/workers').then(_unwrap),
  resources: () => _get('/boss/resources').then(_unwrap),

  // ── Catalog ───────────────────────────────────────────────────────────────
  catalogSources:       ()      => _get('/catalog/sources'),
  catalogCreateSource:  (body)  => _postJson('/catalog/sources', body),
  catalogUpdateSource:  (id, body) => _patchJson(`/catalog/sources/${_enc(id)}`, body),
  catalogDeleteSource:  (id)    => _delete(`/catalog/sources/${_enc(id)}`),

  catalogFolders:  (prefix) => _get(`/catalog/folders/${_enc(prefix)}/`),
  catalogCreateDataset: (body) => _postJson('/catalog/datasets', body),
  catalogDataset:          (id) => _get(`/catalog/datasets/${_enc(id)}`),
  catalogDatasetUpdate:    (id, body) => _patchJson(`/catalog/datasets/${_enc(id)}`, body),
  catalogDatasetVersions:  (id) => _get(`/catalog/datasets/${_enc(id)}/versions`),
  catalogDatasetMetadata: (id, ver) => _get(`/catalog/datasets/${_enc(id)}/versions/${_enc(ver)}/metadata`),
  catalogDatasetPreview: (id, ver, limit = 10, offset = 0) => _get(`/catalog/datasets/${_enc(id)}/_preview/${_enc(ver)}`, { limit, offset }),
  catalogDatasetSchema:  (id) => _get(`/catalog/datasets/${_enc(id)}/schema`),
  catalogSchemaUpdateColumn: (id, col, body) => _patchJson(`/catalog/datasets/${_enc(id)}/schema/${_enc(col)}`, body),
  catalogSchemaApproveColumn: (id, col, publisher = 'admin') => _post(`/catalog/datasets/${_enc(id)}/schema/${_enc(col)}/approve?publisher=${_enc(publisher)}`),
  catalogSchemaDeleteColumn:  (id, col) => _delete(`/catalog/datasets/${_enc(id)}/schema/${_enc(col)}`),
  catalogSchemaPublish:  (id, body) => _postJson(`/catalog/datasets/${_enc(id)}/schema/publish`, body),
  catalogDatasetLineage:   (id, ver) => _get(`/catalog/datasets/${_enc(id)}/lineage/${_enc(ver)}`),

  dqRules: ()       => _get('/catalog/dq/rules'),
  dqSuite: (path)   => _get('/catalog/dq/suite', { path }),

  datasetCharts:       (id)              => _get(`/catalog/datasets/${_enc(id)}/charts`),
  addChart:            (id, body)        => _postJson(`/catalog/datasets/${_enc(id)}/charts`, body),
  updateChart:         (id, cid, body)   => _patchJson(`/catalog/datasets/${_enc(id)}/charts/${cid}`, body),
  deleteChart:         (id, cid)         => _delete(`/catalog/datasets/${_enc(id)}/charts/${cid}`),
  renderChart:         (id, cid, ver)    => _get(`/catalog/datasets/${_enc(id)}/charts/${cid}/render`, ver ? { version: ver } : {}),
  renderChartByKey:    (id, key, ver)    => _get(`/catalog/datasets/${_enc(id)}/charts/_render`, { key, ...(ver ? { version: ver } : {}) }),

  datasetDQResults:    (id)              => _get(`/catalog/datasets/${_enc(id)}/dq`),
  datasetDQResult:     (id, ver)         => _get(`/catalog/datasets/${_enc(id)}/dq/${_enc(ver)}`),

  catalogMaterialize:  (id, body)        => _postJson(`/catalog/datasets/${_enc(id)}/materialize`, body),

  datasetExpectations: (id)              => _get(`/catalog/datasets/${_enc(id)}/expectations`),
  addExpectation:      (id, body)        => _postJson(`/catalog/datasets/${_enc(id)}/expectations`, body),
  updateExpectation:   (id, expId, body) => _patchJson(`/catalog/datasets/${_enc(id)}/expectations/${expId}`, body),
  deleteExpectation:   (id, expId)       => _delete(`/catalog/datasets/${_enc(id)}/expectations/${expId}`),
};
