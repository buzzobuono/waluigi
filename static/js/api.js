
const AUTH_KEY = 'waluigi_auth_token';

export function getToken()    { return localStorage.getItem(AUTH_KEY); }
export function setToken(t)   { localStorage.setItem(AUTH_KEY, t); }
export function clearToken()  { localStorage.removeItem(AUTH_KEY); }

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

function _enc(s)  { return encodeURIComponent(s); }

export const api = {
  adminUsers:      ()            => _get('/auth/users'),
  adminCreateUser: (body)        => _postJson('/auth/users', body),
  adminDeleteUser: (userid)      => _delete(`/auth/users/${_enc(userid)}`),

  login: async (username, password) => {
    const r = await fetch('/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });
    if (!r.ok) throw new Error('Invalid credentials');
    return r.json();
  },

  jobs:      () => _get('/boss/api/jobs'),
  deleteJob: (jobId) => _delete(`/boss/api/jobs/${_enc(jobId)}`),
  jobTasks: (jobId) => _get(`/boss/api/jobs/${_enc(jobId)}/tasks`),

  namespaces: () => _get('/boss/api/namespaces'),
  resetNamespace:  (ns) => _post(`/boss/api/reset/namespace/${_enc(ns)}`),
  deleteNamespace: (ns) => _post(`/boss/api/delete/namespace/${_enc(ns)}`),

  tasks:     () => _get('/boss/api/tasks'),
  resetTask:       (id) => _post(`/boss/api/reset/task/${_enc(id)}`),
  deleteTask:      (id) => _post(`/boss/api/delete/task/${_enc(id)}`),
  logs:      (taskId, limit = 100) => _get(`/boss/api/logs/${_enc(taskId)}`, { limit }),

  workers:   () => _get('/boss/api/workers'),

  resources: () => _get('/boss/api/resources'),

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
