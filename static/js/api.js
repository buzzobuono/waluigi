
async function _get(url, params) {
  const qs = params ? '?' + new URLSearchParams(params).toString() : '';
  const r = await fetch(url + qs);
  if (!r.ok) throw new Error(`GET ${url} → ${r.status}`);
  return r.json();
}

async function _post(url) {
  const r = await fetch(url, { method: 'POST' });
  if (!r.ok) throw new Error(`POST ${url} → ${r.status}`);
  return r.json();
}

async function _postJson(url, data) {
  const r = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
  if (!r.ok) throw new Error(`POST ${url} → ${r.status}`);
  return r.json();
}

async function _patchJson(url, data) {
  const r = await fetch(url, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
  if (!r.ok) throw new Error(`PATCH ${url} → ${r.status}`);
  return r.json();
}

async function _delete(url) {
  const r = await fetch(url, { method: 'DELETE' });
  if (!r.ok) throw new Error(`DELETE ${url} → ${r.status}`);
  return r.json();
}

function _enc(s)  { return encodeURIComponent(s); }

export const api = {
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
  catalogDatasetVersions:  (id) => _get(`/catalog/datasets/${_enc(id)}/versions`),
  catalogDatasetMetadata: (id, ver) => _get(`/catalog/datasets/${_enc(id)}/versions/${_enc(ver)}/metadata`),
  catalogDatasetPreview: (id, ver, limit = 10, offset = 0) => _get(`/catalog/datasets/${_enc(id)}/_preview/${_enc(ver)}`, { limit, offset }),
  catalogDatasetSchema:  (id) => _get(`/catalog/datasets/${_enc(id)}/schema`),
  catalogSchemaUpdateColumn: (id, col, body) => _patchJson(`/catalog/datasets/${_enc(id)}/schema/${_enc(col)}`, body),
  catalogSchemaPublish:  (id, body) => _postJson(`/catalog/datasets/${_enc(id)}/schema/publish`, body),
  catalogDatasetLineage:   (id, ver) => _get(`/catalog/datasets/${_enc(id)}/lineage/${_enc(ver)}`),
  
};