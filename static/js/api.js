
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

async function _delete(url) {
  const r = await fetch(url, { method: 'DELETE' });
  if (!r.ok) throw new Error(`DELETE ${url} → ${r.status}`);
  return r.json();
}

function _enc(s)  { return encodeURIComponent(s); }

export const api = {
  jobs:      () => _get('/api/jobs'),
  deleteJob: (jobId) => _delete(`/api/jobs/${_enc(jobId)}`),
  jobTasks: (jobId) => _get(`/api/jobs/${_enc(jobId)}/tasks`),

  namespaces: () => _get('/api/namespaces'),
  resetNamespace:  (ns) => _post(`/api/reset/namespace/${_enc(ns)}`),
  deleteNamespace: (ns) => _post(`/api/delete/namespace/${_enc(ns)}`),

  tasks:     () => _get('/api/tasks'),
  resetTask:       (id) => _post(`/api/reset/task/${_enc(id)}`),
  deleteTask:      (id) => _post(`/api/delete/task/${_enc(id)}`),
  logs:      (taskId, limit = 100) => _get(`/api/logs/${_enc(taskId)}`, { limit }),

  workers:   () => _get('/api/workers'),

  resources: () => _get('/api/resources'),
  
  catalogFolders:  (prefix) => _get(`/catalog/folders/${_enc(prefix)}/`),
  catalogDatasetVersions:  (id) => _get(`/catalog/datasets/${_enc(id)}/versions`),
  catalogDatasetMetadata: (id, ver) => _get(`/catalog/datasets/${_enc(id)}/metadata/${_enc(ver)}`),
  catalogDatasetPreview: (id, ver, limit = 10, offset = 0) => _get(`/catalog/datasets/${_enc(id)}/preview/${_enc(ver)}`, { limit, offset }),
  catalogDatasetLineage:   (id, ver) => _get(`/catalog/datasets/${_enc(id)}/lineage/${_enc(ver)}`),
  
};