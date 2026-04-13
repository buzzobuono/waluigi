
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

function _encNs(s)  { return String(s); }
function _encId(s)  { return encodeURIComponent(s); }
function _encVer(s) { return String(s).replace(/:/g, '%3A'); }

export const api = {
  jobs:      () => _get('/api/jobs'),
  deleteJob: (jobId) => _delete(`/api/jobs/${encodeURIComponent(jobId)}`),
  jobTasks: (jobId) => _get(`/api/jobs/${encodeURIComponent(jobId)}/tasks`),

  namespaces: () => _get('/api/namespaces'),
  resetNamespace:  (ns) => _post(`/api/reset/namespace/${encodeURIComponent(ns)}`),
  deleteNamespace: (ns) => _post(`/api/delete/namespace/${encodeURIComponent(ns)}`),

  tasks:     () => _get('/api/tasks'),
  resetTask:       (id) => _post(`/api/reset/task/${encodeURIComponent(id)}`),
  deleteTask:      (id) => _post(`/api/delete/task/${encodeURIComponent(id)}`),
  logs:      (taskId, limit = 100) => _get(`/api/logs/${encodeURIComponent(taskId)}`, { limit }),

  workers:   () => _get('/api/workers'),

  resources: () => _get('/api/resources'),
  
  catalogNamespaces:  () => _get('/catalog/namespaces'),
  catalogNsChildren:  (ns) => _get(`/catalog/namespaces/${_encNs(ns)}/children`),
  catalogNsDatasets:  (ns, recursive = false) => _get(`/catalog/namespaces/${_encNs(ns)}/datasets`, { recursive }),

  catalogFolders:  (prefix) => _get(`/catalog/folders/${_encodeURIComponent(prefix)}/`),
    
  datasetPreview: (ns, id, ver, limit = 10, offset = 0) => _get(`/catalog/datasets/${_encNs(ns)}/${_encId(id)}/${_encVer(ver)}/preview`, { limit, offset }),

  catalogDatasetHistory:  (ns, id)        => _get(`/catalog/datasets/${_encNs(ns)}/${_encId(id)}/history`),
  catalogDatasetMetadata: (ns, id)        => _get(`/catalog/datasets/${_encNs(ns)}/${_encId(id)}/metadata`),
  catalogDataset:         (ns, id)        => _get(`/catalog/datasets/${_encNs(ns)}/${_encId(id)}/latest`),

  catalogLineageUpstream:   (ns, id, ver) => _get(`/catalog/lineage/${_encNs(ns)}/${_encId(id)}/${_encVer(ver)}`),
  catalogLineageDownstream: (ns, id, ver) => _get(`/catalog/lineage/${_encNs(ns)}/${_encId(id)}/${_encVer(ver)}/downstream`),
  
};