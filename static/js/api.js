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

export const api = {
  jobs:      () => _get('/api/jobs'),
  tasks:     () => _get('/api/tasks'),
  workers:   () => _get('/api/workers'),
  resources: () => _get('/api/resources'),
  logs:      (taskId, limit = 100) => _get(`/api/logs/${encodeURIComponent(taskId)}`, { limit }),

  resetTask:       (id) => _post(`/api/reset/task/${encodeURIComponent(id)}`),
  deleteTask:      (id) => _post(`/api/delete/task/${encodeURIComponent(id)}`),
  resetNamespace:  (ns) => _post(`/api/reset/namespace/${encodeURIComponent(ns)}`),
  deleteNamespace: (ns) => _post(`/api/delete/namespace/${encodeURIComponent(ns)}`),
  
  catalogNamespaces:       ()           => _get('/catalog/namespaces'),
  catalogNsChildren:       (ns)         => _get(`/catalog/namespaces/${encodeURIComponent(ns)}/children`),
  catalogNsDatasets:       (ns, recursive = false) =>
                                           _get(`/catalog/namespaces/${encodeURIComponent(ns)}/datasets`, { recursive }),
  catalogDatasetHistory:   (id)         => _get(`/catalog/datasets/${encodeURIComponent(id)}/history`),
  catalogDatasetMetadata:  (id)         => _get(`/catalog/datasets/${encodeURIComponent(id)}/metadata`),
  catalogLineageUpstream:  (id, ver)    => _get(`/catalog/lineage/${encodeURIComponent(id)}/${encodeURIComponent(ver)}`),
  catalogLineageDownstream:(id, ver)    => _get(`/catalog/lineage/${encodeURIComponent(id)}/${encodeURIComponent(ver)}/downstream`),
};
