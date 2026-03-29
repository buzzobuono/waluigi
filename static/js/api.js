// api.js — centralized fetch calls
// All endpoints go through the console server proxy → boss

const BASE = '';

async function _get(url) {
  const r = await fetch(BASE + url);
  if (!r.ok) throw new Error(`GET ${url} → ${r.status}`);
  return r.json();
}

async function _post(url) {
  const r = await fetch(BASE + url, { method: 'POST' });
  if (!r.ok) throw new Error(`POST ${url} → ${r.status}`);
  return r.json();
}

export const api = {
  jobs:      () => _get('/api/jobs'),
  tasks:     () => _get('/api/tasks'),
  workers:   () => _get('/api/workers'),
  resources: () => _get('/api/resources'),
  logs:      (taskId, limit = 100) => _get(`/api/logs/${encodeURIComponent(taskId)}?limit=${limit}`),

  resetTask:      (id) => _post(`/api/reset/task/${encodeURIComponent(id)}`),
  deleteTask:     (id) => _post(`/api/delete/task/${encodeURIComponent(id)}`),
  resetNamespace: (ns) => _post(`/api/reset/namespace/${encodeURIComponent(ns)}`),
  deleteNamespace:(ns) => _post(`/api/delete/namespace/${encodeURIComponent(ns)}`),
};
