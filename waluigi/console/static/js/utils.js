export function fmtDt(v) {
  if (!v || v === '-') return '—';
  try { return new Date(v).toLocaleString(); } catch { return v; }
}
