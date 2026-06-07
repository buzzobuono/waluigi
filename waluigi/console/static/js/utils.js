export function fmtDt(v) {
  if (!v || v === '-') return '—';
  try {
    const s = String(v);
    // If no timezone marker present, assume UTC (legacy naive strings from DB)
    const hasZone = s.endsWith('Z') || /[+-]\d{2}:?\d{2}$/.test(s);
    return new Date(hasZone ? s : s + 'Z').toLocaleString();
  } catch { return v; }
}
