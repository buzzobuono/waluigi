export const TASK_STATUS = {
  PENDING:   { color: 'secondary', icon: 'fas fa-clock' },
  READY:     { color: 'info',      icon: 'fas fa-check-circle' },
  RUNNING:   { color: 'warning',   icon: 'fas fa-spinner fa-spin' },
  SUCCESS:   { color: 'success',   icon: 'fas fa-check' },
  FAILED:    { color: 'danger',    icon: 'fas fa-times' },
  CANCELLED: { color: 'dark',      icon: 'fas fa-ban' },
};

export const JOB_STATUS = {
  PENDING:   { color: 'secondary', icon: 'fas fa-clock' },
  RUNNING:   { color: 'warning',   icon: 'fas fa-spinner fa-spin' },
  PAUSED:    { color: 'info',      icon: 'fas fa-pause' },
  SUCCESS:   { color: 'success',   icon: 'fas fa-check' },
  FAILED:    { color: 'danger',    icon: 'fas fa-times' },
  CANCELLED: { color: 'dark',      icon: 'fas fa-ban' },
};

export const TASK_STATUSES = Object.entries(TASK_STATUS).map(([key, v]) => ({ key, ...v }));
export const JOB_STATUSES  = Object.entries(JOB_STATUS).map(([key, v]) => ({ key, ...v }));
