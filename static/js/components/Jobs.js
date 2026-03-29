// components/Jobs.js
export default {
  name: 'Jobs',
  props: { jobs: Array },
  computed: {
    counts() {
      const c = { RUNNING: 0, SUCCESS: 0, FAILED: 0, PENDING: 0 };
      (this.jobs || []).forEach(j => { if (c[j.status] !== undefined) c[j.status]++; });
      return c;
    }
  },
  methods: {
    statusBadge(status) {
      const blink = status === 'RUNNING' ? ' blink' : '';
      return `<span class="badge badge-${status}${blink}">${status}</span>`;
    }
  },
  template: `
    <div>
      <!-- Stats -->
      <div class="row mb-3">
        <div class="col-6 col-sm-3" v-for="(val, key) in counts" :key="key">
          <div class="info-box">
            <span class="info-box-icon"
                  :class="{
                    'bg-warning': key==='RUNNING',
                    'bg-success': key==='SUCCESS',
                    'bg-danger':  key==='FAILED',
                    'bg-secondary': key==='PENDING'
                  }">
              <i class="fas"
                 :class="{
                   'fa-spinner fa-spin': key==='RUNNING',
                   'fa-check':  key==='SUCCESS',
                   'fa-times':  key==='FAILED',
                   'fa-clock':  key==='PENDING'
                 }"></i>
            </span>
            <div class="info-box-content">
              <span class="info-box-text">{{ key }}</span>
              <span class="info-box-number">{{ val }}</span>
            </div>
          </div>
        </div>
      </div>

      <!-- Table -->
      <div class="card card-outline">
        <div class="card-header">
          <h3 class="card-title"><i class="fas fa-briefcase mr-2"></i>Jobs</h3>
        </div>
        <div class="card-body p-0">
          <div class="table-responsive">
            <table class="table table-sm table-hover mb-0">
              <thead>
                <tr>
                  <th>Job ID</th>
                  <th>Status</th>
                  <th>Locked By</th>
                  <th>Locked Until</th>
                </tr>
              </thead>
              <tbody>
                <tr v-if="!jobs || !jobs.length">
                  <td colspan="4" class="text-center text-muted py-3">No jobs found</td>
                </tr>
                <tr v-for="j in jobs" :key="j.job_id">
                  <td style="font-family:monospace; font-size:0.8em;">{{ j.job_id }}</td>
                  <td><span :class="['badge', 'badge-'+j.status, j.status==='RUNNING'?'blink':'']">{{ j.status }}</span></td>
                  <td style="font-size:0.8em;">{{ j.locked_by || '—' }}</td>
                  <td style="font-size:0.8em;">{{ j.locked_until || '—' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  `
};
