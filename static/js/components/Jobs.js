// components/Jobs.js
import { api } from '../api.js';

export default {
  name: 'Jobs',
  props: { jobs: Array },
  emits: ['refresh'],
  computed: {
    counts() {
      const c = { RUNNING: 0, SUCCESS: 0, FAILED: 0, PENDING: 0 };
      (this.jobs || []).forEach(j => { if (c[j.status] !== undefined) c[j.status]++; });
      return c;
    }
  },
  methods: {
    async deleteJob(jobId) {
      if (!confirm(`Sei sicuro di voler eliminare il job "${jobId}" e tutti i suoi task?`)) return;
      try {
        await api.deleteJob(jobId);
        this.$emit('refresh');
      } catch (e) {
        alert(`Errore durante l'eliminazione: ${e.message}`);
      }
    }
  },
  template: `
    <div>
      <div class="row mb-3">
        <div class="col-6 col-sm-3" v-for="(val, key) in counts" :key="key">
          <div class="info-box shadow-none border">
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

      <div class="card card-outline">
        <div class="card-header">
          <h3 class="card-title"><i class="fas fa-briefcase mr-2"></i>Jobs</h3>
        </div>
        <div class="card-body p-0">
          <div class="table-responsive">
            <table class="table table-sm table-hover mb-0">
              <thead>
                <tr>
                  <th>Job ID (Click for DAG)</th>
                  <th>Status</th>
                  <th>Locked By</th>
                  <th>Locked Until</th>
                  <th style="width: 80px;">Actions</th>
                </tr>
              </thead>
              <tbody>
                <tr v-if="!jobs || !jobs.length">
                  <td colspan="5" class="text-center text-muted py-3">No jobs found</td>
                </tr>
                
                <tr v-for="j in jobs" :key="j.job_id" 
                    style="cursor:pointer;" 
                    @click="$router.push('/jobs/' + encodeURIComponent(j.job_id))">
                  <td style="font-family:monospace; font-size:0.85em;">
                    <i class="fas fa-project-diagram mr-2 text-muted"></i>
                    <span class="text-primary font-weight-bold">{{ j.job_id }}</span>
                  </td>
                  <td>
                    <span :class="['badge', 'badge-'+j.status, j.status==='RUNNING'?'blink':'']">
                      {{ j.status }}
                    </span>
                  </td>
                  <td style="font-size:0.8em;" class="text-muted">{{ j.locked_by || '—' }}</td>
                  <td style="font-size:0.8em;" class="text-muted">{{ j.locked_until || '—' }}</td>
                  <td class="text-center">
                    <button class="btn btn-xs btn-outline-danger" 
                            title="Delete Job"
                            @click.stop="deleteJob(j.job_id)">
                      <i class="fas fa-trash"></i>
                    </button>
                  </td>
                </tr>

              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  `
};