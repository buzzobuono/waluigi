// components/LogModal.js
import { api } from '../api.js';

export default {
  name: 'LogModal',
  template: `
    <div class="modal fade" id="logModal" tabindex="-1" role="dialog">
      <div class="modal-dialog modal-xl" role="document">
        <div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title">📜 Logs: <code style="color:#00d4ff;">{{ taskId }}</code></h5>
            <button type="button" class="close" data-dismiss="modal"><span>&times;</span></button>
          </div>
          <div class="modal-body p-0"
               style="max-height:65vh; overflow-y:auto; background:#0d001a; font-family:monospace;">
            <div v-if="loading" class="text-muted p-3">Loading...</div>
            <div v-else-if="!logs.length" class="text-muted p-3">No logs found.</div>
            <div v-else>
              <div v-for="e in logs" :key="e.id" class="log-entry">
                <span class="log-ts">{{ e.timestamp }}</span>
                <span class="log-worker">[{{ e.worker_id || '?' }}]</span>
                <span class="log-msg">{{ e.message }}</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  `,
  data() {
    return { taskId: '', logs: [], loading: false };
  },
  methods: {
    async show(taskId) {
      this.taskId = taskId;
      this.logs = [];
      this.loading = true;
      $('#logModal').modal('show');
      try {
        this.logs = await api.logs(taskId, 200);
      } catch(e) {
        console.error('Failed to load logs', e);
      } finally {
        this.loading = false;
      }
    }
  }
};
