import { api } from '../api.js';

export default {
  name: 'LogModal',
  setup() {
    const taskId = Vue.ref('');
    const logs = Vue.ref([]);
    const loading = Vue.ref(false);
    const error = Vue.ref('');

    const show = async (id) => {
      taskId.value = id;
      logs.value = [];
      loading.value = true;
      error.value = '';

      // Utilizziamo l'interfaccia jQuery di AdminLTE/Bootstrap per mostrare la modale
      $('#logModal').modal('show');

      try {
        const data = await api.logs(id, 200);
        logs.value = data;
      } catch (e) {
        error.value = `Errore nel caricamento dei log: ${e.message}`;
      } finally {
        loading.value = false;
      }
    };

    return { taskId, logs, loading, error, show };
  },
  template: `
    <div class="modal fade" id="logModal" tabindex="-1" role="dialog" aria-hidden="true">
      <div class="modal-dialog modal-xl" role="document">
        <div class="modal-content bg-dark">
          <div class="modal-header border-secondary">
            <h5 class="modal-title text-white">
              <i class="fas fa-terminal mr-2 text-info"></i>
              Logs: <code class="text-info">{{ taskId }}</code>
            </h5>
            <button type="button" class="close text-white" data-dismiss="modal" aria-label="Close">
              <span aria-hidden="true">&times;</span>
            </button>
          </div>
          <div class="modal-body p-0" style="background:#0d001a; min-height:300px; max-height:70vh; overflow-y:auto;">
            
            <div v-if="loading" class="d-flex align-items-center justify-content-center p-5">
              <i class="fas fa-spinner fa-spin fa-2x text-info"></i>
            </div>

            <div v-else-if="error" class="p-3 text-danger">
              {{ error }}
            </div>

            <div v-else-if="!logs.length" class="p-5 text-center text-muted">
              Nessun log trovato per questo task.
            </div>

            <div v-else class="py-2">
              <div v-for="e in logs" :key="e.id" 
                   style="padding: 2px 15px; border-bottom: 1px solid #1a0033; font-family: 'Courier New', Courier, monospace; font-size: 0.85rem; line-height: 1.4;">
                <span style="color: #666; margin-right: 10px;">{{ e.timestamp }}</span>
                <span style="color: #00ff00; margin-right: 10px;">[{{ e.worker_id || '?' }}]</span>
                <span style="color: #e0e0e0; white-space: pre-wrap; word-break: break-all;">{{ e.message }}</span>
              </div>
            </div>

          </div>
          <div class="modal-footer border-secondary">
            <button type="button" class="btn btn-secondary btn-sm" data-dismiss="modal">Chiudi</button>
          </div>
        </div>
      </div>
    </div>
  `
};
