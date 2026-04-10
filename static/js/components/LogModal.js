import { api } from '../api.js';
import BaseModal from './BaseModal.js';

export default {
  name: 'LogModal',
  components: { BaseModal },
  
  setup() {
    const taskId = Vue.ref('');
    const logs = Vue.ref([]);
    const loading = Vue.ref(false);
    const error = Vue.ref('');
    const logModal = Vue.ref(null);

    const show = async (id) => {
      taskId.value = id;
      logs.value = [];
      loading.value = true;
      error.value = '';

      // ✅ apertura corretta
      logModal.value.open();

      try {
        const data = await api.logs(id, 200);
        logs.value = data;
      } catch (e) {
        error.value = `Errore nel caricamento dei log: ${e.message}`;
      } finally {
        loading.value = false;
      }
    };

    return { taskId, logs, loading, error, show, logModal };
  },

  template: `
    <BaseModal ref="logModal" :body-style="{ background: '#000', padding: '0', overflowX: 'hidden', overflowY: 'auto'}">

      <template #title>
        <i class="fas fa-terminal mr-2 text-info"></i>
        Logs: '{{ taskId }}' task
      </template>

      <div v-if="loading" class="d-flex justify-content-center p-5">
        <i class="fas fa-spinner fa-spin fa-2x text-info"></i>
      </div>
      <div v-else-if="error" class="p-3" style="color:#ff5555;">{{ error }}</div>
      <div v-else-if="!logs.length" class="p-5 text-center" style="color:#555;">Nessun log trovato</div>

      <div v-else style="padding: 8px 0;">
        <div v-for="e in logs" :key="e.id" style=" padding: 1px 15px; border-bottom: 1px solid #0a0a0a; font-family: 'Courier New', Courier, monospace; font-size: 0.82rem; line-height: 1.5;">
          <span style="color:#555; margin-right:10px;">{{ e.timestamp }}</span>
          <span style="color:#00cc00; margin-right:10px;">[{{ e.worker_id || '?' }}]</span>
          <span style="color:#e0e0e0; white-space:pre-wrap; word-break:break-all;">{{ e.message }}</span>
        </div>
      </div>

    </BaseModal>
  `
};
