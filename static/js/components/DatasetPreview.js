// components/DatasetPreview.js
import { api } from '../api.js';

const { defineComponent, ref, computed, watch, onMounted } = Vue;

export default defineComponent({
  name: 'DatasetPreview',

  setup() {
    const route = VueRouter.useRoute();
    const columns = ref([]);
    const rows    = ref([]);
    const loading = ref(false);
    const error   = ref(null);
    
    // Stato paginazione
    const currentPage = ref(1);
    const pageSize    = ref(10);

    const params = computed(() => {
      const formatParam = (val) => {
        if (!val) return '';
        const joined = Array.isArray(val) ? val.join('/') : String(val);
        return joined.replace(/^\/|\/$/g, '');
      };

      return {
        namespace: formatParam(route.params.namespace),
        id:        route.params.id,
        version:   formatParam(route.params.version)
      };
    });

    async function loadPreview() {
      const { namespace, id, version } = params.value;
      if (!namespace || !id || !version) return;

      loading.value = true;
      error.value = null;
      try {
        // Calcoliamo l'offset in base alla pagina
        const limit = pageSize.value;
        const offset = (currentPage.value - 1) * limit;
        
        // Passiamo i parametri di paginazione all'API
        const response = await api.datasetPreview(namespace, id, version, limit, offset);
        
        columns.value = response.columns || [];
        rows.value    = response.data || [];
      } catch (e) {
        console.error("Preview load error:", e);
        error.value = "Errore nel caricamento: " + e.message;
      } finally {
        loading.value = false;
      }
    }

    function changePage(delta) {
      const next = currentPage.value + delta;
      if (next > 0) {
        currentPage.value = next;
        loadPreview();
      }
    }

    // Se cambiano i parametri dell'URL, resetta alla pagina 1
    watch(() => params.value, () => {
      currentPage.value = 1;
      loadPreview();
    }, { deep: true });

    onMounted(loadPreview);

    return { 
      columns, rows, loading, error, params, 
      currentPage, changePage 
    };
  },

  template: `
    <div>
        <div class="d-flex align-items-center mb-3">
            <router-link to="/catalog" class="btn btn-xs btn-outline-light mr-3">
                <i class="fas fa-arrow-left mr-1"></i>Back
            </router-link>
        </div>

        <div v-if="loading" class="text-center p-5 text-muted card card-outline">
            <i class="fas fa-sync fa-spin fa-3x mb-3"></i>
            <p>Loading page {{ currentPage }}...</p>
        </div>

        <div v-else-if="error" class="text-center p-5 text-danger card card-outline">
            <i class="fas fa-exclamation-triangle fa-3x mb-3"></i>
            <p>{{ error }}</p>
        </div>

        <div v-else-if="!rows.length" class="text-center p-5 text-muted card card-outline">
            <i class="fas fa-filter fa-3x mb-3"></i>
            <p>No more data available.</p>
        </div>

        <div v-else class="card card-outline mb-4">
            <div class="card-header d-flex justify-content-between align-items-center">
                <h3 class="card-title">
                    <i class="fas fa-table mr-2 text-warning"></i>
                    <span class="font-weight-bold">Data Preview: </span>
                    <span class="ns-header-yellow ml-1 font-weight-bold" style="font-size: 1.1em; font-family: monospace;">
                        {{ params.id }}
                    </span>
                </h3>
                <div class="btn-group">
                    <button class="btn btn-xs btn-outline-warning mr-1" @click="changePage(-1)" :disabled="loading || currentPage <= 1">
                        <i class="fas fa-chevron-left mr-1"></i>Prev
                    </button>
                    <button class="btn btn-xs btn-outline-light mr-1 disabled">
                        Page {{ currentPage }}
                    </button>
                    <button class="btn btn-xs btn-outline-warning" @click="changePage(1)" :disabled="loading || rows.length < 10">
                        Next<i class="fas fa-chevron-right ml-1"></i>
                    </button>
                </div>
            </div>

            <div class="card-body p-0">
                <div class="table-responsive">
                    <table class="table table-sm table-hover mb-0">
                        <thead>
                            <tr>
                                <th v-for="col in columns" :key="col" 
                                    class="pl-3 text-uppercase text-muted small" 
                                    style="border-top: none;">
                                    {{ col }}
                                </th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr v-for="(row, idx) in rows" :key="idx">
                                <td v-for="col in columns" :key="col" 
                                    class="pl-3 info-box-text" 
                                    style="font-family: monospace; font-size: 0.82em;">
                                    <span v-if="row[col] === null" style="font-style: italic; opacity: 0.5;">null</span>
                                    <span v-else>{{ row[col] }}</span>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="card-footer d-flex justify-content-between align-items-center" style="background: transparent; border-top: 1px solid var(--wl-accent);">
                <div class="info-box-text" style="font-size: 0.8em;">
                    <i class="fas fa-info-circle mr-1"></i> 
                    Rows {{ (currentPage-1)*10 + 1 }} - {{ (currentPage-1)*10 + rows.length }}
                </div>
                <div class="info-box-text" style="font-size: 0.8em;">
                    <span>Path: </span>
                    <span style="color: #00d4ff;">{{ params.namespace }}/{{ params.id }}</span>
                    <span class="badge ml-2" style="background: var(--wl-border); color: var(--wl-light); font-size: 0.75em; padding: 0.25em 0.5em;">
                        v.{{ params.version }}
                    </span>
                </div>
            </div>
        </div>
    </div>
  `
});