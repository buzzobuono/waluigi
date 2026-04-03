// components/Namespaces.js
import { api } from '../api.js';

const { defineComponent, ref, computed, onMounted } = Vue;
const { useRouter } = VueRouter; // Assumendo l'uso di vue-router

export default defineComponent({
  name: 'Namespaces',

  setup() {
    const router     = useRouter(); 
    const items      = ref([]);   // Array di {namespace, task_count}
    const loading    = ref(false);
    const filterText = ref('');

    async function load() {
      loading.value = true;
      try {
        const data = await api.namespaces();
        items.value = Array.isArray(data) ? data : [];
      } catch(e) {
        console.error('Namespaces load error', e);
        items.value = [];
      } finally {
        loading.value = false;
      }
    }

    const filteredItems = computed(() => {
      if (!filterText.value) return items.value;
      const q = filterText.value.toLowerCase();
      return items.value.filter(it => it.namespace.toLowerCase().includes(q));
    });

    // Navigazione programmatica al click sulla riga
    function openNamespace(nsName) {
      router.push(`/tasks/${encodeURIComponent(nsName)}`);
    }

    onMounted(load);

    return {
      items, loading, filterText,
      filteredItems, openNamespace, load
    };
  },

  template: `
    <div class="row">
      <div class="col-12">
        
        <div class="card card-outline">
          <div class="card-header d-flex justify-content-between align-items-center">
            <h3 class="card-title">
              <i class="fas fa-layer-group mr-2"></i>Namespaces
            </h3>
            <div class="card-tools d-flex">
              <div class="input-group input-group-sm" style="width: 200px;">
                <input type="text" v-model="filterText" class="form-control" placeholder="Filter...">
              </div>
              <button class="btn btn-xs btn-outline-light ml-2" @click="load">
                <i class="fas fa-sync"></i>
              </button>
            </div>
          </div>

          <div class="card-body p-0">
            <div v-if="loading" class="text-muted p-3">Loading...</div>
            <div v-else-if="!filteredItems.length" class="text-muted p-3">No namespaces found.</div>
            
            <div v-else class="table-responsive">
              <table class="table table-sm table-hover mb-0">
                <thead>
                  <tr>
                    <th style="padding-left:15px;">Namespace</th>
                    <th class="text-center" style="width:100px;">Tasks</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="it in filteredItems" :key="it.namespace"
                      style="cursor:pointer;" @click="openNamespace(it.namespace)">
                    <td style="padding-left:15px;">
                      <i class="fas fa-folder mr-2" style="color:#d080ff;"></i>
                      
                      <router-link :to="'/tasks/' + encodeURIComponent(it.namespace)" 
                                   style="color: rgb(0, 212, 255); font-family: monospace; font-size: 0.82em; text-decoration:none;"
                                   @click.stop>
                        {{ it.namespace }}
                      </router-link>
                    </td>
                    <td class="text-center">
                      <span class="badge badge-secondary" style="background:#4b0082; border:1px solid #d080ff;">
                        {{ it.task_count }}
                      </span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>

      </div>
    </div>
  `
});
