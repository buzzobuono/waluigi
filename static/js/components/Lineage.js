import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';

export default {
  name: 'Lineage',
  components: { BasePage, BasePanel },
  setup() {
    const nsInput    = Vue.ref('');   // e.g. "sales/raw"
    const idInput    = Vue.ref('');   // e.g. "sales_raw"
    const verInput   = Vue.ref('');   // optional
    const upstream   = Vue.ref([]);
    const downstream = Vue.ref([]);
    const current    = Vue.ref(null);
    const loading    = Vue.ref(false);
    const error      = Vue.ref('');
    
    const route = VueRouter.useRoute();
    Vue.onMounted(() => {
      if (route.query.ns)  nsInput.value  = route.query.ns;
      if (route.query.id)  idInput.value  = route.query.id;
      if (route.query.ver) verInput.value = route.query.ver;
      if (route.query.ns && route.query.id) search();
    });
    
    async function search() {
      const ns = nsInput.value.trim();
      const id = idInput.value.trim();
      if (!ns || !id) {
        error.value = 'Namespace and Dataset ID are required.';
        return;
      }
      loading.value    = true;
      error.value      = '';
      upstream.value   = [];
      downstream.value = [];
      current.value    = null;

      try {
        let ver = verInput.value.trim();
        if (!ver) {
          const hist = await api.catalogDatasetHistory(ns, id);
          if (!hist || !hist.length) {
            error.value = 'Dataset not found or no committed versions.';
            return;
          }
          ver = hist[0].version;
          current.value = hist[0];
          verInput.value = ver;
        } else {
          current.value = { namespace: ns, id, version: ver };
        }

        const [up, down] = await Promise.all([
          api.catalogLineageUpstream(ns, id, ver),
          api.catalogLineageDownstream(ns, id, ver),
        ]);
        upstream.value   = up.upstream    || [];
        downstream.value = down.downstream || [];

      } catch(e) {
        error.value = `Error: ${e.message}`;
      } finally {
        loading.value = false;
      }
    }

    function navigateTo(ns, id) {
      nsInput.value  = ns;
      idInput.value  = id;
      verInput.value = '';
      search();
    }

    return {
      nsInput, idInput, verInput,
      upstream, downstream, current,
      loading, error,
      search, navigateTo,
    };
  },

  template: `
    <base-page 
      title="Lineage" 
      subtitle="Explore dataset lineage"
      icon="fas fa-project-diagram">
    
      <template #actions>
         <div class="form-row align-items-end">
            <div class="col-12 col-sm-4">
              <label style="color:#aaa; font-size:0.85em;">Namespace</label>
              <input class="form-control form-control-sm"
                     v-model="nsInput"
                     placeholder="e.g. sales/raw"
                     @keyup.enter="search" />
            </div>
            <div class="col-12 col-sm-4 mt-2 mt-sm-0">
              <label style="color:#aaa; font-size:0.85em;">Dataset ID</label>
              <input class="form-control form-control-sm"
                     v-model="idInput"
                     placeholder="e.g. sales_raw"
                     @keyup.enter="search" />
            </div>
            <div class="col-12 col-sm-2 mt-2 mt-sm-0">
              <label style="color:#aaa; font-size:0.85em;">Version (optional)</label>
              <input class="form-control form-control-sm"
                     v-model="verInput"
                     placeholder="latest"
                     @keyup.enter="search" />
            </div>
            <div class="col-12 col-sm-2 mt-2 mt-sm-0">
              <label style="visibility:hidden; font-size:0.85em;">_</label>
              <button class="btn btn-sm btn-outline-light w-100 d-block"
                      @click="search" :disabled="loading">
                <i class="fas fa-search mr-1"></i>
                {{ loading ? '...' : 'Search' }}
              </button>
            </div>
          </div>
          <div v-if="error" class="text-danger mt-2" style="font-size:0.85em;">{{ error }}</div>
      </template>

      <!-- Lineage graph: upstream | current | downstream -->
      <div v-if="current" class="row">

        <!-- Upstream -->
          <base-panel :no-padding="true">
            <template #title>
                <i class="fas fa-arrow-up mr-2"></i>Upstream
                <span class="badge badge-info ml-2">{{ upstream.length }}</span>
            </template>
              <div v-if="!upstream.length" class="text-muted p-3 text-center">
                No upstream — source dataset
              </div>
              <div v-for="u in upstream" :key="u.namespace+'/'+u.id+'/'+u.version"
                   class="p-3" style="border-bottom:1px solid #3a005a; cursor:pointer;"
                   @click="navigateTo(u.namespace, u.id)">
                <div style="font-size:0.78em; color:#aaa;">{{ u.namespace }}</div>
                <div style="color:#00d4ff; font-family:monospace; font-size:0.85em;">
                  {{ u.id }}
                </div>
                <div style="color:#888; font-size:0.72em; font-family:monospace;">
                  {{ u.version ? u.version : 'live' }}
                </div>
                <div style="margin-top:4px;">
                  <span v-if="u.format" class="badge badge-secondary mr-1">{{ u.format }}</span>
                  <span v-if="u.rows != null" style="font-size:0.75em; color:#aaa;">
                    {{ u.rows.toLocaleString() }} rows
                  </span>
                </div>
              </div>
          </base-panel>

        <!-- Current -->
        <base-panel :no-padding="true">
          <template #title>
             <i class="fas fa-database mr-2"></i>Current
          </template>
             <div style="color:#aaa; font-size:0.8em;">{{ current.namespace }}</div>
              <div style="font-size:1.1em; color:#00d4ff; font-family:monospace;
                          word-break:break-all; margin-top:4px;">
                {{ current.id }}
              </div>
              <div style="color:#888; font-size:0.78em; font-family:monospace; margin-top:6px;">
                {{ current.version ? current.version : '' }}
              </div>
              <div style="margin-top:10px;">
                <span v-if="current.format" class="badge badge-secondary mr-1">
                  {{ current.format }}
                </span>
                <span v-if="current.rows != null" style="font-size:0.82em; color:#aaa;">
                  {{ current.rows.toLocaleString() }} rows
                </span>
              </div>
              <div v-if="current.produced_by_task"
                   style="margin-top:8px; font-size:0.78em; color:#aaa;">
                Task: <code>{{ current.produced_by_task }}</code>
              </div>
              <div v-if="current.hash"
                   style="margin-top:4px; font-size:0.7em; color:#555; font-family:monospace;">
                {{ current.hash.slice(0,16) }}...
              </div>
        </base-panel>

        <!-- Downstream -->
        <div class="col-12 col-md-4 mt-3 mt-md-0">
          <div class="card card-outline h-100">
            <div class="card-header">
              <h3 class="card-title" style="color:#28a745;">
                <i class="fas fa-arrow-down mr-2"></i>Downstream
                <span class="badge badge-success ml-2">{{ downstream.length }}</span>
              </h3>
            </div>
            <div class="card-body p-0">
              <div v-if="!downstream.length" class="text-muted p-3 text-center">
                No downstream — leaf dataset
              </div>
              <div v-for="d in downstream" :key="d.namespace+'/'+d.id+'/'+d.version"
                   class="p-3" style="border-bottom:1px solid #3a005a; cursor:pointer;"
                   @click="navigateTo(d.namespace, d.id)">
                <div style="font-size:0.78em; color:#aaa;">{{ d.namespace }}</div>
                <div style="color:#00d4ff; font-family:monospace; font-size:0.85em;">
                  {{ d.id }}
                </div>
                <div style="color:#888; font-size:0.72em; font-family:monospace;">
                  {{ d.version ? d.version : '' }}
                </div>
                <div style="margin-top:4px;">
                  <span v-if="d.format" class="badge badge-secondary mr-1">{{ d.format }}</span>
                  <span v-if="d.rows != null" style="font-size:0.75em; color:#aaa;">
                    {{ d.rows.toLocaleString() }} rows
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>

      </div>

      <!-- Empty state -->
      <div v-else-if="!loading" class="text-center mt-5" style="color:#555;">
        <i class="fas fa-project-diagram"
           style="font-size:3em; margin-bottom:16px; display:block;"></i>
        Enter namespace and dataset ID to explore lineage
      </div>

    </base-page>
  `
};
