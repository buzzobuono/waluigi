// components/Lineage.js
import { api } from '../api.js';

const { defineComponent, ref } = Vue;

export default defineComponent({
  name: 'Lineage',

  setup() {
    const datasetId  = ref('');
    const version    = ref('');
    const upstream   = ref([]);
    const downstream = ref([]);
    const current    = ref(null);
    const loading    = ref(false);
    const error      = ref('');

    async function search() {
      if (!datasetId.value.trim()) return;
      loading.value = true;
      error.value   = '';
      upstream.value   = [];
      downstream.value = [];
      current.value    = null;

      try {
        // get latest version if not specified
        let ver = version.value.trim();
        if (!ver) {
          const history = await api.catalogDatasetHistory(datasetId.value.trim());
          if (!history || !history.length) {
            error.value = 'Dataset not found or no committed versions.';
            return;
          }
          ver = history[0].version;
          current.value = history[0];
        }

        const [up, down] = await Promise.all([
          api.catalogLineageUpstream(datasetId.value.trim(), ver),
          api.catalogLineageDownstream(datasetId.value.trim(), ver),
        ]);
        upstream.value   = up.upstream   || [];
        downstream.value = down.downstream || [];
        if (!current.value) current.value = { id: datasetId.value, version: ver };

      } catch(e) {
        error.value = `Error: ${e.message}`;
      } finally {
        loading.value = false;
      }
    }

    function navigateTo(id) {
      datasetId.value = id;
      version.value   = '';
      search();
    }

    return {
      datasetId, version, upstream, downstream,
      current, loading, error,
      search, navigateTo,
    };
  },

  template: `
    <div>

      <!-- Search bar -->
      <div class="card card-outline mb-3">
        <div class="card-header">
          <h3 class="card-title"><i class="fas fa-project-diagram mr-2"></i>Lineage Explorer</h3>
        </div>
        <div class="card-body">
          <div class="form-row align-items-end">
            <div class="col-12 col-sm-5">
              <label style="color:#aaa; font-size:0.85em;">Dataset ID</label>
              <input class="form-control form-control-sm"
                     v-model="datasetId"
                     placeholder="e.g. clean_erp"
                     @keyup.enter="search" />
            </div>
            <div class="col-12 col-sm-5 mt-2 mt-sm-0">
              <label style="color:#aaa; font-size:0.85em;">Version (leave blank for latest)</label>
              <input class="form-control form-control-sm"
                     v-model="version"
                     placeholder="e.g. 2026-03-27T10:00:00"
                     @keyup.enter="search" />
            </div>
            <div class="col-12 col-sm-2 mt-2 mt-sm-0">
              <button class="btn btn-sm btn-outline-light w-100"
                      @click="search" :disabled="loading">
                <i class="fas fa-search mr-1"></i>
                {{ loading ? 'Loading...' : 'Search' }}
              </button>
            </div>
          </div>
          <div v-if="error" class="text-danger mt-2" style="font-size:0.85em;">{{ error }}</div>
        </div>
      </div>

      <!-- Lineage graph -->
      <div v-if="current" class="row">

        <!-- Upstream -->
        <div class="col-12 col-md-4">
          <div class="card card-outline h-100">
            <div class="card-header">
              <h3 class="card-title" style="color:#17a2b8;">
                <i class="fas fa-arrow-up mr-2"></i>Upstream
                <span class="badge badge-info ml-2">{{ upstream.length }}</span>
              </h3>
            </div>
            <div class="card-body p-0">
              <div v-if="!upstream.length" class="text-muted p-3 text-center">
                <i class="fas fa-circle" style="font-size:0.6em;"></i>
                No upstream — this is a source dataset
              </div>
              <div v-else>
                <div v-for="u in upstream" :key="u.input_id + u.input_version"
                     class="p-3" style="border-bottom:1px solid #3a005a;">
                  <div style="font-size:0.82em;">
                    <a href="#" @click.prevent="navigateTo(u.input_id)"
                       style="color:#00d4ff; font-family:monospace;">{{ u.input_id }}</a>
                  </div>
                  <div style="color:#888; font-size:0.75em; font-family:monospace;">
                    {{ u.input_version ? u.input_version.slice(0,19) : 'live' }}
                  </div>
                  <div style="font-size:0.78em; color:#aaa; margin-top:4px;">
                    <span v-if="u.namespace" class="badge badge-secondary mr-1">{{ u.namespace }}</span>
                    <span v-if="u.format" class="badge badge-secondary mr-1">{{ u.format }}</span>
                    <span v-if="u.rows != null">{{ u.rows.toLocaleString() }} rows</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Current dataset -->
        <div class="col-12 col-md-4 mt-3 mt-md-0">
          <div class="card card-outline h-100" style="border-color:#d080ff !important;">
            <div class="card-header" style="border-color:#d080ff !important;">
              <h3 class="card-title" style="color:#d080ff;">
                <i class="fas fa-database mr-2"></i>Current
              </h3>
            </div>
            <div class="card-body text-center" style="padding-top:30px;">
              <div style="font-size:1.1em; color:#00d4ff; font-family:monospace; word-break:break-all;">
                {{ current.id }}
              </div>
              <div style="color:#888; font-size:0.8em; margin-top:8px; font-family:monospace;">
                {{ current.version ? current.version.slice(0,19) : '' }}
              </div>
              <div style="margin-top:12px;">
                <span v-if="current.format" class="badge badge-secondary mr-1">{{ current.format }}</span>
                <span v-if="current.namespace" class="badge badge-secondary mr-1">{{ current.namespace }}</span>
                <span v-if="current.rows != null" style="font-size:0.82em; color:#aaa;">
                  {{ current.rows.toLocaleString() }} rows
                </span>
              </div>
              <div v-if="current.produced_by_task" style="margin-top:8px; font-size:0.78em; color:#aaa;">
                Task: <code>{{ current.produced_by_task }}</code>
              </div>
              <div v-if="current.hash" style="margin-top:4px; font-size:0.72em; color:#555; font-family:monospace;">
                {{ current.hash.slice(0,16) }}...
              </div>
            </div>
          </div>
        </div>

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
                <i class="fas fa-circle" style="font-size:0.6em;"></i>
                No downstream — this is a leaf dataset
              </div>
              <div v-else>
                <div v-for="d in downstream" :key="d.output_id + d.output_version"
                     class="p-3" style="border-bottom:1px solid #3a005a;">
                  <div style="font-size:0.82em;">
                    <a href="#" @click.prevent="navigateTo(d.output_id)"
                       style="color:#00d4ff; font-family:monospace;">{{ d.output_id }}</a>
                  </div>
                  <div style="color:#888; font-size:0.75em; font-family:monospace;">
                    {{ d.output_version ? d.output_version.slice(0,19) : '' }}
                  </div>
                  <div style="font-size:0.78em; color:#aaa; margin-top:4px;">
                    <span v-if="d.namespace" class="badge badge-secondary mr-1">{{ d.namespace }}</span>
                    <span v-if="d.format" class="badge badge-secondary mr-1">{{ d.format }}</span>
                    <span v-if="d.rows != null">{{ d.rows.toLocaleString() }} rows</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

      </div>

      <!-- Empty state -->
      <div v-else-if="!loading" class="text-center mt-5" style="color:#555;">
        <i class="fas fa-project-diagram" style="font-size:3em; margin-bottom:16px; display:block;"></i>
        Enter a dataset ID to explore its lineage
      </div>

    </div>
  `
});
