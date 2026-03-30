// components/Catalog.js
import { api } from '../api.js';

const { defineComponent, ref, computed } = Vue;

export default defineComponent({
  name: 'Catalog',

  setup() {
    const nsStack    = ref([]);   // breadcrumb: [{path, name}]
    const children   = ref([]);   // child namespaces
    const datasets   = ref([]);   // datasets in current namespace
    const loading    = ref(false);

    // detail panel
    const selNs      = ref(null); // selected dataset namespace
    const selId      = ref(null); // selected dataset id
    const history    = ref([]);
    const metadata   = ref({});
    const detailOpen = ref(false);

    const currentNs = computed(() =>
      nsStack.value.length ? nsStack.value[nsStack.value.length - 1].path : null
    );

    async function loadNamespace(path) {
      loading.value = true;
      try {
        if (!path) {
          children.value = await api.catalogNamespaces();
          datasets.value = [];
        } else {
          const [nsData, dsData] = await Promise.all([
            api.catalogNsChildren(path),
            api.catalogNsDatasets(path, false),
          ]);
          children.value = nsData.children || [];
          datasets.value = dsData.datasets || [];
        }
      } catch(e) {
        console.error('Catalog load error', e);
        children.value = [];
        datasets.value = [];
      } finally {
        loading.value = false;
      }
    }

    async function openDataset(ns, id) {
      selNs.value      = ns;
      selId.value      = id;
      detailOpen.value = true;
      history.value    = [];
      metadata.value   = {};
      try {
        const [h, m] = await Promise.all([
          api.catalogDatasetHistory(ns, id),
          api.catalogDatasetMetadata(ns, id),
        ]);
        history.value  = Array.isArray(h) ? h : [];
        metadata.value = m || {};
      } catch(e) {
        console.error('Dataset detail error', e);
      }
    }

    function navigateTo(ns) {
      nsStack.value.push({ path: ns.path, name: ns.name });
      loadNamespace(ns.path);
    }

    function navigateBreadcrumb(idx) {
      if (idx < 0) {
        nsStack.value = [];
        loadNamespace(null);
      } else {
        nsStack.value = nsStack.value.slice(0, idx + 1);
        loadNamespace(nsStack.value[idx].path);
      }
    }

    function closeDetail() {
      detailOpen.value = false;
      selNs.value = null;
      selId.value = null;
    }

    loadNamespace(null);

    return {
      nsStack, children, datasets, loading,
      selNs, selId, history, metadata, detailOpen,
      currentNs,
      navigateTo, navigateBreadcrumb, openDataset, closeDetail,
    };
  },

  template: `
    <div class="row">

      <!-- Left: namespace tree + dataset list -->
      <div :class="detailOpen ? 'col-md-5' : 'col-12'">

        <!-- Breadcrumb -->
        <ol class="breadcrumb" style="background:transparent; padding:0; margin-bottom:12px;">
          <li class="breadcrumb-item">
            <a href="#" @click.prevent="navigateBreadcrumb(-1)" style="color:#d080ff;">🏠 root</a>
          </li>
          <li v-for="(crumb, idx) in nsStack" :key="crumb.path"
              :class="['breadcrumb-item', idx===nsStack.length-1 ? 'active' : '']">
            <a v-if="idx < nsStack.length-1"
               href="#" @click.prevent="navigateBreadcrumb(idx)"
               style="color:#d080ff;">{{ crumb.name }}</a>
            <span v-else style="color:#e0e0e0;">{{ crumb.name }}</span>
          </li>
        </ol>

        <!-- Child namespaces -->
        <div v-if="children.length" class="card card-outline mb-3">
          <div class="card-header">
            <h3 class="card-title"><i class="fas fa-folder-open mr-2"></i>Namespaces</h3>
          </div>
          <div class="card-body p-0">
            <div class="table-responsive">
              <table class="table table-sm table-hover mb-0">
                <thead><tr><th>Name</th><th>Description</th></tr></thead>
                <tbody>
                  <tr v-for="ns in children" :key="ns.path"
                      style="cursor:pointer;" @click="navigateTo(ns)">
                    <td style="color:#d080ff;">
                      <i class="fas fa-folder mr-2"></i>{{ ns.name }}
                    </td>
                    <td style="font-size:0.82em; color:#aaa;">{{ ns.description || '—' }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <!-- Datasets -->
        <div v-if="currentNs" class="card card-outline">
          <div class="card-header">
            <h3 class="card-title"><i class="fas fa-database mr-2"></i>Datasets</h3>
          </div>
          <div class="card-body p-0">
            <div v-if="loading" class="text-muted p-3">Loading...</div>
            <div v-else-if="!datasets.length" class="text-muted p-3">No datasets in this namespace.</div>
            <div v-else class="table-responsive">
              <table class="table table-sm table-hover mb-0">
                <thead>
                  <tr><th>ID</th><th>Format</th><th>Rows</th><th>Committed</th></tr>
                </thead>
                <tbody>
                  <tr v-for="d in datasets" :key="d.namespace + '/' + d.id"
                      style="cursor:pointer;"
                      :class="selId===d.id && selNs===d.namespace ? 'table-active' : ''"
                      @click="openDataset(d.namespace, d.id)">
                    <td style="color:#00d4ff; font-family:monospace; font-size:0.82em;">{{ d.id }}</td>
                    <td><span class="badge badge-secondary">{{ d.format || '—' }}</span></td>
                    <td style="font-size:0.82em;">{{ d.rows != null ? d.rows.toLocaleString() : '—' }}</td>
                    <td style="font-size:0.78em;">{{ d.committed_at ? d.committed_at.slice(0,19) : '—' }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <div v-if="!currentNs && !children.length && !loading" class="text-muted mt-3">
          No namespaces found. Run <code>wlcatalog --scan</code> to populate the catalog.
        </div>

      </div>

      <!-- Right: dataset detail -->
      <div v-if="detailOpen" class="col-md-7">
        <div class="card card-outline">
          <div class="card-header d-flex justify-content-between align-items-center">
            <h3 class="card-title">
              <i class="fas fa-database mr-2"></i>
              <span style="color:#aaa; font-size:0.85em;">{{ selNs }}/</span>
              <code style="color:#00d4ff;">{{ selId }}</code>
            </h3>
            <button class="btn btn-xs btn-outline-secondary" @click="closeDetail">
              <i class="fas fa-times"></i>
            </button>
          </div>
          <div class="card-body p-0">

            <!-- Custom metadata -->
            <div v-if="Object.keys(metadata).length"
                 class="p-3" style="border-bottom:1px solid #3a005a;">
              <h6 style="color:#d080ff; margin-bottom:8px;">Custom Metadata</h6>
              <div v-for="(val, key) in metadata" :key="key"
                   style="font-size:0.85em; margin-bottom:4px;">
                <span style="color:#aaa;">{{ key }}:</span>
                <span class="ml-2">{{ val }}</span>
              </div>
            </div>

            <!-- Version history -->
            <div class="table-responsive">
              <table class="table table-sm mb-0">
                <thead>
                  <tr>
                    <th>Version</th>
                    <th>Format</th>
                    <th>Rows</th>
                    <th>Hash</th>
                    <th>Task</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-if="!history.length">
                    <td colspan="6" class="text-muted text-center py-3">No versions found</td>
                  </tr>
                  <tr v-for="v in history" :key="v.version">
                    <td style="font-family:monospace; font-size:0.75em;">
                      <router-link
                        v-if="v.version"
                        :to="{ path: '/lineage', query: { ns: selNs, id: selId, ver: v.version } }"
                        style="color:#d080ff;">
                        {{ v.version }}
                      </router-link>
                      <span v-else>—</span>
                    </td>
                    <td><span class="badge badge-secondary">{{ v.format || '—' }}</span></td>
                    <td style="font-size:0.82em;">
                      {{ v.rows != null ? v.rows.toLocaleString() : '—' }}
                    </td>
                    <td style="font-family:monospace; font-size:0.72em; color:#888;">
                      {{ v.hash ? v.hash.slice(0,8) : '—' }}
                    </td>
                    <td style="font-size:0.75em; color:#aaa;">{{ v.produced_by_task || '—' }}</td>
                    <td>
                      <span :class="['badge', v.status==='committed' ? 'badge-SUCCESS' : 'badge-PENDING']">
                        {{ v.status }}
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
