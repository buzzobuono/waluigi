import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseButton from './BaseButton.js';
import BaseTable from './BaseTable.js';
import Materialize from './Materialize.js';

const { defineComponent, ref, computed, watch, onMounted } = Vue;

export default defineComponent({
  name: 'Catalog',

  components: { BasePage, BasePanel, BaseButton, BaseTable, Materialize },

  setup() {
    const columns = [
      { key: 'name', label: 'Name' },
      { key: 'description', label: 'Description' },
      { key: 'format', label: 'Format' },
      { key: 'committed_at', label: 'Committed' }
    ];
    
    const route  = VueRouter.useRoute();
    const router = VueRouter.useRouter();

    const nsStack    = ref([]);   // breadcrumb: [{path, name}]
    const children   = ref([]);   // child namespaces
    const datasets   = ref([]);   // datasets in current namespace
    const items   = ref([]);
    const loading    = ref(false);
    const materializeRef = ref(null);

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
          const nsData = await api.catalogNamespaces();
          const nsItems = (nsData || []).map(n => ({ ...n, type: 'ns' }));
          children.value = nsItems;
          datasets.value = [];
          items.value = nsItems;
        } else {
          const [nsData, dsData] = await Promise.all([
            api.catalogNsChildren(path),
            api.catalogNsDatasets(path, false),
          ]);
          children.value = nsData.children || [];
          datasets.value = dsData.datasets || [];
          
          const nsItems = (nsData.children || []).map(n => ({ ...n, type: 'ns' }));
          const dsItems = (dsData.datasets || []).map(d => ({ ...d, type: 'ds', name: d.id }));
          items.value = [...nsItems, ...dsItems];
          
        }
      } catch(e) {
        console.error('Catalog load error', e);
        children.value = [];
        datasets.value = [];
        items.value = [];
      } finally {
        loading.value = false;
      }
    }

    async function openDataset(ns, id) {
      router.push({ path: '/catalog', query: { ns, ds: id } });
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
        if (history.value.length > 0) {
            selVersion.value = history.value[0].version;
        }
      } catch(e) {
        console.error('Dataset detail error', e);
      }
    }

    function navigateTo(ns) {
      nsStack.value.push({ path: ns.path, name: ns.name });
      router.push({ path: '/catalog', query: { ns: ns.path } });
      loadNamespace(ns.path);
    }

    function navigateBreadcrumb(idx) {
      if (idx < 0) {
        nsStack.value = [];
        router.push({ path: '/catalog' });
        loadNamespace(null);
      } else {
        nsStack.value = nsStack.value.slice(0, idx + 1);
        const path = nsStack.value[idx].path;
        router.push({ path: '/catalog', query: { ns: path } });
        loadNamespace(path);
      }
    }

    function goBack() {
      router.go(-1);
    }

    function closeDetail() {
      detailOpen.value = false;
      selNs.value = null;
      selId.value = null;
      router.push({ path: '/catalog', query: { ns: currentNs.value || undefined } });
    }

    onMounted(async () => {
      const ns = route.query.ns;
      const ds = route.query.ds;

      if (ns) {
        // ricostruisci il breadcrumb
        const parts = ns.split('/');
        nsStack.value = parts.map((name, i) => ({
          path: parts.slice(0, i + 1).join('/'),
          name
        }));
        await loadNamespace(ns);
      } else {
        await loadNamespace(null);
      }

      if (ds && ns) {
        await openDataset(ns, ds);
      }
    });

    watch(() => route.query, async (q) => {
      const ns = q.ns || null;
      const ds = q.ds || null;

      // risincronizza breadcrumb
      if (ns) {
        const parts = ns.split('/');
        nsStack.value = parts.map((name, i) => ({
          path: parts.slice(0, i + 1).join('/'),
          name
        }));
        await loadNamespace(ns);
      } else {
        nsStack.value = [];
        await loadNamespace(null);
      }

      if (ds && ns) {
        await openDataset(ns, ds);
      } else {
        detailOpen.value = false;
      }
    }, { immediate: true });

    loadNamespace(null);

    return {
      columns, items, nsStack, children, datasets, loading,
      selNs, selId, history, metadata, detailOpen,
      currentNs,
      navigateTo, navigateBreadcrumb, openDataset, closeDetail, goBack, materializeRef,
    };
  },

  template: `
    <base-page 
      title="Catalog" 
      subtitle="Browse datasets"
      icon="fas fa-table"
    >
  
      <template #actions>
  
        <div class="d-flex align-items-center w-100">
          <base-button 
            label="Back" 
            icon="fas fa-arrow-left" 
            color="outline-light" 
            size="sm"
            class="mr-2"
            @click="goBack"
          />
          <base-button 
            label="Materialize" 
            icon="fas fa-cloud-download-alt" 
            color="outline-primary" 
            size="sm"
            class="ml-auto"
            @click="materializeRef && materializeRef.open(currentNs || '')"
          />
        </div>
      </template>
    
      <base-panel
        :no-padding="true">
  
        <template #title>
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
        </template>
  
        <base-table 
          :columns="columns" 
          :items="items"
        >
          <template #cell(name)="{ item }" >
             <div v-if="item.type === 'ns'" class="text-nowrap">
              <a href="#" @click.prevent="navigateTo(item)" 
               class="wl-accent font-weight-bold">
              <i class="fas fa-folder mr-2 text-warning opacity-75"></i>{{ item.name }}
              </a>
             </div>
             <div v-if="item.type === 'ds'" class="text-nowrap">
              <a href="#" @click.prevent="openDataset(item.namespace, item.id)" 
               class="wl-accent font-weight-bold">
              <i class="fas fa-table mr-2 text-warning opacity-75"></i>{{ item.id }}
              </a>
             </div>
          </template>
  
          <template #cell(description)="{ item }">
            {{ item.description || '—' }}
          </template>
          
          <template #cell(format)="{ item }">
            {{ item.format || '—' }}
          </template>
  
          <template #cell(committed_at)="{ item }">
            {{ item.committed_at || '—' }}
          </template>
  
        </base-table>
       </base-panel>
  
    <div class="row">
        <div v-if="detailOpen" class="col-md-12">
        <div class="card card-outline">
          <div class="card-header d-flex justify-content-between align-items-center">
            <h3 class="card-title">
              <i class="fas fa-table mr-2"></i>
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
                    <th style="width: 80px;">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-if="!history.length">
                    <td colspan="6" class="text-muted text-center py-3">No versions found</td>
                  </tr>
                  <tr v-for="v in history" :key="v.version">
                    <td style="font-family:monospace; font-size:0.75em;">
                      {{ v.version ? v.version : '—' }}
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
                    <td class="text-center">
                      <div class="btn-group">
                        <router-link 
                          :to="{ path: '/lineage', query: { ns: selNs, id: selId, ver: v.version } }"
                          class="btn btn-xs btn-outline-primary" 
                          title="View Lineage"
                          style="color: #d080ff; border-color: rgba(208, 128, 255, 0.4);">
                          <i class="fas fa-sitemap"></i>
                        </router-link>

                        <router-link 
                          :to="'/datasets/' + selNs + '/' + selId + '/' + v.version"
                          class="btn btn-xs btn-outline-info" 
                          title="View Preview"
                          style="color: #00d4ff; border-color: rgba(0, 212, 255, 0.4);">
                          <i class="fas fa-eye"></i>
                        </router-link>
                        
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>

          </div>
        </div>
      </div>

      <materialize ref="materializeRef" @done="loadNamespace(currentNs)"></materialize>
    </div>
  
      
    </base-page>
  `
});