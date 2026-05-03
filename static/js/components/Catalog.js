import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseButton from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseTable from './BaseTable.js';
import Materialize from './Materialize.js';

const { computed, watch, onMounted } = Vue;

export default {
  name: 'Catalog',

  components: { BasePage, BasePanel, BaseButton, BaseButtonGroup, BaseTable, Materialize },

  setup() {
    const columns = [
      { key: 'name', label: 'Name' },
      { key: 'format', label: 'Format' },
      { key: 'description', label: 'Description' },
      { key: 'source_id', label: 'Source' },
      { key: 'type', label: 'Type' },
      { key: 'status', label: 'Status' }
    ];
    
    const columns_history = [
      { key: 'version', label: 'Version' },
      { key: 'hash', label: 'Hash' },
      { key: 'status', label: 'Status' },
      { key: 'actions', label: 'Actions' }
    ];
    
    const route  = VueRouter.useRoute();
    const router = VueRouter.useRouter();

    const folderStack    = Vue.ref([]);   // breadcrumb: [{path, name}]
    const children   = Vue.ref([]);   
    const datasets   = Vue.ref([]);  
    const items   = Vue.ref([]);
    const loading    = Vue.ref(false);
    const materializeRef = Vue.ref(null);

    // detail panel
    const selFolder      = Vue.ref(null);
    const selDataset      = Vue.ref(null); // selected dataset id
    const history    = Vue.ref([]);
    const metadata         = Vue.ref({});
    const selectedVersion  = Vue.ref(null);
    const detailOpen = Vue.ref(false);

    const currentFolder = computed(() =>
      folderStack.value.length ? folderStack.value[folderStack.value.length - 1].path : null
    );
    
    function getParent(path) {
      if (!path || path === "/") return null;

      const clean = path.replace(/\/+$/, '');
      const parts = clean.split('/');

      parts.pop();

      return parts.length ? parts.join('/') : null;
    }

    async function loadFolders(path) {
      loading.value = true;
      
      try {
        if (!path) {
          path = "/";
        }
        
        const res = await api.catalogFolders(path);
        const prefix = res.data.prefix;
        //alert(prefix)
        const prefixItems = [];
        const cleanPrefix = prefix ? prefix.replace(/\/+$/, '') : null;

        prefixItems.push({ 
          name: ".", 
          path: cleanPrefix, 
          description: "Current folder", 
          type: 'folder' 
        });

        prefixItems.push({ 
          name: "..", 
          path: getParent(cleanPrefix),
          description: "Parent folder", 
          type: 'folder' 
        });
        const realPrefixItem = (res.data.prefixes || []).map(folder => {
          const clean = folder.replace(/\/+$/, '');

          return {
            name: clean.split('/').pop(),
            path: clean,
            description: "",
            type: 'folder'
          };
        });
        prefixItems.push(...realPrefixItem);
        const datasetItems = (res.data.datasets || []).map(d => ({
          ...d,
          name: d.id.replace(/\/$/, '').split('/').pop(),
          path: prefix ? prefix.replace(/\/+$/, '') : null,
          type: 'dataset'
        }));
        items.value = [...prefixItems, ...datasetItems];
        
      } catch(e) {
        console.error('Catalog load error', e);
        items.value = [];
      } finally {
        loading.value = false;
      }
    }

    async function loadDataset(folder, dataset) {
      selFolder.value       = folder;
      selDataset.value      = dataset;
      detailOpen.value      = true;
      history.value         = [];
      selectedVersion.value = null;
      metadata.value        = {};

      try {
        const res = await api.catalogDatasetVersions(dataset);
        history.value = Array.isArray(res.data) ? res.data : [];
      } catch(e) {
        console.error('Dataset detail error', e);
      }
    }

    async function selectVersion(ver) {
      selectedVersion.value = ver.version;
      metadata.value = {};
      try {
        const res = await api.catalogDatasetMetadata(selDataset.value, ver.version);
        metadata.value = res.data || {};
      } catch(e) {
        alert(e)
        console.error('Metadata load error', e);
      }
    }

    async function openDataset(folder, dataset) {
      router.push({ path: '/catalog', query: { folder, dataset } });
    }

    function navigateTo(path) {
      router.push({ path: '/catalog', query: { folder: path || undefined } });
    }

    function navigateBreadcrumb(idx) {
      if (idx < 0) {
        folderStack.value = [];
        router.push({ path: '/catalog' });
        loadFolders(null);
      } else {
        folderStack.value = folderStack.value.slice(0, idx + 1);
        const path = folderStack.value[idx].path;
        router.push({ path: '/catalog', query: { folder: path } });
        loadFolders(path);
      }
    }

    function goBack() {
      router.go(-1);
    }

    function closeDetail() {
      detailOpen.value = false;
      selFolder.value = null;
      selDataset.value = null;
      router.push({ path: '/catalog', query: { folder: currentFolder.value || undefined } });
    }

    onMounted(async () => {
      const folder = route.query.folder;
      const dataset = route.query.dataset;

      if (folder) {
        const parts = folder.split('/');
        folderStack.value = parts.map((name, i) => ({
          path: parts.slice(0, i + 1).join('/'),
          name
        }));
        await loadFolders(folder);
      } else {
        await loadFolders(null);
      }

      if (dataset && folder) {
        await openDataset(folder, dataset);
      }
    });

    watch(() => route.query, async (q) => {
      let folder = q.folder || null;
      const dataset = q.dataset || null;

      // ✅ normalizzazione
      folder = folder ? folder.replace(/\/+$/, '') : null;

      if (folder) {
        const parts = folder.split('/');
        folderStack.value = parts.map((name, i) => ({
          path: parts.slice(0, i + 1).join('/'),
          name
        }));
        await loadFolders(folder);
      } else {
        folderStack.value = [];
        await loadFolders(null);
      }

      if (dataset && folder) {
        await loadDataset(folder, dataset);
      } else {
        detailOpen.value = false;
      }
    }, { immediate: true });

    loadFolders(null);

    const dqDetails = Vue.computed(() => {
      const raw = metadata.value?.['sys.dq.details'];
      if (!raw) return [];
      try { return JSON.parse(raw); } catch { return []; }
    });

    return {
      columns, items, folderStack, children, datasets, loading,
      selFolder, selDataset, columns_history, history, metadata, detailOpen,
      selectedVersion, currentFolder,
      navigateTo, navigateBreadcrumb, openDataset, closeDetail, goBack,
      selectVersion, materializeRef, dqDetails,
    };
  },

  template: `
    <base-page 
      title="Catalog" 
      subtitle="Browse datasets"
      icon="fas fa-table">
  
      <template #actions>
         <base-button 
            label="Back" 
            icon="fas fa-arrow-left" 
            color="outline-secondary"
            @click="goBack"
          />
          <base-button 
            label="Materialize" 
            icon="fas fa-cloud-download-alt" 
            color="outline-primary" 
            class="ml-auto"
            @click="materializeRef && materializeRef.open(currentFolder || '')"
          />
      </template>
    
      <base-panel
        :no-padding="true">
  
        <template #title>
          <ol class="breadcrumb bg-transparent p-0">
            <li class="breadcrumb-item">
              <a href="#" @click.prevent="navigateBreadcrumb(-1)">🏠</a>
            </li>
            <li v-for="(crumb, idx) in folderStack" :key="crumb.path"
                :class="['breadcrumb-item', idx===folderStack.length-1 ? 'active' : '']">
              <a v-if="idx < folderStack.length-1" href="#" @click.prevent="navigateBreadcrumb(idx)" >{{ crumb.name }}</a>
              <span v-else >{{ crumb.name }}</span>
            </li>
          </ol>
        </template>
  
        <base-table 
          :columns="columns" 
          :items="items">
  
          <template #cell(name)="{ item }" >
             <div v-if="item.type === 'folder'" class="text-nowrap">
              <a href="#" @click.prevent="navigateTo(item.path)">
               <i class="fas fa-folder mr-2 text-warning opacity-75"></i>{{ item.name }}
              </a>
             </div>
             <div v-if="item.type === 'dataset'" class="text-nowrap">
              <a href="#" @click.prevent="openDataset(item.path, item.id)">
               <i class="fas fa-table mr-2 opacity-75"></i>{{ item.name }}
              </a>
             </div>
          </template>
  
          <template #cell(type)="{ item }" >
            <div v-if="item.type === 'folder'">
              <span class="badge badge-warning">
               {{ item.type }}
             </span>
            </div>
            <div v-if="item.type === 'dataset'">
              <span class="badge badge-primary">
               {{ item.type }}
             </span>
            </div>
          </template>
  
          <template #cell(format)="{ item }" >
             <span class="badge badge-info">
               {{ item.format }}
             </span>
          </template>
  
          <template #cell(status)="{ item }" >
             <span class="badge badge-success">
               {{ item.status }}
             </span>
          </template>
  
        </base-table>
       </base-panel>
  
     <base-panel
        :no-padding="true" 
        v-if="detailOpen">
  
        <template #title>
          <div class="d-flex align-items-center">
            <i class="fas fa-table mr-2"></i>
            <code class="text-info">{{ selDataset }}</code>
          </div>
        </template>
  
        <template #tools>
          <base-button
            icon="fas fa-project-diagram"
            label="Schema"
            color="outline-warning"
            title="Manage Schema"
            @click="$router.push('/schema/' + selDataset)"
          />
          <base-button
            icon="fas fa-times"
            color="outline-secondary"
            class="ml-auto"
            @click="closeDetail"/>
        </template>
        
        <base-table 
          :columns="columns_history" 
          :items="history">
  
           <template #cell(version)="{ item }">
             <a href="#" @click.prevent="selectVersion(item)"
                :class="selectedVersion === item.version ? 'font-weight-bold text-primary' : ''">
               {{ item.version.slice(0, 19) }}
             </a>
           </template>

           <template #cell(hash)="{ item }" >
             {{ item.hash ? item.hash.slice(0,8) : '—' }}
           </template>
        
           <template #cell(status)="{ item }" >
             <span class="badge badge-success">
               {{ item.status }}
             </span>
           </template>
  
           <template #cell(actions)="{ item }">
             <base-button-group>
              <base-button 
                icon="fas fa-sitemap" 
                color="outline-primary" 
                title="Lineage"
                @click="$router.push({ path: '/lineage', query: { folder: selFolder, id: selDataset, ver: item.version } })"
              />
              <base-button 
                icon="fas fa-eye" 
                color="outline-info" 
                title="Preview"
                @click="$router.push({ path: '/datasets/' + selDataset + '/' + item.version })"
              />
             </base-button-group>
           </template>
  
        </base-table>
  
        <div v-if="selectedVersion" class="p-3 border-top">
          <h6 class="text-muted mb-2">
            Metadata
            <small class="ml-2 font-weight-normal text-secondary">{{ selectedVersion.slice(0,19) }}</small>
          </h6>

          <!-- DQ summary + per-rule detail -->
          <div v-if="metadata['sys.dq.score'] !== undefined" class="mb-3">
            <div class="p-2 rounded mb-2"
                 :style="{ background: metadata['sys.dq.success'] === 'True' ? '#d4edda' : '#f8d7da' }">
              <div class="d-flex align-items-center">
                <i :class="['fas mr-2', metadata['sys.dq.success'] === 'True' ? 'fa-check-circle text-success' : 'fa-times-circle text-danger']"></i>
                <strong class="small">Data Quality</strong>
                <span class="ml-auto badge"
                      :class="metadata['sys.dq.success'] === 'True' ? 'badge-success' : 'badge-danger'">
                  {{ (parseFloat(metadata['sys.dq.score']) * 100).toFixed(1) }}%
                </span>
              </div>
              <div class="small text-muted mt-1">
                {{ metadata['sys.dq.passed'] }} / {{ metadata['sys.dq.total'] }} rules passed
              </div>
            </div>

            <!-- per-rule breakdown -->
            <table v-if="dqDetails.length" class="table table-sm table-bordered mb-0 small">
              <thead class="thead-light">
                <tr>
                  <th>Rule</th>
                  <th style="width:60px" class="text-center">Status</th>
                  <th style="width:60px" class="text-center">Score</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="r in dqDetails" :key="r.rule_id">
                  <td>
                    <code>{{ r.rule_id }}</code>
                    <div v-if="r.error" class="text-danger small mt-1">{{ r.error }}</div>
                  </td>
                  <td class="text-center">
                    <i v-if="r.success" class="fas fa-check-circle text-success"></i>
                    <i v-else class="fas fa-times-circle text-danger"></i>
                  </td>
                  <td class="text-center">
                    {{ r.score !== null ? (r.score * 100).toFixed(1) + '%' : '—' }}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <div v-if="metadata['sys.dq.error']" class="mb-3 alert alert-warning py-1 px-2 small">
            <i class="fas fa-exclamation-triangle mr-1"></i>DQ error: {{ metadata['sys.dq.error'] }}
          </div>

          <div v-if="!Object.keys(metadata).filter(k => !k.startsWith('sys.dq.')).length && !metadata['sys.dq.score']"
               class="text-muted small">
            No metadata for this version.
          </div>
          <div v-for="(val, key) in metadata" :key="key" class="small mb-1"
               v-if="!key.startsWith('sys.dq.')">
            <span class="text-muted">{{ key }}:</span>
            <span class="ml-2">{{ val }}</span>
          </div>
        </div>
  
     </base-panel>
     
     <materialize ref="materializeRef" @done="loadFolders(currentFolder)"></materialize>
    
    </base-page>
  `
};