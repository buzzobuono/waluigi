import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseButton from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseTable from './BaseTable.js';

export default {
  name: 'DatasetPreview',
  components: { BasePage, BasePanel, BaseButton, BaseButtonGroup, BaseTable },

  setup() {
    const route = VueRouter.useRoute();
    const router = VueRouter.useRouter();

    const columns       = Vue.ref([]);
    const rows          = Vue.ref([]);
    const loading       = Vue.ref(false);
    const error         = Vue.ref(null);
    const metadata      = Vue.ref({});
    const schema        = Vue.ref([]);
    const schemaColumns = [
      { key: 'column_name', label: 'Column' },
      { key: 'physical_type', label: 'Type' },
      { key: 'description', label: 'Description' },
      { key: 'pii', label: 'PII' },
      { key: 'status', label: 'Status' },
    ];
    const currentPage = Vue.ref(1);
    const pageSize    = Vue.ref(10);

    const params = Vue.computed(() => {
      const formatParam = (val) => {
        if (!val) return '';
        const joined = Array.isArray(val) ? val.join('/') : String(val);
        return joined.replace(/^\/|\/$/g, '');
      };

      return {
        id:        formatParam(route.params.id),
        version:   route.params.version
      };
    });

    async function loadPreview() {
      const { id, version } = params.value;
      if (!id || !version) return;

      loading.value = true;
      error.value = null;
      try {
        const limit = pageSize.value;
        const offset = (currentPage.value - 1) * limit;
        const isFirstPage = currentPage.value === 1;
        const requests = [
          api.catalogDatasetPreview(id, version, limit, offset),
          api.catalogDatasetMetadata(id, version),
        ];
        if (isFirstPage) requests.push(api.catalogDatasetSchema(id));

        const [previewRes, metaRes, schemaRes] = await Promise.all(requests);

        columns.value = (previewRes.data.columns || []).map(col => ({
          key: col,
          label: col
        }));
        rows.value     = previewRes.data.rows || [];
        metadata.value = metaRes.data || {};
        if (schemaRes) schema.value = schemaRes.data?.columns || [];
      } catch (e) {
        console.error("Preview load error:", e);
        error.value = "Loading error: " + e.message;
      } finally {
        loading.value = false;
      }
    }

    function goBack() {
      router.go(-1);
    }

    function changePage(delta) {
      const next = currentPage.value + delta;
      if (next > 0) {
        currentPage.value = next;
        loadPreview();
      }
    }

    Vue.watch(() => params.value, () => {
      currentPage.value = 1;
      loadPreview();
    }, { deep: true });

    Vue.onMounted(loadPreview);

    return {
      columns, rows, loading, error, params, metadata, schema, schemaColumns,
      currentPage, changePage, goBack
    };
  },

  template: `
    <base-page 
      title="Dataset" 
      subtitle="Preview"
      icon="fas fa-table"
      :loading="loading">
      
      <template #actions>
         <base-button 
            label="Back" 
            icon="fas fa-arrow-left" 
            color="outline-secondary"
            @click="goBack"
          />
      </template>

      <base-panel :no-padding="true">

        <template #title>
          <i class="fas fa-table mr-2 text-warning"></i>
          <span class="font-weight-bold">Data Preview: </span>
          <code class="ml-1 text-warning">{{ params.id }}</code>
        </template>

        <template #tools>
          <base-button-group class="ml-auto">
            <base-button 
              :disabled="loading || currentPage <= 1"
              icon="fas fa-chevron-left"
              color="outline-primary" 
              @click="changePage(-1)"
            />
            <base-button 
              :label="currentPage"
              :disabled="true"
              color="outline-secondary" 
            />
            <base-button 
              :disabled="loading || rows.length < 10"
              icon="fas fa-chevron-right"
              color="outline-primary" 
              @click="changePage(1)"
            />
          </base-button-group>
        </template>

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
        
        <base-table :columns="columns" :items="rows">

        </base-table>

        <template #footer>
          <div class="text-muted" >
            Rows {{ (currentPage-1)*10 + 1 }} - {{ (currentPage-1)*10 + rows.length }}
          </div>
          <div class="text-muted">
            <span>Path: {{ params.id }}</span>
            <span>
                v.{{ params.version }}
            </span>
          </div>
        </template>
        
      </base-panel>

      <base-panel v-if="Object.keys(metadata).length" title="Metadata">
        <div class="p-3">
          <div v-for="(val, key) in metadata" :key="key" class="small mb-1">
            <span class="text-muted">{{ key }}:</span>
            <span class="ml-2">{{ val }}</span>
          </div>
        </div>
      </base-panel>

      <base-panel v-if="schema.length" title="Schema" :no-padding="true">
        <base-table :columns="schemaColumns" :items="schema">
          <template #cell(pii)="{ item }">
            <span v-if="item.pii" class="badge badge-danger">PII</span>
            <span v-else class="text-muted">—</span>
          </template>
          <template #cell(status)="{ item }">
            <span :class="['badge', item.status === 'published' ? 'badge-success' : 'badge-secondary']">
              {{ item.status }}
            </span>
          </template>
        </base-table>
      </base-panel>

    </base-page>
  `
};