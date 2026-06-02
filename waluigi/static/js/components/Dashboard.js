import { api }         from '../api.js';
import { nsStore }    from '../store.js';
import BasePage       from './BasePage.js';
import BasePanel      from './BasePanel.js';
import BaseButton     from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseModal      from './BaseModal.js';
import BaseInput      from './BaseInput.js';
import ChartWidget    from './ChartWidget.js';

const { ref, nextTick, watch } = Vue;

const LS_KEY = 'waluigi_dashboard_panels_v2';

function loadPanels()  { try { return JSON.parse(localStorage.getItem(LS_KEY) || '[]'); } catch { return []; } }
function savePanels(p) { localStorage.setItem(LS_KEY, JSON.stringify(p)); }

function fmtVersion(ver) {
  if (!ver) return '';
  return ver.slice(0, 19).replace('T', ' ');
}

export default {
  name: 'Dashboard',
  components: { BasePage, BasePanel, BaseButton, BaseButtonGroup, BaseModal, BaseInput, ChartWidget },

  setup() {
    // panel shape: { dataset_id, chart_key, title, version (null=latest), option, renderedVersion, isLatest, loading, error }
    const panels       = ref([]);
    const modalRef     = ref(null);
    const addDatasetId = ref('');
    const addCharts    = ref([]);
    const addChartKey  = ref(null);
    const addVersion   = ref('');
    const addVersions  = ref([]);
    const addLoading   = ref(false);
    const addError     = ref(null);

    async function renderPanel(idx) {
      if (!nsStore.selected) return;
      const p   = panels.value[idx];
      p.loading = true;
      p.error   = null;
      p.option  = null;
      try {
        const res = await api.renderChartByKey(nsStore.selected, p.dataset_id, p.chart_key, p.version || null);
        p.option          = res.data?.option          ?? null;
        p.renderedVersion = res.data?.version         ?? null;
        p.isLatest        = res.data?.is_latest       ?? true;
      } catch (e) {
        p.error = e.message;
      } finally {
        p.loading = false;
      }
    }

    async function init() {
      if (!nsStore.selected) { panels.value = []; return; }
      const saved  = loadPanels();
      panels.value = saved.map(s => ({ ...s, option: null, renderedVersion: null, isLatest: true, loading: false, error: null }));
      await Promise.all(panels.value.map((_, idx) => renderPanel(idx)));
    }

    async function loadChartsForDataset() {
      addCharts.value   = [];
      addChartKey.value = null;
      addVersions.value = [];
      addVersion.value  = '';
      addError.value    = null;
      if (!addDatasetId.value.trim()) return;
      addLoading.value = true;
      try {
        const ns = nsStore.selected;
        const [chartsRes, versionsRes] = await Promise.all([
          api.datasetCharts(ns, addDatasetId.value.trim()),
          api.catalogDatasetVersions(ns, addDatasetId.value.trim()),
        ]);
        addCharts.value   = chartsRes.data  ?? [];
        addVersions.value = versionsRes.data ?? [];
        if (!addCharts.value.length) addError.value = 'No charts defined for this dataset.';
      } catch (e) {
        addError.value = e.message;
      } finally {
        addLoading.value = false;
      }
    }

    async function addPanel() {
      const chart = addCharts.value.find(c => c.key === addChartKey.value);
      if (!chart) return;
      const pinned = addVersion.value.trim() || null;
      panels.value.push({
        dataset_id:      addDatasetId.value.trim(),
        chart_key:       chart.key,
        title:           chart.title,
        version:         pinned,
        option:          null,
        renderedVersion: null,
        isLatest:        true,
        loading:         true,
        error:           null,
      });
      _save();
      modalRef.value?.close();
      await nextTick();
      await renderPanel(panels.value.length - 1);
    }

    function removePanel(idx) {
      panels.value.splice(idx, 1);
      _save();
    }

    function _save() {
      savePanels(panels.value.map(({ dataset_id, chart_key, title, version }) =>
        ({ dataset_id, chart_key, title, version })));
    }

    function openAdd() {
      addDatasetId.value = '';
      addCharts.value    = [];
      addChartKey.value  = null;
      addVersions.value  = [];
      addVersion.value   = '';
      addError.value     = null;
      modalRef.value?.open();
    }

    watch(() => nsStore.selected, init, { immediate: true });

    return {
      panels, modalRef,
      addDatasetId, addCharts, addChartKey, addVersion, addVersions, addLoading, addError,
      loadChartsForDataset, addPanel, removePanel, openAdd, fmtVersion,
    };
  },

  template: `
    <base-page title="Dashboard" icon="fas fa-th-large">

      <template #actions>
        <base-button icon="fas fa-plus" label="Add Chart"
                     color="outline-primary" class="ml-auto" @click="openAdd" />
      </template>

      <div v-if="!panels.length" class="alert alert-info">
        No charts on this dashboard yet — click <strong>Add Chart</strong> to start.
      </div>

      <div class="row">
        <div v-for="(panel, idx) in panels" :key="idx" class="col-12 col-lg-6 mb-4">
          <base-panel :title="panel.title" icon="fa-chart-bar" :no-padding="true">
            <template #tools>
              <base-button-group>
                <base-button icon="fas fa-times" color="outline-danger"
                             title="Remove from dashboard" @click="removePanel(idx)" />
              </base-button-group>
            </template>
            <div style="height:300px;">
              <chart-widget :option="panel.option" :loading="panel.loading"
                            :error="panel.error" height="300px" />
            </div>
            <div v-if="panel.renderedVersion" class="px-3 pb-2 pt-1 d-flex align-items-center small text-muted border-top">
              <i class="fas fa-clock mr-1"></i>
              <span>{{ fmtVersion(panel.renderedVersion) }}</span>
              <span v-if="panel.version" class="badge badge-warning ml-2">pinned</span>
              <span v-else class="badge badge-success ml-2">latest</span>
            </div>
          </base-panel>
        </div>
      </div>

    </base-page>

    <base-modal ref="modalRef" title="Add Chart to Dashboard" size="sm">

      <div class="form-group">
        <label class="small font-weight-bold">Dataset ID</label>
        <base-input v-model="addDatasetId" placeholder="analytics/reports/global_report" />
      </div>

      <base-button label="Load charts" icon="fas fa-search" color="outline-secondary"
                   class="mb-3" :disabled="addLoading" @click="loadChartsForDataset" />

      <div v-if="addError" class="alert alert-warning small py-2">{{ addError }}</div>

      <div v-if="addCharts.length">
        <div class="form-group">
          <label class="small font-weight-bold">Chart</label>
          <select class="form-control form-control-sm" v-model="addChartKey">
            <option :value="null" disabled>— select a chart —</option>
            <option v-for="c in addCharts" :key="c.key" :value="c.key">
              {{ c.title }} ({{ c.spec?.type || 'bar' }})
            </option>
          </select>
        </div>

        <div class="form-group">
          <label class="small font-weight-bold">Version <span class="text-muted font-weight-normal">(leave empty for latest)</span></label>
          <select class="form-control form-control-sm" v-model="addVersion">
            <option value="">Latest (auto-update)</option>
            <option v-for="v in addVersions" :key="v.version" :value="v.version">
              {{ fmtVersion(v.version) }}
            </option>
          </select>
        </div>
      </div>

      <template #footer>
        <base-button label="Add to dashboard" icon="fas fa-plus" color="primary"
                     :disabled="!addChartKey" @click="addPanel" />
        <base-button label="Cancel" color="secondary" class="ml-2"
                     @click="modalRef?.close()" />
      </template>

    </base-modal>
  `,
};
