import { api }         from '../api.js';
import BasePage       from './BasePage.js';
import BasePanel      from './BasePanel.js';
import BaseButton     from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseModal      from './BaseModal.js';
import BaseInput      from './BaseInput.js';
import ChartWidget    from './ChartWidget.js';

const { ref, onMounted, nextTick } = Vue;

const LS_KEY = 'waluigi_dashboard_panels';

function loadPanels()  { try { return JSON.parse(localStorage.getItem(LS_KEY) || '[]'); } catch { return []; } }
function savePanels(p) { localStorage.setItem(LS_KEY, JSON.stringify(p)); }

export default {
  name: 'Dashboard',
  components: { BasePage, BasePanel, BaseButton, BaseButtonGroup, BaseModal, BaseInput, ChartWidget },

  setup() {
    const panels       = ref([]);  // [{ dataset_id, chart_id, title, option, loading, error }]
    const modalRef     = ref(null);
    const addDatasetId = ref('');
    const addCharts    = ref([]);
    const addChartId   = ref(null);
    const addLoading   = ref(false);
    const addError     = ref(null);

    // Always access panels through the reactive array by index so Vue tracks mutations.
    async function renderPanel(idx) {
      const p    = panels.value[idx];
      p.loading  = true;
      p.error    = null;
      p.option   = null;
      try {
        const res = await api.renderChart(p.dataset_id, p.chart_id);
        p.option  = res.data?.option ?? null;
      } catch (e) {
        p.error = e.message;
      } finally {
        p.loading = false;
      }
    }

    async function init() {
      const saved  = loadPanels();
      panels.value = saved.map(s => ({ ...s, option: null, loading: false, error: null }));
      await Promise.all(panels.value.map((_, idx) => renderPanel(idx)));
    }

    async function loadChartsForDataset() {
      addCharts.value  = [];
      addChartId.value = null;
      addError.value   = null;
      if (!addDatasetId.value.trim()) return;
      addLoading.value = true;
      try {
        const res = await api.datasetCharts(addDatasetId.value.trim());
        addCharts.value = res.data ?? [];
        if (!addCharts.value.length) addError.value = 'No charts defined for this dataset.';
      } catch (e) {
        addError.value = e.message;
      } finally {
        addLoading.value = false;
      }
    }

    async function addPanel() {
      const chart = addCharts.value.find(c => c.id === Number(addChartId.value));
      if (!chart) return;
      panels.value.push({
        dataset_id: addDatasetId.value.trim(),
        chart_id:   chart.id,
        title:      chart.title,
        option:     null,
        loading:    true,
        error:      null,
      });
      savePanels(panels.value.map(({ dataset_id, chart_id, title }) => ({ dataset_id, chart_id, title })));
      modalRef.value?.close();
      // Wait for Vue to render the new panel before rendering the chart into it.
      await nextTick();
      await renderPanel(panels.value.length - 1);
    }

    function removePanel(idx) {
      panels.value.splice(idx, 1);
      savePanels(panels.value.map(({ dataset_id, chart_id, title }) => ({ dataset_id, chart_id, title })));
    }

    function openAdd() {
      addDatasetId.value = '';
      addCharts.value    = [];
      addChartId.value   = null;
      addError.value     = null;
      modalRef.value?.open();
    }

    onMounted(init);

    return {
      panels, modalRef,
      addDatasetId, addCharts, addChartId, addLoading, addError,
      loadChartsForDataset, addPanel, removePanel, openAdd,
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
          </base-panel>
        </div>
      </div>

    </base-page>

    <base-modal ref="modalRef" title="Add Chart to Dashboard" size="sm">

      <div class="form-group">
        <label class="small font-weight-bold">Dataset ID</label>
        <base-input v-model="addDatasetId" placeholder="analytics/sales/monthly" />
      </div>

      <base-button label="Load charts" icon="fas fa-search" color="outline-secondary"
                   class="mb-3" :disabled="addLoading" @click="loadChartsForDataset" />

      <div v-if="addError" class="alert alert-warning small py-2">{{ addError }}</div>

      <div v-if="addCharts.length" class="form-group">
        <label class="small font-weight-bold">Chart</label>
        <select class="form-control form-control-sm" v-model="addChartId">
          <option :value="null" disabled>— select a chart —</option>
          <option v-for="c in addCharts" :key="c.id" :value="c.id">
            {{ c.title }} ({{ c.spec?.type || 'bar' }})
          </option>
        </select>
      </div>

      <template #footer>
        <base-button label="Add to dashboard" icon="fas fa-plus" color="primary"
                     :disabled="!addChartId" @click="addPanel" />
        <base-button label="Cancel" color="secondary" class="ml-2"
                     @click="modalRef?.close()" />
      </template>

    </base-modal>
  `,
};
