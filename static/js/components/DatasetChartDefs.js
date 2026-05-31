import { api } from '../api.js';
import { nsStore } from '../store.js';
import BasePage        from './BasePage.js';
import BasePanel       from './BasePanel.js';
import BaseButton      from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseTable       from './BaseTable.js';
import BaseModal       from './BaseModal.js';
import BaseInput       from './BaseInput.js';
import ConfirmDialog   from './ConfirmDialog.js';

const { ref, computed, onMounted } = Vue;

const CHART_COLUMNS = [
  { key: 'title',   label: 'Title' },
  { key: 'type',    label: 'Type' },
  { key: 'x',      label: 'X' },
  { key: 'y',      label: 'Y / Agg' },
  { key: 'actions', label: '', class: 'text-right pr-3' },
];

export default {
  name: 'DatasetChartDefs',
  components: {
    BasePage, BasePanel, BaseButton, BaseButtonGroup,
    BaseTable, BaseModal, BaseInput, ConfirmDialog,
  },

  setup() {
    const route  = VueRouter.useRoute();
    const router = VueRouter.useRouter();

    const datasetId = computed(() => {
      const raw = route.params.id;
      return (Array.isArray(raw) ? raw.join('/') : String(raw || '')).replace(/^\/|\/$/g, '');
    });

    const charts        = ref([]);
    const loading       = ref(false);
    const chartSaving   = ref(false);
    const pageError     = ref(null);
    const chartError    = ref(null);
    const chartModalRef  = ref(null);
    const confirmChartDel = ref(null);
    const chartEditId   = ref(null);
    const chartForm     = ref({ title: '', spec_yaml: '' });

    function _defaultSpec() {
      return `type: bar\nx:\n  field: column_name\ny:\n  field: value_column\n  agg: sum\n`;
    }

    async function load() {
      if (!datasetId.value) return;
      loading.value   = true;
      pageError.value = null;
      try {
        const res = await api.datasetCharts(nsStore.selected, datasetId.value);
        charts.value = res.data || [];
      } catch (e) {
        pageError.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    function openAddChart() {
      chartEditId.value = null;
      chartError.value  = null;
      chartForm.value   = { title: '', spec_yaml: _defaultSpec() };
      chartModalRef.value?.open();
    }

    function openEditChart(chart) {
      chartEditId.value = chart.id;
      chartError.value  = null;
      const yamlText = window.jsyaml ? window.jsyaml.dump(chart.spec) : JSON.stringify(chart.spec, null, 2);
      chartForm.value = { title: chart.title, spec_yaml: yamlText };
      chartModalRef.value?.open();
    }

    async function submitChart() {
      chartError.value  = null;
      chartSaving.value = true;
      try {
        let spec;
        try {
          spec = window.jsyaml ? window.jsyaml.load(chartForm.value.spec_yaml)
                               : JSON.parse(chartForm.value.spec_yaml);
        } catch (e) {
          chartError.value = `Invalid YAML: ${e.message}`;
          return;
        }
        const body = { title: chartForm.value.title, spec };
        let res;
        if (chartEditId.value !== null) {
          res = await api.updateChart(nsStore.selected, datasetId.value, chartEditId.value, body);
        } else {
          res = await api.addChart(nsStore.selected, datasetId.value, body);
        }
        if (res.diagnostic?.result === 'KO') {
          chartError.value = res.diagnostic?.messages?.[0] || 'Error';
          return;
        }
        chartModalRef.value?.close();
        await load();
      } catch (e) {
        chartError.value = e.message;
      } finally {
        chartSaving.value = false;
      }
    }

    function askDeleteChart(chart) {
      confirmChartDel.value?.ask(
        `Delete chart "${chart.title}"?`,
        async (ok) => { if (ok) { await api.deleteChart(nsStore.selected, datasetId.value, chart.id); await load(); } }
      );
    }

    onMounted(load);

    return {
      datasetId, charts, loading, chartSaving, pageError, chartError,
      CHART_COLUMNS,
      chartModalRef, confirmChartDel, chartEditId, chartForm,
      openAddChart, openEditChart, submitChart, askDeleteChart,
      goBack: () => router.go(-1),
      load,
    };
  },

  template: `
    <base-page
      title="Chart Definitions"
      :subtitle="datasetId"
      icon="fas fa-chart-bar"
      :loading="loading && !charts.length"
      :error="pageError">

      <template #actions>
        <base-button icon="fas fa-arrow-left" label="Back"    color="outline-secondary" @click="goBack" />
        <base-button icon="fas fa-sync-alt"   label="Refresh" color="outline-primary"   class="ml-2" :loading="loading" @click="load" />
      </template>

      <base-panel :no-padding="true">
        <template #tools>
          <base-button icon="fas fa-plus" label="Add" color="outline-primary" @click="openAddChart" />
        </template>

        <base-table :columns="CHART_COLUMNS" :items="charts">

          <template #cell(type)="{ item }">
            <span class="badge badge-secondary">{{ item.spec.type || 'bar' }}</span>
          </template>

          <template #cell(x)="{ item }">
            <code class="small">{{ item.spec.x?.field || '—' }}</code>
          </template>

          <template #cell(y)="{ item }">
            <code class="small">{{ item.spec.y?.field || '—' }}</code>
            <span v-if="item.spec.y?.agg" class="text-muted small ml-1">({{ item.spec.y.agg }})</span>
          </template>

          <template #cell(actions)="{ item }">
            <base-button-group>
              <base-button icon="fas fa-pencil-alt" color="outline-primary" title="Edit"   @click="openEditChart(item)" />
              <base-button icon="fas fa-trash"      color="outline-danger"  title="Delete" @click="askDeleteChart(item)" />
            </base-button-group>
          </template>

        </base-table>

        <div v-if="!loading && !charts.length" class="p-3 text-muted small text-center">
          No charts defined — click Add to create one.
        </div>
      </base-panel>

      <!-- add/edit modal -->
      <base-modal ref="chartModalRef" size="lg" icon="fas fa-chart-bar"
                  :title="chartEditId !== null ? 'Edit Chart' : 'Add Chart'"
                  :scrollable="true">

        <div v-if="chartError" class="alert alert-danger mb-3">{{ chartError }}</div>

        <div class="form-group">
          <label class="small text-muted">Title</label>
          <base-input v-model="chartForm.title" placeholder="e.g. Revenue by Category" />
        </div>

        <div class="form-group">
          <label class="small text-muted">Spec <span class="text-secondary">(YAML)</span></label>
          <textarea class="form-control form-control-sm"
                    rows="14" v-model="chartForm.spec_yaml"
                    style="font-family: monospace; font-size: 0.8rem;"></textarea>
          <small class="text-muted">
            type: bar | line | pie | scatter | histogram &nbsp;·&nbsp;
            agg: sum | mean | count | max | min
          </small>
        </div>

        <template #footer>
          <base-button label="Save" icon="fas fa-save" color="primary"
                       :disabled="chartSaving || !chartForm.title" :loading="chartSaving"
                       @click="submitChart" />
          <base-button label="Close" icon="fas fa-times" color="outline-secondary"
                       class="ml-auto" @click="chartModalRef && chartModalRef.close()" />
        </template>
      </base-modal>

      <confirm-dialog ref="confirmChartDel" />
    </base-page>
  `,
};
