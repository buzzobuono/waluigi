import { api }       from '../api.js';
import { nsStore }  from '../store.js';
import BasePage     from './BasePage.js';
import BasePanel    from './BasePanel.js';
import BaseButton   from './BaseButton.js';
import BaseTable    from './BaseTable.js';
import ChartWidget  from './ChartWidget.js';

const { ref, computed, onMounted, watch } = Vue;

function trendOption(results) {
  const sorted = [...results].sort((a, b) => a.version.localeCompare(b.version));
  return {
    tooltip: { trigger: 'axis', formatter: (p) => `${p[0].name.slice(0,19)}<br/>Score: ${p[0].value}%` },
    grid:    { left: '8%', right: '4%', top: '10%', bottom: '15%' },
    xAxis: {
      type: 'category',
      data: sorted.map(r => r.version.slice(0, 16)),
      axisLabel: { rotate: 30, fontSize: 10 },
    },
    yAxis:  { type: 'value', min: 0, max: 100, axisLabel: { formatter: '{value}%' } },
    series: [{
      type: 'line',
      data: sorted.map(r => +(r.score * 100).toFixed(1)),
      smooth: true,
      symbol: 'circle',
      symbolSize: 6,
      lineStyle: { width: 2 },
      itemStyle: { color: '#007bff' },
      areaStyle: { opacity: 0.1, color: '#007bff' },
      markLine: {
        silent: true,
        data: [{ type: 'average', label: { formatter: 'avg {c}%' } }],
      },
    }],
  };
}

const HISTORY_COLUMNS = [
  { key: 'version', label: 'Version' },
  { key: 'score',   label: 'Score',   class: 'text-center', style: 'width:90px' },
  { key: 'passed',  label: 'Rules',   class: 'text-center', style: 'width:90px' },
  { key: 'status',  label: 'Status',  class: 'text-center', style: 'width:90px' },
  { key: 'actions', label: '',        class: 'text-right pr-3' },
];

export default {
  name: 'DatasetDQHistory',
  components: { BasePage, BasePanel, BaseButton, BaseTable, ChartWidget },

  setup() {
    const route  = VueRouter.useRoute();
    const router = VueRouter.useRouter();

    const datasetId = computed(() => {
      const raw = route.params.id;
      return (Array.isArray(raw) ? raw.join('/') : String(raw || '')).replace(/^\/|\/$/g, '');
    });

    const history  = ref([]);
    const loading  = ref(false);
    const error    = ref(null);

    const trendOpt = computed(() => history.value.length > 1 ? trendOption(history.value) : null);

    async function load() {
      if (!datasetId.value || !nsStore.selected) return;
      loading.value = true;
      error.value   = null;
      try {
        const res    = await api.datasetDQResults(nsStore.selected, datasetId.value);
        history.value = res.data || [];
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    onMounted(load);
    watch(() => nsStore.selected, (ns) => { if (ns) load(); });

    return {
      datasetId, history, loading, error, trendOpt,
      HISTORY_COLUMNS,
      goBack: () => router.go(-1),
      load,
      goToVersion: (ver) => router.push(`/dq/${datasetId.value}/${ver}`),
    };
  },

  template: `
    <base-page
      title="DQ History"
      :subtitle="datasetId"
      icon="fas fa-chart-line"
      :loading="loading && !history.length"
      :error="error">

      <template #actions>
        <base-button icon="fas fa-arrow-left" label="Back"    color="outline-secondary" @click="goBack" />
        <base-button icon="fas fa-sync-alt"   label="Refresh" color="outline-primary"   class="ml-2" :loading="loading" @click="load" />
      </template>

      <div v-if="!loading && !error && !history.length" class="text-center text-muted p-5">
        <i class="fas fa-chart-line fa-3x mb-3"></i>
        <p>No DQ runs recorded yet for this dataset.</p>
      </div>

      <template v-if="history.length">

        <base-panel v-if="trendOpt" title="Score Trend" icon="fa-chart-line" :no-padding="true" class="mb-4">
          <div style="height:220px;">
            <chart-widget :option="trendOpt" height="220px" />
          </div>
        </base-panel>

        <base-panel title="Run History" icon="fa-list" :no-padding="true">
          <base-table :columns="HISTORY_COLUMNS" :items="history">

            <template #cell(version)="{ item }">
              <code class="small">{{ item.version.slice(0, 19) }}</code>
            </template>

            <template #cell(score)="{ item }">
              <span :class="item.success ? 'text-success font-weight-bold' : 'text-danger font-weight-bold'">
                {{ (item.score * 100).toFixed(1) }}%
              </span>
            </template>

            <template #cell(passed)="{ item }">
              <span class="text-muted">{{ item.passed }} / {{ item.total }}</span>
            </template>

            <template #cell(status)="{ item }">
              <span :class="['badge', item.success ? 'badge-success' : 'badge-danger']">
                {{ item.success ? 'Passed' : 'Failed' }}
              </span>
            </template>

            <template #cell(actions)="{ item }">
              <base-button icon="fas fa-eye" color="outline-primary" title="View detail"
                           @click="goToVersion(item.version)" />
            </template>

          </base-table>
        </base-panel>

      </template>

    </base-page>
  `,
};
