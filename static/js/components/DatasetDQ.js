import { api }       from '../api.js';
import BasePage     from './BasePage.js';
import BasePanel    from './BasePanel.js';
import BaseButton   from './BaseButton.js';
import BaseInfoBox  from './BaseInfoBox.js';
import BaseTable    from './BaseTable.js';
import ChartWidget  from './ChartWidget.js';

const { ref, computed, onMounted } = Vue;

function gaugeOption(score, success) {
  const pct   = +(score * 100).toFixed(1);
  const color = success ? '#28a745' : '#dc3545';
  return {
    series: [{
      type: 'gauge',
      min: 0, max: 100,
      radius: '90%',
      progress: { show: true, width: 14 },
      axisLine: { lineStyle: { width: 14, color: [[score, color], [1, '#e9ecef']] } },
      axisTick: { show: false },
      splitLine: { length: 10, lineStyle: { width: 2 } },
      axisLabel: { distance: 18, fontSize: 10 },
      pointer: { itemStyle: { color } },
      detail: {
        valueAnimation: true,
        formatter: '{value}%',
        fontSize: 22,
        fontWeight: 'bold',
        color,
        offsetCenter: [0, '70%'],
      },
      data: [{ value: pct, name: 'DQ Score' }],
      title: { fontSize: 12, color: '#6c757d', offsetCenter: [0, '90%'] },
    }],
  };
}

function barOption(details) {
  const rules  = details.map(d => d.rule_id);
  const data   = details.map(d => ({
    value:     d.score !== null ? +(d.score * 100).toFixed(1) : 0,
    itemStyle: { color: d.success ? '#28a745' : '#dc3545' },
  }));
  const tolLine = details.map(d => +(( d.tolerance ?? 1) * 100).toFixed(1));
  return {
    tooltip: { trigger: 'axis' },
    grid:    { left: '35%', right: '8%', top: '5%', bottom: '8%' },
    xAxis:   { type: 'value', max: 100, axisLabel: { formatter: '{value}%' } },
    yAxis:   { type: 'category', data: rules, axisLabel: { fontSize: 11 } },
    series: [
      {
        name: 'Score',
        type: 'bar',
        data,
        label: { show: true, position: 'right', formatter: '{c}%', fontSize: 11 },
        barMaxWidth: 28,
      },
      {
        name: 'Tolerance',
        type: 'scatter',
        symbol: 'line',
        symbolSize: [2, 20],
        data: tolLine.map((t, i) => [t, i]),
        itemStyle: { color: '#6c757d' },
        tooltip: { formatter: (p) => `Tolerance: ${p.data[0]}%` },
      },
    ],
  };
}

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

const DQ_COLUMNS = [
  { key: 'rule_id',   label: 'Rule' },
  { key: 'status',    label: 'Status',    class: 'text-center', style: 'width:80px' },
  { key: 'score',     label: 'Score',     class: 'text-center', style: 'width:80px' },
  { key: 'tolerance', label: 'Tolerance', class: 'text-center', style: 'width:80px' },
];

export default {
  name: 'DatasetDQ',
  components: { BasePage, BasePanel, BaseButton, BaseInfoBox, BaseTable, ChartWidget },

  setup() {
    const route  = VueRouter.useRoute();
    const router = VueRouter.useRouter();

    const datasetId = computed(() => {
      const raw = route.params.id;
      return (Array.isArray(raw) ? raw.join('/') : String(raw || '')).replace(/^\/|\/$/g, '');
    });
    const version = computed(() => route.params.version);

    const result   = ref(null);
    const history  = ref([]);
    const loading  = ref(false);
    const error    = ref(null);

    const gaugeOpt = computed(() => result.value ? gaugeOption(result.value.score, result.value.success) : null);
    const barOpt   = computed(() => result.value?.details?.length ? barOption(result.value.details) : null);
    const trendOpt = computed(() => history.value.length > 1 ? trendOption(history.value) : null);

    async function load() {
      loading.value = true;
      error.value   = null;
      try {
        const [resR, histR] = await Promise.allSettled([
          api.datasetDQResult(datasetId.value, version.value),
          api.datasetDQResults(datasetId.value),
        ]);
        result.value  = resR.status  === 'fulfilled' ? (resR.value?.data  ?? null) : null;
        history.value = histR.status === 'fulfilled' ? (histR.value?.data ?? [])   : [];
        if (!result.value) error.value = 'No DQ result found for this version.';
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    onMounted(load);

    return {
      datasetId, version, result, history,
      loading, error, gaugeOpt, barOpt, trendOpt,
      DQ_COLUMNS,
      goBack: () => router.go(-1),
    };
  },

  template: `
    <base-page title="Data Quality" :subtitle="datasetId" icon="fas fa-shield-alt" :loading="loading">

      <template #actions>
        <base-button label="Back" icon="fas fa-arrow-left"
                     color="outline-secondary" @click="goBack" />
        <base-button label="Schema" icon="fas fa-project-diagram"
                     color="outline-warning" class="ml-2"
                     @click="$router.push('/schema/' + datasetId)" />
      </template>

      <div v-if="error" class="alert alert-warning">
        <i class="fas fa-exclamation-triangle mr-1"></i>{{ error }}
      </div>

      <template v-if="result">

        <!-- Summary KPIs -->
        <div class="row mb-3">
          <div class="col-6 col-md-3">
            <base-info-box label="Score"
                           :value="(result.score * 100).toFixed(1) + '%'"
                           icon="fas fa-percent"
                           :color="result.success ? 'success' : 'danger'" />
          </div>
          <div class="col-6 col-md-3">
            <base-info-box label="Rules passed"
                           :value="result.passed + ' / ' + result.total"
                           icon="fas fa-check-circle"
                           :color="result.success ? 'success' : 'warning'" />
          </div>
          <div class="col-6 col-md-3">
            <base-info-box label="Status"
                           :value="result.success ? 'Passed' : 'Failed'"
                           icon="fas fa-shield-alt"
                           :color="result.success ? 'success' : 'danger'" />
          </div>
          <div class="col-6 col-md-3">
            <base-info-box label="Version"
                           :value="version.slice(0,16)"
                           icon="fas fa-clock"
                           color="info" />
          </div>
        </div>

        <!-- Charts row -->
        <div class="row mb-4">

          <div class="col-12 col-md-4">
            <base-panel title="Overall Score" icon="fa-tachometer-alt" :no-padding="true">
              <div style="height:240px;">
                <chart-widget :option="gaugeOpt" height="240px" />
              </div>
            </base-panel>
          </div>

          <div v-if="barOpt" class="col-12 col-md-8">
            <base-panel title="Score per Rule" icon="fa-bars" :no-padding="true">
              <div style="height:240px;">
                <chart-widget :option="barOpt" height="240px" />
              </div>
            </base-panel>
          </div>

        </div>

        <!-- Trend (only if multiple versions) -->
        <base-panel v-if="trendOpt" title="Score Trend" icon="fa-chart-line" :no-padding="true" class="mb-4">
          <div style="height:200px;">
            <chart-widget :option="trendOpt" height="200px" />
          </div>
        </base-panel>

        <!-- Detail table -->
        <base-panel v-if="result.details && result.details.length"
                    title="Rule Details" icon="fa-list" :no-padding="true">
          <base-table :columns="DQ_COLUMNS" :items="result.details">

            <template #cell(rule_id)="{ item }">
              <code>{{ item.rule_id }}</code>
              <div v-if="item.error" class="text-danger small mt-1">{{ item.error }}</div>
            </template>

            <template #cell(status)="{ item }">
              <i v-if="item.success" class="fas fa-check-circle text-success"></i>
              <i v-else              class="fas fa-times-circle text-danger"></i>
            </template>

            <template #cell(score)="{ item }">
              {{ item.score !== null ? (item.score * 100).toFixed(1) + '%' : '—' }}
            </template>

            <template #cell(tolerance)="{ item }">
              <span class="text-muted">
                {{ item.tolerance !== undefined ? (item.tolerance * 100).toFixed(0) + '%' : '—' }}
              </span>
            </template>

          </base-table>
        </base-panel>

      </template>

    </base-page>
  `,
};
