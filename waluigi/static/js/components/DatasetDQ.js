import { api }       from '../api.js';
import { nsStore }  from '../store.js';
import BasePage     from './BasePage.js';
import BasePanel    from './BasePanel.js';
import BaseButton   from './BaseButton.js';
import BaseInfoBox  from './BaseInfoBox.js';
import BaseTable    from './BaseTable.js';
import ChartWidget  from './ChartWidget.js';

const { ref, computed, onMounted, watch } = Vue;

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
    const loading  = ref(false);
    const error    = ref(null);
    const noResult = ref(false);

    const gaugeOpt = computed(() => result.value ? gaugeOption(result.value.score, result.value.success) : null);
    const barOpt   = computed(() => result.value?.details?.length ? barOption(result.value.details) : null);

    async function load() {
      if (!nsStore.selected) return;
      loading.value  = true;
      error.value    = null;
      noResult.value = false;
      try {
        const res    = await api.datasetDQResult(nsStore.selected, datasetId.value, version.value);
        result.value = res.data ?? null;
      } catch (e) {
        if (e.message.includes('404')) noResult.value = true;
        else error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    onMounted(load);
    watch(() => nsStore.selected, (ns) => { if (ns) load(); });

    return {
      datasetId, version, result,
      loading, error, noResult, gaugeOpt, barOpt,
      DQ_COLUMNS, load,
      goBack: () => router.go(-1),
    };
  },

  template: `
    <base-page title="Data Quality" :subtitle="datasetId" icon="fas fa-shield-alt" :loading="loading">

      <template #actions>
        <base-button label="Back"    icon="fas fa-arrow-left"  color="outline-secondary" @click="goBack" />
        <base-button label="Refresh" icon="fas fa-sync-alt"   color="outline-primary"   class="ml-2" :loading="loading" @click="load" />
        <base-button label="History" icon="fas fa-chart-line" color="outline-secondary" class="ml-auto"
                     @click="$router.push('/dq-history/' + datasetId)" />
      </template>

      <div v-if="error" class="alert alert-danger">
        <i class="fas fa-exclamation-triangle mr-1"></i>{{ error }}
      </div>

      <div v-if="noResult" class="text-center text-muted p-5">
        <i class="fas fa-shield-alt fa-3x mb-3"></i>
        <p>No DQ result for this version yet.</p>
        <p class="small">DQ runs automatically on commit if expectations are configured.</p>
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
