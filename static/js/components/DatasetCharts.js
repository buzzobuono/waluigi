import { api }        from '../api.js';
import BasePage      from './BasePage.js';
import BasePanel     from './BasePanel.js';
import BaseButton    from './BaseButton.js';

const { ref, computed, onMounted, onBeforeUnmount, watch, nextTick } = Vue;

// ── ChartWidget ───────────────────────────────────────────────────────────────
// Self-contained ECharts widget. Receives an `option` object and renders it.
const ChartWidget = {
  name: 'ChartWidget',
  props: {
    option:  { type: Object,  default: null },
    loading: { type: Boolean, default: false },
    error:   { type: String,  default: null },
    height:  { type: String,  default: '320px' },
  },
  setup(props) {
    const elRef = ref(null);
    let ec = null;

    function init() {
      if (!elRef.value || !window.echarts) return;
      ec = window.echarts.init(elRef.value, null, { renderer: 'canvas' });
      if (props.option) ec.setOption(props.option);
    }

    function resize() { ec?.resize(); }

    onMounted(() => { nextTick(init); });
    onBeforeUnmount(() => { ec?.dispose(); ec = null; });

    watch(() => props.option, (opt) => {
      if (!opt) return;
      if (!ec) { nextTick(init); return; }
      ec.setOption(opt, true);
    });

    return { elRef, resize };
  },
  template: `
    <div style="position:relative;">
      <div v-if="loading" class="d-flex justify-content-center align-items-center"
           :style="{ height }">
        <i class="fas fa-spinner fa-spin text-muted fa-2x"></i>
      </div>
      <div v-else-if="error" class="alert alert-warning small m-2">
        <i class="fas fa-exclamation-triangle mr-1"></i>{{ error }}
      </div>
      <div v-else ref="elRef" :style="{ width: '100%', height }"></div>
    </div>
  `,
};

// ── DatasetCharts page ────────────────────────────────────────────────────────
export default {
  name: 'DatasetCharts',
  components: { BasePage, BasePanel, BaseButton, ChartWidget },

  setup() {
    const route  = VueRouter.useRoute();
    const router = VueRouter.useRouter();

    const datasetId = computed(() => {
      const raw = route.params.id;
      return (Array.isArray(raw) ? raw.join('/') : String(raw || '')).replace(/^\/|\/$/g, '');
    });

    const charts     = ref([]);
    const renders    = ref({});   // { [chart.id]: { loading, error, option } }
    const pageLoading = ref(false);
    const pageError   = ref(null);

    async function loadAll() {
      if (!datasetId.value) return;
      pageLoading.value = true;
      pageError.value   = null;
      try {
        const res = await api.datasetCharts(datasetId.value);
        charts.value = res.data || [];
        // kick off renders in parallel
        await Promise.all(charts.value.map(c => renderChart(c)));
      } catch (e) {
        pageError.value = e.message;
      } finally {
        pageLoading.value = false;
      }
    }

    async function renderChart(chart) {
      renders.value[chart.id] = { loading: true, error: null, option: null };
      try {
        const res = await api.renderChart(datasetId.value, chart.id);
        renders.value[chart.id] = { loading: false, error: null, option: res.data?.option || null };
      } catch (e) {
        renders.value[chart.id] = { loading: false, error: e.message, option: null };
      }
    }

    onMounted(loadAll);

    return {
      datasetId, charts, renders, pageLoading, pageError,
      goBack:       () => router.go(-1),
      goToSchema:   () => router.push('/schema/' + datasetId.value),
    };
  },

  template: `
    <base-page
      title="Charts"
      :subtitle="datasetId"
      icon="fas fa-chart-bar"
      :loading="pageLoading">

      <template #actions>
        <base-button label="Schema" icon="fas fa-project-diagram"
                     color="outline-warning" class="ml-auto" @click="goToSchema" />
        <base-button label="Back"   icon="fas fa-arrow-left"
                     color="outline-secondary" class="ml-2" @click="goBack" />
      </template>

      <div v-if="pageError" class="alert alert-danger">{{ pageError }}</div>

      <div v-if="!pageLoading && !charts.length" class="alert alert-info">
        No charts defined for this dataset.
        <a href="#" @click.prevent="goToSchema">Go to Schema</a> to add some.
      </div>

      <div class="row">
        <div v-for="chart in charts" :key="chart.id" class="col-12 col-lg-6 mb-4">
          <base-panel :title="chart.title" icon="fa-chart-bar" :no-padding="true">
            <template #tools>
              <span class="badge badge-secondary mr-2">{{ chart.spec.type || 'bar' }}</span>
            </template>
            <chart-widget
              :option="renders[chart.id]?.option"
              :loading="renders[chart.id]?.loading"
              :error="renders[chart.id]?.error"
              height="300px"
            />
            <div v-if="renders[chart.id]?.option" class="px-3 pb-2 text-muted small text-right">
              {{ chart.spec.x?.field }}
              <span v-if="chart.spec.y?.field"> / {{ chart.spec.y?.field }}
                <span v-if="chart.spec.y?.agg">({{ chart.spec.y.agg }})</span>
              </span>
            </div>
          </base-panel>
        </div>
      </div>

    </base-page>
  `,
};
