import { api }      from '../api.js';
import ChartWidget from './ChartWidget.js';

const { ref, computed, onMounted } = Vue;

// Single-chart embed — no page chrome, fills the available viewport.
// Route: /chart/:id+/:cid(\d+)
// Embeddable as-is or composable in Dashboard.
export default {
  name: 'DatasetCharts',
  components: { ChartWidget },

  setup() {
    const route = VueRouter.useRoute();

    const datasetId = computed(() => {
      const raw = route.params.id;
      return (Array.isArray(raw) ? raw.join('/') : String(raw || '')).replace(/^\/|\/$/g, '');
    });
    const chartId = computed(() => Number(route.params.cid));

    const option  = ref(null);
    const loading = ref(false);
    const error   = ref(null);
    const title   = ref('');

    async function load() {
      if (!datasetId.value || !chartId.value) return;
      loading.value = true;
      error.value   = null;
      try {
        const res  = await api.renderChart(datasetId.value, chartId.value);
        option.value = res.data?.option ?? null;
        title.value  = res.data?.title  ?? '';
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    onMounted(load);

    return { option, loading, error, title };
  },

  template: `
    <div style="display:flex; flex-direction:column; height:calc(100vh - 120px);">
      <div v-if="title" class="text-muted small px-1 py-1" style="flex-shrink:0; line-height:1.2;">
        {{ title }}
      </div>
      <div style="flex:1; min-height:0;">
        <chart-widget :option="option" :loading="loading" :error="error" height="100%" />
      </div>
    </div>
  `,
};
