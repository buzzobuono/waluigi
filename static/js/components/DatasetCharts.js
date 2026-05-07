import { api }      from '../api.js';
import BasePage     from './BasePage.js';
import BaseButton   from './BaseButton.js';
import ChartWidget  from './ChartWidget.js';

const { ref, computed, onMounted } = Vue;

export default {
  name: 'DatasetCharts',
  components: { BasePage, BaseButton, ChartWidget },

  setup() {
    const route = VueRouter.useRoute();
    const router = VueRouter.useRouter();

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

    function goBack() { router.push(`/schema/${datasetId.value}`); }

    onMounted(load);

    return { option, loading, error, title, datasetId, load, goBack };
  },

  template: `
    <base-page
      :title="title || 'Chart'"
      :subtitle="datasetId"
      icon="fas fa-chart-bar"
      :loading="loading && !option"
      :error="error">

      <template #actions>
        <base-button
          icon="fas fa-arrow-left"
          label="Back"
          color="outline-secondary"
          @click="goBack"
        />
        <base-button
          icon="fas fa-sync-alt"
          label="Refresh"
          color="outline-primary"
          class="ml-2"
          :loading="loading"
          @click="load"
        />
      </template>

      <div style="height: calc(100vh - 220px); min-height: 300px;">
        <chart-widget :option="option" :loading="loading" :error="error" height="100%" />
      </div>

    </base-page>
  `,
};
