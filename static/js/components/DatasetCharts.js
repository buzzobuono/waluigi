import { api }      from '../api.js';
import BasePage     from './BasePage.js';
import BaseButton   from './BaseButton.js';
import BasePanel    from './BasePanel.js';
import ChartWidget  from './ChartWidget.js';

const { ref, computed, onMounted } = Vue;

export default {
  name: 'DatasetCharts',
  components: { BasePage, BaseButton, BasePanel, ChartWidget },

  setup() {
    const route  = VueRouter.useRoute();
    const router = VueRouter.useRouter();

    const datasetId = computed(() => {
      const raw = route.params.id;
      return (Array.isArray(raw) ? raw.join('/') : String(raw || '')).replace(/^\/|\/$/g, '');
    });
    const version = computed(() => route.params.version || '');

    const charts  = ref([]);
    const renders = ref({});   // chart.id → { option, loading, error }
    const loading = ref(false);
    const error   = ref(null);

    async function loadCharts() {
      if (!datasetId.value) return;
      loading.value = true;
      error.value   = null;
      charts.value  = [];
      renders.value = {};
      try {
        const res = await api.datasetCharts(datasetId.value);
        charts.value = res.data || [];
        await Promise.all(charts.value.map(chart => renderOne(chart)));
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    async function renderOne(chart) {
      renders.value[chart.id] = { option: null, loading: true, error: null };
      try {
        const res = await api.renderChart(datasetId.value, chart.id, version.value || undefined);
        renders.value[chart.id] = { option: res.data?.option ?? null, loading: false, error: null };
      } catch (e) {
        renders.value[chart.id] = { option: null, loading: false, error: e.message };
      }
    }

    function goBack() { router.push(`/datasets/${datasetId.value}/${version.value}`); }

    onMounted(loadCharts);

    return { charts, renders, loading, error, datasetId, version, loadCharts, goBack };
  },

  template: `
    <base-page
      title="Charts"
      :subtitle="datasetId + ' @ ' + version"
      icon="fas fa-chart-bar"
      :loading="loading && !charts.length"
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
          @click="loadCharts"
        />
      </template>

      <div v-if="!loading && !error && !charts.length" class="text-center text-muted p-5">
        <i class="fas fa-chart-bar fa-3x mb-3"></i>
        <p>No charts defined for this dataset.</p>
      </div>

      <div class="row">
        <div v-for="chart in charts" :key="chart.id" class="col-12 col-lg-6 mb-4">
          <base-panel :title="chart.title || chart.id">
            <div style="height: 320px;">
              <chart-widget
                :option="renders[chart.id]?.option"
                :loading="renders[chart.id]?.loading ?? true"
                :error="renders[chart.id]?.error"
                height="320px"
              />
            </div>
          </base-panel>
        </div>
      </div>

    </base-page>
  `,
};
