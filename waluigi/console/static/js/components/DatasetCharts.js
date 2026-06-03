import { api }      from '../api.js';
import { nsStore } from '../store.js';
import BasePage     from './BasePage.js';
import BaseButton   from './BaseButton.js';
import BasePanel    from './BasePanel.js';
import ChartWidget  from './ChartWidget.js';

const { ref, computed, watch } = Vue;

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
      if (!datasetId.value || !nsStore.selected) return;
      loading.value = true;
      error.value   = null;
      charts.value  = [];
      renders.value = {};
      try {
        const res = await api.datasetCharts(nsStore.selected, datasetId.value);
        charts.value = res.data || [];
        await Promise.all(charts.value.map(chart => renderOne(chart)));
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    async function renderOne(chart) {
      renders.value[chart.key] = { option: null, loading: true, error: null };
      try {
        const res = await api.renderChartByKey(nsStore.selected, datasetId.value, chart.key, version.value || undefined);
        renders.value[chart.key] = { option: res.data?.option ?? null, loading: false, error: null };
      } catch (e) {
        renders.value[chart.key] = { option: null, loading: false, error: e.message };
      }
    }

    function goBack() { router.go(-1); }

    watch(() => nsStore.selected, loadCharts, { immediate: true });

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
          class="ml-auto"
          :loading="loading"
          @click="loadCharts"
        />
      </template>

      <div v-if="!loading && !error && !charts.length" class="text-center text-muted p-5">
        <i class="fas fa-chart-bar fa-3x mb-3"></i>
        <p>No charts defined for this dataset.</p>
      </div>

      <div class="row">
        <div v-for="chart in charts" :key="chart.key" class="col-12 col-lg-6 mb-4">
          <base-panel :title="chart.title || chart.key">
            <div style="height: 320px;">
              <chart-widget
                :option="renders[chart.key]?.option"
                :loading="renders[chart.key]?.loading ?? true"
                :error="renders[chart.key]?.error"
                height="320px"
              />
            </div>
          </base-panel>
        </div>
      </div>

    </base-page>
  `,
};
