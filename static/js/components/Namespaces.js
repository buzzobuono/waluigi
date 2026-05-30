import { api } from '../api.js';
import { nsStore } from '../store.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseInfoBox from './BaseInfoBox.js';
import BaseButton from './BaseButton.js';

const { ref, computed, watch, onMounted } = Vue;

const TASK_STATUSES = [
  { key: 'PENDING',   color: 'secondary', icon: 'fas fa-clock' },
  { key: 'READY',     color: 'info',      icon: 'fas fa-check-circle' },
  { key: 'RUNNING',   color: 'warning',   icon: 'fas fa-spinner fa-spin' },
  { key: 'SUCCESS',   color: 'success',   icon: 'fas fa-check' },
  { key: 'FAILED',    color: 'danger',    icon: 'fas fa-times' },
  { key: 'CANCELLED', color: 'dark',      icon: 'fas fa-ban' },
];

const JOB_STATUSES = [
  { key: 'PENDING',   color: 'secondary', icon: 'fas fa-clock' },
  { key: 'RUNNING',   color: 'warning',   icon: 'fas fa-spinner fa-spin' },
  { key: 'PAUSED',    color: 'info',      icon: 'fas fa-pause' },
  { key: 'SUCCESS',   color: 'success',   icon: 'fas fa-check' },
  { key: 'FAILED',    color: 'danger',    icon: 'fas fa-times' },
  { key: 'CANCELLED', color: 'dark',      icon: 'fas fa-ban' },
];

export default {
  name: 'Namespaces',
  components: { BasePage, BasePanel, BaseInfoBox, BaseButton },

  setup() {
    const tasks   = ref([]);
    const jobs    = ref([]);
    const loading = ref(false);
    const error   = ref(null);
    const lastUpdated = ref(null);

    async function load() {
      if (!nsStore.selected) { tasks.value = []; jobs.value = []; return; }
      loading.value = true;
      error.value   = null;
      try {
        const [t, j] = await Promise.all([
          api.tasks(nsStore.selected),
          api.jobs(nsStore.selected),
        ]);
        tasks.value      = Array.isArray(t) ? t : [];
        jobs.value       = Array.isArray(j) ? j : [];
        lastUpdated.value = new Date().toLocaleTimeString();
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    watch(() => nsStore.selected, () => load());
    onMounted(load);

    const taskCounts = computed(() => {
      const c = Object.fromEntries(TASK_STATUSES.map(s => [s.key, 0]));
      tasks.value.forEach(t => { if (c[t.status] !== undefined) c[t.status]++; });
      return c;
    });

    const jobCounts = computed(() => {
      const c = Object.fromEntries(JOB_STATUSES.map(s => [s.key, 0]));
      jobs.value.forEach(j => { if (c[j.status] !== undefined) c[j.status]++; });
      return c;
    });

    return {
      nsStore, loading, error, lastUpdated,
      taskCounts, jobCounts,
      TASK_STATUSES, JOB_STATUSES,
      load,
    };
  },

  template: `
    <base-page
      title="Namespace Overview"
      :subtitle="nsStore.selected || 'Select a namespace from the header'"
      icon="fas fa-layer-group"
      :loading="loading"
      :error="error">

      <template #actions>
        <span v-if="lastUpdated" class="text-muted small mr-3 align-self-center">
          <i class="far fa-clock mr-1"></i>Updated at {{ lastUpdated }}
        </span>
        <base-button
          label="Refresh"
          icon="fas fa-sync-alt"
          color="outline-primary"
          :loading="loading"
          :disabled="!nsStore.selected"
          @click="load"
        />
      </template>

      <div v-if="!nsStore.selected" class="text-center py-5 text-muted">
        <i class="fas fa-layer-group fa-3x mb-3 opacity-75"></i>
        <p>Select a namespace from the header to view its statistics.</p>
      </div>

      <template v-else>

        <!-- Task statistics -->
        <base-panel class="mb-4">
          <template #title>
            <i class="fas fa-tasks mr-2 text-primary"></i>
            Tasks
            <span class="badge badge-secondary ml-2">{{ tasks.length }} total</span>
          </template>

          <div class="row">
            <div
              v-for="s in TASK_STATUSES" :key="s.key"
              class="col-6 col-sm-4 col-lg-2 mb-3">
              <base-info-box
                :label="s.key"
                :value="taskCounts[s.key]"
                :icon="s.icon"
                :color="s.color"
              />
            </div>
          </div>
        </base-panel>

        <!-- Job statistics -->
        <base-panel>
          <template #title>
            <i class="fas fa-briefcase mr-2 text-primary"></i>
            Jobs
            <span class="badge badge-secondary ml-2">{{ jobs.length }} total</span>
          </template>

          <div class="row">
            <div
              v-for="s in JOB_STATUSES" :key="s.key"
              class="col-6 col-sm-4 col-lg-2 mb-3">
              <base-info-box
                :label="s.key"
                :value="jobCounts[s.key]"
                :icon="s.icon"
                :color="s.color"
              />
            </div>
          </div>
        </base-panel>

      </template>

    </base-page>
  `
};
