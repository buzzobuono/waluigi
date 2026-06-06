import { api } from '../api.js';
import { nsStore } from '../store.js';
import { TASK_STATUSES, JOB_STATUSES } from '../config.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseInfoBox from './BaseInfoBox.js';
import BaseButton from './BaseButton.js';

const { ref, computed, watch } = Vue;

export default {
  name: 'Namespaces',
  components: { BasePage, BasePanel, BaseInfoBox, BaseButton },

  setup() {
    const tasks       = ref([]);
    const jobs        = ref([]);
    const cronJobs    = ref([]);
    const jobDefs     = ref([]);
    const taskDefs    = ref([]);
    const loading     = ref(false);
    const error       = ref(null);
    const lastUpdated = ref(null);

    async function load() {
      if (!nsStore.selected) {
        tasks.value = []; jobs.value = []; cronJobs.value = [];
        jobDefs.value = []; taskDefs.value = [];
        return;
      }
      loading.value = true;
      error.value   = null;
      try {
        const ov = await api.namespaceOverview(nsStore.selected);
        tasks.value    = Array.isArray(ov.tasks)            ? ov.tasks            : [];
        jobs.value     = Array.isArray(ov.jobs)             ? ov.jobs             : [];
        cronJobs.value = Array.isArray(ov.cron_jobs)        ? ov.cron_jobs        : [];
        jobDefs.value  = Array.isArray(ov.job_definitions)  ? ov.job_definitions  : [];
        taskDefs.value = Array.isArray(ov.task_definitions) ? ov.task_definitions : [];
        lastUpdated.value = new Date().toLocaleTimeString();
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    watch(() => nsStore.selected, () => load(), { immediate: true });

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

    const cronEnabled  = computed(() => cronJobs.value.filter(c => c.enabled).length);
    const cronDisabled = computed(() => cronJobs.value.filter(c => !c.enabled).length);

    return {
      nsStore, loading, error, lastUpdated,
      tasks, jobs, cronJobs, jobDefs, taskDefs,
      taskCounts, jobCounts, cronEnabled, cronDisabled,
      TASK_STATUSES, JOB_STATUSES,
      load,
    };
  },

  template: `
    <base-page
      title="Namespace"
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
          class="ml-auto"
          :loading="loading"
          :disabled="!nsStore.selected"
          @click="load"
        />
      </template>

      <div v-if="!nsStore.selected" class="text-center py-5 text-muted">
        <i class="fas fa-layer-group fa-3x mb-3 opacity-75"></i>
        <p>Select a namespace from the header to view its contents.</p>
      </div>

      <div v-else>

        <!-- Task statistics -->
        <base-panel class="mb-4">
          <template #title>
            <i class="fas fa-tasks mr-2 text-primary"></i>
            Tasks
            <span class="badge badge-secondary ml-2">{{ tasks.length }} total</span>
          </template>
          <div class="row">
            <div v-for="s in TASK_STATUSES" :key="s.key" class="col-6 col-sm-4 col-lg-2 mb-3">
              <base-info-box :label="s.key" :value="taskCounts[s.key]" :icon="s.icon" :color="s.color" />
            </div>
          </div>
        </base-panel>

        <!-- Job statistics -->
        <base-panel class="mb-4">
          <template #title>
            <i class="fas fa-briefcase mr-2 text-primary"></i>
            Jobs
            <span class="badge badge-secondary ml-2">{{ jobs.length }} total</span>
          </template>
          <div class="row">
            <div v-for="s in JOB_STATUSES" :key="s.key" class="col-6 col-sm-4 col-lg-2 mb-3">
              <base-info-box :label="s.key" :value="jobCounts[s.key]" :icon="s.icon" :color="s.color" />
            </div>
          </div>
        </base-panel>

        <!-- Definitions & CronJobs -->
        <div class="row">

          <div class="col-md-4 mb-4">
            <base-panel>
              <template #title>
                <i class="fas fa-clock mr-2 text-primary"></i>
                Cron Jobs
                <span class="badge badge-secondary ml-2">{{ cronJobs.length }} total</span>
              </template>
              <div class="row">
                <div class="col-6 mb-2">
                  <base-info-box label="Enabled"  :value="cronEnabled"  icon="fas fa-check-circle" color="success" />
                </div>
                <div class="col-6 mb-2">
                  <base-info-box label="Disabled" :value="cronDisabled" icon="fas fa-pause-circle"  color="secondary" />
                </div>
              </div>
            </base-panel>
          </div>

          <div class="col-md-4 mb-4">
            <base-panel>
              <template #title>
                <i class="fas fa-list-alt mr-2 text-primary"></i>
                Job Definitions
              </template>
              <div class="text-center py-3">
                <span class="display-4 font-weight-bold">{{ jobDefs.length }}</span>
                <div class="text-muted small mt-1">definitions</div>
              </div>
            </base-panel>
          </div>

          <div class="col-md-4 mb-4">
            <base-panel>
              <template #title>
                <i class="fas fa-cubes mr-2 text-primary"></i>
                Task Definitions
              </template>
              <div class="text-center py-3">
                <span class="display-4 font-weight-bold">{{ taskDefs.length }}</span>
                <div class="text-muted small mt-1">definitions</div>
              </div>
            </base-panel>
          </div>

        </div>

      </div>

    </base-page>
  `
};
