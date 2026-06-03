import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseButton from './BaseButton.js';
import DagChart from './DagChart.js';
import TaskTreeTable from './TaskTreeTable.js';
import LogModal from './LogModal.js';

export default {
  name: 'JobDag',
  components: { BasePage, BasePanel, BaseButton, DagChart, TaskTreeTable, LogModal },

  setup() {
    const route      = VueRouter.useRoute();
    const namespace  = Vue.ref(decodeURIComponent(route.params.namespace));
    const jobId      = Vue.ref(decodeURIComponent(route.params.jobId));
    const tasks      = Vue.ref([]);
    const job        = Vue.ref(null);
    const loading    = Vue.ref(false);
    const logModalRef = Vue.ref(null);

    const STATUS_COLOR = {
      SUCCESS: '#28a745',
      FAILED:  '#dc3545',
      RUNNING: '#ffc107',
      READY:   '#17a2b8',
      PENDING: '#6c757d'
    };

    const load = async () => {
      loading.value = true;
      try {
        [job.value, tasks.value] = await Promise.all([
          api.job(namespace.value, jobId.value),
          api.jobTasks(namespace.value, jobId.value),
        ]);
      } finally {
        loading.value = false;
      }
    };

    const resetTask = async (id) => {
      if (confirm(`Reset task "${id}"?`)) {
        await api.resetTask(namespace.value, id);
        await load();
      }
    };

    const deleteTask = async (id) => {
      if (confirm(`Delete task "${id}"?`)) {
        await api.deleteTask(namespace.value, id);
        await load();
      }
    };

    const openLogs = (id) => logModalRef.value?.show(namespace.value, id);

    Vue.onMounted(load);
    Vue.watch(() => [route.params.namespace, route.params.jobId], ([ns, jid]) => {
      namespace.value = decodeURIComponent(ns);
      jobId.value     = decodeURIComponent(jid);
      load();
    });

    return {
      namespace, jobId, job, tasks, loading, logModalRef,
      STATUS_COLOR, resetTask, deleteTask, openLogs, load
    };
  },

  template: `
    <base-page
      title="Job Details"
      :subtitle="'Workflow DAG for ' + jobId"
      icon="fas fa-sitemap"
      :loading="loading && !tasks.length">

      <template #actions>
        <base-button
          label="Back"
          icon="fas fa-arrow-left"
          color="outline-secondary"
          @click="$router.push('/jobs')"
        />
        <span class="ml-3 badge badge-secondary align-self-center">
          <i class="fas fa-layer-group mr-1"></i>{{ namespace }}
        </span>
        <span v-if="job" :class="['ml-2 badge align-self-center', job.kind === 'StatefulJob' ? 'badge-primary' : 'badge-light border']">
          {{ job.kind || 'Job' }}
        </span>
        <base-button
          label="Refresh"
          icon="fas fa-sync-alt"
          color="outline-primary"
          class="ml-auto"
          @click="load"
        />
      </template>

      <div v-if="tasks.length">
        <base-panel class="mb-4">
          <template #title>
            <i class="fas fa-project-diagram mr-2 text-primary"></i>
            <span>{{ jobId }}</span>
          </template>
          <dag-chart :tasks="tasks" :colors="STATUS_COLOR" />
        </base-panel>

        <base-panel title="Tasks Tree" icon="fas fa-list" :no-padding="true">
          <task-tree-table
            :tasks="tasks"
            :colors="STATUS_COLOR"
            @reset="resetTask"
            @delete="deleteTask"
            @show-logs="openLogs"
          />
        </base-panel>
      </div>

      <div v-else-if="!loading" class="text-center py-5 text-muted">
        <i class="fas fa-ghost fa-3x mb-3 opacity-50"></i>
        <p>No tasks found for job: {{ jobId }}</p>
      </div>

      <log-modal ref="logModalRef" />
    </base-page>
  `
};
