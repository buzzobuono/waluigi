import { api } from '../api.js';
import BasePage from './BasePage.js';
import BaseButton from './BaseButton.js';
import DagChart from './DagChart.js';
import LogModal from './LogModal.js';
import ConfirmDialog from './ConfirmDialog.js';

export default {
  name: 'JobDag',
  components: { BasePage, BaseButton, DagChart, LogModal, ConfirmDialog },

  setup() {
    const route       = VueRouter.useRoute();
    const namespace   = Vue.ref(decodeURIComponent(route.params.namespace));
    const jobId       = Vue.ref(decodeURIComponent(route.params.jobId));
    const tasks       = Vue.ref([]);
    const job         = Vue.ref(null);
    const loading     = Vue.ref(false);
    const logModalRef = Vue.ref(null);
    const confirmRef  = Vue.ref(null);

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

    const resetTask = (id) => {
      confirmRef.value.ask(`Reset task "${id}"?`, async (ok) => {
        if (!ok) return;
        await api.resetTask(namespace.value, id);
        await load();
      });
    };

    const deleteTask = (id) => {
      confirmRef.value.ask(`Delete task "${id}"?`, async (ok) => {
        if (!ok) return;
        await api.deleteTask(namespace.value, id);
        await load();
      });
    };

    const openLogs = (id) => logModalRef.value?.show(namespace.value, id);

    Vue.onMounted(load);
    Vue.watch(() => [route.params.namespace, route.params.jobId], ([ns, jid]) => {
      namespace.value = decodeURIComponent(ns);
      jobId.value     = decodeURIComponent(jid);
      load();
    });

    return {
      namespace, jobId, job, tasks, loading,
      logModalRef, confirmRef, STATUS_COLOR,
      resetTask, deleteTask, openLogs, load
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
        <span class="ml-3 badge align-self-center"
              :class="job && job.execution_policy === 'Stateful' ? 'badge-primary' : 'badge-secondary'">
          {{ job ? (job.execution_policy || 'Ephemeral') : '…' }}
        </span>
        <span class="ml-2 align-self-center text-muted small">
          {{ job ? (job.concurrency_policy || 'Forbid') : '' }}
        </span>
        <base-button
          label="Refresh"
          icon="fas fa-sync-alt"
          color="outline-primary"
          class="ml-auto"
          :loading="loading"
          @click="load"
        />
      </template>

      <dag-chart
        v-if="tasks.length"
        :tasks="tasks"
        :colors="STATUS_COLOR"
        @show-logs="openLogs"
        @reset="resetTask"
        @delete="deleteTask"
      />

      <div v-else-if="!loading" class="text-center py-5 text-muted">
        <i class="fas fa-ghost fa-3x mb-3 opacity-50"></i>
        <p>No tasks found for job: {{ jobId }}</p>
      </div>

      <log-modal ref="logModalRef" />
      <confirm-dialog title="Confirm" ref="confirmRef" />
    </base-page>
  `
};
