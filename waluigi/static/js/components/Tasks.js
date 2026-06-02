import { api } from '../api.js';
import { nsStore } from '../store.js';
import { TASK_STATUS, TASK_STATUSES } from '../config.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseTable from './BaseTable.js';
import BaseInfoBox from './BaseInfoBox.js';
import BaseButton from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import LogModal from './LogModal.js';
import ConfirmDialog from './ConfirmDialog.js';

const { ref, computed, watch } = Vue;
const PAGE_SIZE = 10;

export default {
  name: 'Tasks',
  components: { BasePage, BasePanel, BaseTable, BaseInfoBox, LogModal, BaseButton, BaseButtonGroup, ConfirmDialog },

  setup() {
    const tasks       = ref([]);
    const loading     = ref(false);
    const error       = ref(null);
    const logModalRef = ref(null);
    const confirmRef  = ref(null);
    const currentPage = ref(1);

    const columns = [
      { key: 'id',      label: 'Task ID' },
      { key: 'job_id',  label: 'Job' },
      { key: 'params',  label: 'Params' },
      { key: 'status',  label: 'Status' },
      { key: 'update',  label: 'Last Update' },
      { key: 'actions', label: 'Actions', class: 'text-right pr-3' }
    ];

    async function load() {
      if (!nsStore.selected) { tasks.value = []; return; }
      loading.value = true;
      error.value   = null;
      try {
        tasks.value = await api.tasks(nsStore.selected);
        currentPage.value = 1;
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    watch(() => nsStore.selected, load, { immediate: true });

    const counts = computed(() => {
      const c = Object.fromEntries(TASK_STATUSES.map(s => [s.key, 0]));
      tasks.value.forEach(t => { if (c[t.status] !== undefined) c[t.status]++; });
      return c;
    });

    const totalPages = computed(() => Math.max(1, Math.ceil(tasks.value.length / PAGE_SIZE)));
    const pagedTasks = computed(() => {
      const start = (currentPage.value - 1) * PAGE_SIZE;
      return tasks.value.slice(start, start + PAGE_SIZE);
    });
    const rangeStart = computed(() => (currentPage.value - 1) * PAGE_SIZE + 1);
    const rangeEnd   = computed(() => Math.min(currentPage.value * PAGE_SIZE, tasks.value.length));

    function changePage(delta) {
      const next = currentPage.value + delta;
      if (next >= 1 && next <= totalPages.value) currentPage.value = next;
    }

    function openLogs(taskId) {
      logModalRef.value?.show(nsStore.selected, taskId);
    }

    async function resetTask(id) {
      confirmRef.value.ask(`Reset task "${id}"?`, async (ok) => {
        if (!ok) return;
        try { await api.resetTask(nsStore.selected, id); await load(); }
        catch (e) { error.value = e.message; }
      });
    }

    async function deleteTask(id) {
      confirmRef.value.ask(`Delete task "${id}"?`, async (ok) => {
        if (!ok) return;
        try { await api.deleteTask(nsStore.selected, id); await load(); }
        catch (e) { error.value = e.message; }
      });
    }

    async function resetNs() {
      confirmRef.value.ask(`Reset all tasks in "${nsStore.selected}"?`, async (ok) => {
        if (!ok) return;
        try { await api.resetNamespace(nsStore.selected); await load(); }
        catch (e) { error.value = e.message; }
      });
    }

    async function deleteNs() {
      confirmRef.value.ask(`Delete entire namespace "${nsStore.selected}"?`, async (ok) => {
        if (!ok) return;
        try {
          await api.deleteNamespace(nsStore.selected);
          const idx = nsStore.available.indexOf(nsStore.selected);
          nsStore.available.splice(idx, 1);
          nsStore.selected = nsStore.available[0] || '';
          tasks.value = [];
        } catch (e) { error.value = e.message; }
      });
    }

    return {
      tasks, pagedTasks, loading, error, columns, TASK_STATUS, TASK_STATUSES, nsStore,
      counts, currentPage, totalPages, rangeStart, rangeEnd,
      logModalRef, confirmRef,
      changePage, load, openLogs, resetTask, deleteTask, resetNs, deleteNs,
    };
  },

  template: `
    <base-page
      title="Tasks"
      :subtitle="nsStore.selected ? 'Namespace: ' + nsStore.selected : 'Select a namespace'"
      icon="fas fa-tasks"
      :loading="loading && !tasks.length"
      :error="error">

      <template #actions>
        <div class="row w-100 m-0">
          <div class="col-6 col-md-2 px-1" v-for="s in TASK_STATUSES" :key="s.key">
            <base-info-box
              :label="s.key"
              :value="counts[s.key]"
              :icon="s.icon"
              :color="s.color"
            />
          </div>
        </div>
        <div class="d-flex w-100 mt-2">
          <base-button label="Reset NS" icon="fas fa-history" color="outline-warning"
                       class="mr-2" :disabled="!nsStore.selected" @click="resetNs" />
          <base-button label="Delete NS" icon="fas fa-trash-alt" color="outline-danger"
                       class="mr-2" :disabled="!nsStore.selected" @click="deleteNs" />
          <base-button label="Refresh" icon="fas fa-sync-alt" color="outline-primary"
                       class="ml-auto" :loading="loading" @click="load" />
        </div>
      </template>

      <div v-if="!nsStore.selected" class="text-center py-5 text-muted">
        <i class="fas fa-layer-group fa-3x mb-3 opacity-75"></i>
        <p>Select a namespace from the header to view tasks.</p>
      </div>

      <base-panel v-else :no-padding="true">

        <template #tools>
          <base-button-group class="ml-auto">
            <base-button :disabled="loading || currentPage <= 1"
                         icon="fas fa-chevron-left" color="outline-primary"
                         @click="changePage(-1)" />
            <base-button :label="currentPage + ' / ' + totalPages"
                         :disabled="true" color="outline-secondary" />
            <base-button :disabled="loading || currentPage >= totalPages"
                         icon="fas fa-chevron-right" color="outline-primary"
                         @click="changePage(1)" />
          </base-button-group>
        </template>

        <base-table :columns="columns" :items="pagedTasks">

          <template #cell(id)="{ item }">
            <a href="#" @click.prevent="openLogs(item.id)">{{ item.id }}</a>
          </template>

          <template #cell(job_id)="{ item }">
            <router-link v-if="item.job_id"
              :to="'/jobs/' + encodeURIComponent(nsStore.selected) + '/' + encodeURIComponent(item.job_id)"
              class="text-muted small">
              {{ item.job_id }}
            </router-link>
            <span v-else class="text-muted">—</span>
          </template>

          <template #cell(status)="{ item }">
            <span :class="['badge shadow', 'badge-' + (TASK_STATUS[item.status]?.color || 'secondary'), item.status === 'RUNNING' ? 'blink' : '']">
              {{ item.status }}
            </span>
          </template>

          <template #cell(update)="{ item }">
            <span class="text-muted small">
              <i class="far fa-clock mr-1"></i>{{ item.last_update || '—' }}
            </span>
          </template>

          <template #cell(actions)="{ item }">
            <base-button-group>
              <base-button icon="fas fa-terminal" color="outline-info"
                           title="View Logs" @click="openLogs(item.id)" />
              <base-button icon="fas fa-undo" color="outline-warning"
                           title="Reset Task" @click="resetTask(item.id)" />
              <base-button icon="fas fa-trash" color="outline-danger"
                           title="Delete Task" @click="deleteTask(item.id)" />
            </base-button-group>
          </template>

        </base-table>

        <template #footer>
          <div class="text-muted small">
            {{ tasks.length ? rangeStart + ' – ' + rangeEnd + ' of ' + tasks.length : 'No tasks' }}
          </div>
        </template>

      </base-panel>

      <log-modal ref="logModalRef" />
      <confirm-dialog title="Confirm" ref="confirmRef" />
    </base-page>
  `
};
