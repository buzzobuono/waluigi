import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseTable from './BaseTable.js';
import BaseButton from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import LogModal from './LogModal.js';
import ConfirmDialog from './ConfirmDialog.js';

const { ref, computed, onMounted } = Vue;
const PAGE_SIZE = 10;

export default {
  name: 'Tasks',
  components: { BasePage, BasePanel, BaseTable, LogModal, BaseButton, BaseButtonGroup, ConfirmDialog },

  setup() {
    const route        = VueRouter.useRoute();
    const tasks        = ref([]);
    const loading      = ref(false);
    const error        = ref(null);
    const logModalRef  = ref(null);
    const confirmRef   = ref(null);
    const currentPage  = ref(1);

    const availableNs  = ref([]);
    const selectedNs   = ref('');
    const nsLoading    = ref(false);

    const STATUS_COLOR = {
      SUCCESS: '#28a745',
      FAILED:  '#dc3545',
      RUNNING: '#ffc107',
      READY:   '#17a2b8',
      PENDING: '#6c757d',
    };

    const columns = [
      { key: 'id',      label: 'Task ID' },
      { key: 'job_id',  label: 'Job' },
      { key: 'params',  label: 'Params' },
      { key: 'status',  label: 'Status' },
      { key: 'update',  label: 'Last Update' },
      { key: 'actions', label: 'Actions', class: 'text-right pr-3' }
    ];

    async function loadNamespaces() {
      nsLoading.value = true;
      try {
        const data = await api.namespaces();
        availableNs.value = (Array.isArray(data) ? data : []).map(r => r.namespace);
        // preselect from route param if navigating from Namespaces page
        const paramNs = route.params.namespace;
        const presel  = paramNs ? (Array.isArray(paramNs) ? paramNs.join('/') : paramNs) : null;
        if (presel && availableNs.value.includes(presel)) {
          selectedNs.value = presel;
        } else if (!selectedNs.value && availableNs.value.length) {
          selectedNs.value = availableNs.value[0];
        }
      } catch (e) {
        error.value = e.message;
      } finally {
        nsLoading.value = false;
      }
    }

    async function load() {
      if (!selectedNs.value) return;
      loading.value = true;
      error.value   = null;
      try {
        tasks.value = await api.tasks(selectedNs.value);
        currentPage.value = 1;
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    async function onNsChange() {
      tasks.value = [];
      await load();
    }

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
      logModalRef.value?.show(selectedNs.value, taskId);
    }

    async function resetTask(id) {
      confirmRef.value.ask(`Reset task "${id}"?`, async (ok) => {
        if (!ok) return;
        try { await api.resetTask(selectedNs.value, id); await load(); }
        catch (e) { error.value = e.message; }
      });
    }

    async function deleteTask(id) {
      confirmRef.value.ask(`Delete task "${id}"?`, async (ok) => {
        if (!ok) return;
        try { await api.deleteTask(selectedNs.value, id); await load(); }
        catch (e) { error.value = e.message; }
      });
    }

    async function resetNs() {
      confirmRef.value.ask(`Reset all tasks in "${selectedNs.value}"?`, async (ok) => {
        if (!ok) return;
        try { await api.resetNamespace(selectedNs.value); await load(); }
        catch (e) { error.value = e.message; }
      });
    }

    async function deleteNs() {
      confirmRef.value.ask(`Delete entire namespace "${selectedNs.value}"?`, async (ok) => {
        if (!ok) return;
        try {
          await api.deleteNamespace(selectedNs.value);
          // namespace is gone — reload the selector
          await loadNamespaces();
          await load();
        } catch (e) { error.value = e.message; }
      });
    }

    onMounted(async () => {
      await loadNamespaces();
      await load();
    });

    return {
      tasks, pagedTasks, loading, error, columns, STATUS_COLOR,
      currentPage, totalPages, rangeStart, rangeEnd,
      availableNs, selectedNs, nsLoading,
      logModalRef, confirmRef,
      changePage, load, onNsChange,
      openLogs, resetTask, deleteTask, resetNs, deleteNs,
    };
  },

  template: `
    <base-page
      title="Tasks"
      subtitle="Task monitoring and management"
      icon="fas fa-tasks"
      :loading="loading && !tasks.length"
      :error="error">

      <template #actions>
        <div class="d-flex align-items-center w-100">
          <label class="mr-2 mb-0 text-muted small font-weight-bold text-nowrap">
            <i class="fas fa-layer-group mr-1"></i>Namespace
          </label>
          <select class="form-control form-control-sm mr-3" style="max-width: 220px;"
                  v-model="selectedNs" @change="onNsChange" :disabled="nsLoading">
            <option v-if="!availableNs.length" value="">— no namespaces —</option>
            <option v-for="ns in availableNs" :key="ns" :value="ns">{{ ns }}</option>
          </select>
          <base-button
            label="Reset NS"
            icon="fas fa-history"
            color="outline-warning"
            class="mr-2"
            :disabled="!selectedNs"
            @click="resetNs"
          />
          <base-button
            label="Delete NS"
            icon="fas fa-trash-alt"
            color="outline-danger"
            class="mr-2"
            :disabled="!selectedNs"
            @click="deleteNs"
          />
          <base-button
            label="Refresh"
            icon="fas fa-sync-alt"
            color="outline-primary"
            class="ml-auto"
            :loading="loading"
            @click="load"
          />
        </div>
      </template>

      <div v-if="!selectedNs" class="text-center py-5 text-muted">
        <i class="fas fa-layer-group fa-3x mb-3 opacity-75"></i>
        <p>No namespaces available.</p>
      </div>

      <base-panel v-else :no-padding="true">

        <template #tools>
          <base-button-group class="ml-auto">
            <base-button
              :disabled="loading || currentPage <= 1"
              icon="fas fa-chevron-left"
              color="outline-primary"
              @click="changePage(-1)"
            />
            <base-button
              :label="String(currentPage) + ' / ' + String(totalPages)"
              :disabled="true"
              color="outline-secondary"
            />
            <base-button
              :disabled="loading || currentPage >= totalPages"
              icon="fas fa-chevron-right"
              color="outline-primary"
              @click="changePage(1)"
            />
          </base-button-group>
        </template>

        <base-table :columns="columns" :items="pagedTasks">

          <template #cell(id)="{ item }">
            <a href="#" @click.prevent="openLogs(item.id)">{{ item.id }}</a>
          </template>

          <template #cell(job_id)="{ item }">
            <router-link v-if="item.job_id"
              :to="'/jobs/' + encodeURIComponent(selectedNs) + '/' + encodeURIComponent(item.job_id)"
              class="text-muted small">
              {{ item.job_id }}
            </router-link>
            <span v-else class="text-muted">—</span>
          </template>

          <template #cell(status)="{ item }">
            <span class="badge shadow"
              :style="{ background: STATUS_COLOR[item.status] || '#6c757d', color: '#fff', minWidth: '70px' }">
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
              <base-button
                icon="fas fa-terminal"
                color="outline-info"
                title="View Logs"
                @click="openLogs(item.id)"
              />
              <base-button
                icon="fas fa-undo"
                color="outline-warning"
                title="Reset Task"
                @click="resetTask(item.id)"
              />
              <base-button
                icon="fas fa-trash"
                color="outline-danger"
                title="Delete Task"
                @click="deleteTask(item.id)"
              />
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
