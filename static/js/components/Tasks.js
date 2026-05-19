import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseTable from './BaseTable.js';
import BaseButton from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseModal from './BaseModal.js';
import LogModal from './LogModal.js';
import ConfirmDialog from './ConfirmDialog.js';

const { ref, computed, onMounted } = Vue;
const PAGE_SIZE = 10;

export default {
  name: 'Tasks',
  components: { BasePage, BasePanel, BaseTable, LogModal, BaseButton, BaseButtonGroup, BaseModal, ConfirmDialog },

  setup() {
    const route        = VueRouter.useRoute();
    const tasks        = ref([]);
    const loading      = ref(false);
    const error        = ref(null);
    const logModalRef  = ref(null);
    const confirmRef   = ref(null);
    const pages        = ref({});

    const STATUS_COLOR = {
      SUCCESS: '#28a745',
      FAILED:  '#dc3545',
      RUNNING: '#ffc107',
      READY:   '#17a2b8',
      PENDING: '#6c757d',
    };

    const columns = [
      { key: 'id', label: 'Task ID' },
      { key: 'params', label: 'Params' },
      { key: 'status', label: 'Status' },
      { key: 'update', label: 'Last Update' },
      { key: 'actions', label: 'Actions', class: 'text-right pr-3' }
    ];

    async function load() {
      loading.value = true;
      error.value   = null;
      try {
        tasks.value = await api.tasks();
        pages.value = {};
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    const filterNs = computed(() => {
      const p = route.params.namespace;
      return p ? (Array.isArray(p) ? p.join('/') : p) : null;
    });

    const byNamespace = computed(() => {
      const map = {};
      const filtered = filterNs.value
        ? tasks.value.filter(t => t.namespace === filterNs.value)
        : tasks.value;
      filtered.forEach(t => {
        const ns = t.namespace || '(none)';
        if (!map[ns]) map[ns] = [];
        map[ns].push(t);
      });
      return map;
    });

    async function resetTask(id) {
      confirmRef.value.ask(`Reset task "${id}"?`, async (ok) => {
        if (!ok) return;
        await api.resetTask(id);
        await load();
      });
    }

    async function deleteTask(id) {
      confirmRef.value.ask(`Delete task "${id}"?`, async (ok) => {
        if (!ok) return;
        await api.deleteTask(id);
        await load();
      });
    }

    async function resetNs(ns) {
      confirmRef.value.ask(`Reset all in "${ns}"?`, async (ok) => {
        if (!ok) return;
        await api.resetNamespace(ns);
        await load();
      });
    }

    async function deleteNs(ns) {
      confirmRef.value.ask(`Delete all in "${ns}"?`, async (ok) => {
        if (!ok) return;
        await api.deleteNamespace(ns);
        await load();
      });
    }

    function openLogs(id) {
      if (logModalRef.value) logModalRef.value.show(id);
    }

    function getNsPage(ns) {
      return pages.value[ns] || 1;
    }

    function changeNsPage(ns, delta) {
      const total = Math.max(1, Math.ceil((byNamespace.value[ns] || []).length / PAGE_SIZE));
      const next  = getNsPage(ns) + delta;
      if (next >= 1 && next <= total) pages.value = { ...pages.value, [ns]: next };
    }

    function totalPagesFor(taskList) {
      return Math.max(1, Math.ceil(taskList.length / PAGE_SIZE));
    }

    function pagedTasksFor(ns, taskList) {
      const start = (getNsPage(ns) - 1) * PAGE_SIZE;
      return taskList.slice(start, start + PAGE_SIZE);
    }

    function rangeStartFor(ns) { return (getNsPage(ns) - 1) * PAGE_SIZE + 1; }
    function rangeEndFor(ns, taskList) { return Math.min(getNsPage(ns) * PAGE_SIZE, taskList.length); }

    onMounted(load);

    return {
      tasks, loading, error, columns, STATUS_COLOR,
      filterNs, byNamespace, logModalRef, confirmRef, pages,
      load, resetTask, deleteTask, resetNs, deleteNs, openLogs,
      getNsPage, changeNsPage, totalPagesFor, pagedTasksFor, rangeStartFor, rangeEndFor
    };
  },

  template: `
    <base-page
      :title="filterNs ? 'Tasks in ' + filterNs : 'All Tasks'"
      :subtitle="filterNs ? 'Namespace View' : 'Global View'"
      icon="fas fa-tasks"
      :loading="loading && !tasks.length"
      :error="error">

      <template #actions>
        <base-button
          v-if="filterNs"
          label="Back"
          icon="fas fa-arrow-left"
          color="outline-secondary"
          @click="$router.push('/namespaces')"
        />

        <base-button
          label="Update"
          class="ml-auto"
          icon="fas fa-sync-alt"
          color="outline-primary"
          :loading="loading"
          @click="load"
        />
      </template>

      <div v-if="!Object.keys(byNamespace).length" class="text-center py-5 text-muted">
        <i class="fas fa-filter fa-3x mb-3 opacity-75"></i>
        <p>No tasks found for this selection.</p>
      </div>

      <base-panel
        v-for="(taskList, ns) in byNamespace"
        :key="ns"
        :no-padding="true">

        <template #title>
          <i class="fas fa-layer-group mr-2"></i>
          <span class="font-weight-bold">Namespace: </span>
          <code class="ml-2">{{ ns }}</code>
        </template>

        <template #tools>
          <base-button-group>
            <base-button
              label="Reset"
              icon="fas fa-history"
              color="outline-warning"
              @click="resetNs(ns)"
            />
            <base-button
              label="Delete"
              icon="fas fa-trash-alt"
              color="outline-danger"
              @click="deleteNs(ns)"
            />
          </base-button-group>
          <base-button-group class="ml-auto">
            <base-button
              :disabled="loading || getNsPage(ns) <= 1"
              icon="fas fa-chevron-left"
              color="outline-primary"
              @click="changeNsPage(ns, -1)"
            />
            <base-button
              :label="String(getNsPage(ns)) + ' / ' + String(totalPagesFor(taskList))"
              :disabled="true"
              color="outline-secondary"
            />
            <base-button
              :disabled="loading || getNsPage(ns) >= totalPagesFor(taskList)"
              icon="fas fa-chevron-right"
              color="outline-primary"
              @click="changeNsPage(ns, 1)"
            />
          </base-button-group>
        </template>

        <base-table :columns="columns" :items="pagedTasksFor(ns, taskList)">

          <template #cell(id)="{ item }">
            <a href="#" @click.prevent="openLogs(item.id)">
              {{ item.id }}
            </a>
          </template>

          <template #cell(status)="{ item }">
            <span class="badge shadow" :style="{ background: STATUS_COLOR[item.status], color: '#fff', minWidth: '70px' }">
              {{ item.status }}
            </span>
          </template>

          <template #cell(update)="{ item }">
            <i class="far fa-clock mr-1"></i>{{ item.last_update || '—' }}
          </template>

          <template #cell(actions)="{ item }">
            <base-button-group>
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
            {{ taskList.length ? rangeStartFor(ns) + ' – ' + rangeEndFor(ns, taskList) + ' of ' + taskList.length : 'No tasks' }}
          </div>
        </template>

      </base-panel>

      <log-modal ref="logModalRef" />
      <confirm-dialog title="Confirm" ref="confirmRef" />

    </base-page>
  `
};
