import { api }        from '../api.js';
import { nsStore }    from '../store.js';
import BasePage        from './BasePage.js';
import BasePanel       from './BasePanel.js';
import BaseTable       from './BaseTable.js';
import BaseButton      from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseModal       from './BaseModal.js';
import ConfirmDialog   from './ConfirmDialog.js';

const { ref, computed, watch } = Vue;

const COLUMNS = [
  { key: 'id',      label: 'Name' },
  { key: 'tasks',   label: 'Tasks' },
  { key: 'workdir', label: 'Workdir' },
  { key: 'actions', label: '', class: 'text-right pr-3' },
];

const TASK_COLUMNS = [
  { key: 'id',        label: 'Task ID' },
  { key: 'type',      label: 'Type' },
  { key: 'resources', label: 'Resources' },
];

function taskType(t) {
  if (t.taskRef)  return `ref:${t.taskRef?.name || '?'}`;
  if (t.taskSpec) return 'inline';
  return '-';
}

function taskResources(t) {
  const res = t.resources || {};
  const pairs = Object.entries(res);
  return pairs.length ? pairs.map(([k, v]) => `${k}:${v}`).join(', ') : '-';
}

function flattenDefnTasks(tasks) {
  const byId = Object.fromEntries(tasks.map(t => [t.id, t]));
  const allRequired = new Set(tasks.flatMap(t => t.requires || []));
  const roots = tasks.filter(t => t.id && !allRequired.has(t.id));
  const result = [];
  const visited = new Set();
  function traverse(id, level) {
    if (visited.has(id)) return;
    visited.add(id);
    const t = byId[id];
    if (!t) return;
    result.push({ ...t, _level: level });
    for (const reqId of (t.requires || [])) {
      traverse(reqId, level + 1);
    }
  }
  if (roots.length) {
    traverse(roots[0].id, 0);
  } else {
    tasks.forEach(t => t.id && traverse(t.id, 0));
  }
  return result;
}

export default {
  name: 'JobDefinitions',
  components: { BasePage, BasePanel, BaseTable, BaseButton, BaseButtonGroup, BaseModal, ConfirmDialog },

  setup() {
    const items      = ref([]);
    const loading    = ref(false);
    const pageError  = ref(null);
    const modalRef   = ref(null);
    const confirmRef = ref(null);
    const selected   = ref(null);

    async function load() {
      if (!nsStore.selected) { items.value = []; return; }
      loading.value   = true;
      pageError.value = null;
      try {
        items.value = await api.jobDefinitions(nsStore.selected);
      } catch (e) {
        pageError.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    watch(() => nsStore.selected, load, { immediate: true });

    function openDetail(item) {
      selected.value = item;
      modalRef.value?.open();
    }

    function deleteItem(item) {
      confirmRef.value.ask(
        `Delete job definition "<b>${item.id}</b>"?<br><small class="text-muted">CronJobs that reference it via jobRef will fail to fire.</small>`,
        async (confirmed) => {
          if (!confirmed) return;
          try {
            await api.deleteJobDefinition(nsStore.selected, item.id);
            await load();
          } catch (e) {
            pageError.value = e.message;
          }
        }
      );
    }

    const hasNs = computed(() => !!nsStore.selected);

    const detailTasks = computed(() => {
      const raw = selected.value?.spec?.tasks || [];
      return flattenDefnTasks(raw).map(t => ({
        _level:    t._level,
        id:        t.id || '-',
        type:      taskType(t),
        resources: taskResources(t),
      }));
    });

    return {
      items, loading, pageError,
      columns: COLUMNS, taskColumns: TASK_COLUMNS,
      modalRef, confirmRef, selected, detailTasks, hasNs,
      load, openDetail, deleteItem,
    };
  },

  template: `
    <base-page
      title="Job Definitions"
      subtitle="Reusable job templates referenced by CronJobs via jobRef"
      icon="fas fa-list-alt"
      :loading="loading"
      :error="pageError"
    >
      <template #actions>
        <base-button
          icon="fas fa-sync-alt"
          color="outline-primary"
          label="Refresh"
          class="ml-auto"
          :loading="loading"
          @click="load"
        />
      </template>

      <div v-if="!hasNs" class="alert alert-warning">
        <i class="fas fa-info-circle mr-2"></i>Select a namespace to view job definitions.
      </div>

      <base-panel v-else :no-padding="true">
        <base-table :columns="columns" :items="items">

          <template #cell(id)="{ item }">
            <code class="text-dark" style="cursor:pointer;" @click="openDetail(item)">{{ item.id }}</code>
          </template>

          <template #cell(tasks)="{ item }">
            <span class="badge badge-secondary">{{ (item.spec?.tasks || []).length }}</span>
          </template>

          <template #cell(workdir)="{ item }">
            <code class="text-muted small">{{ item.metadata?.workdir || '—' }}</code>
          </template>

          <template #cell(actions)="{ item }">
            <base-button-group>
              <base-button
                icon="fas fa-eye"
                color="outline-secondary"
                title="View tasks"
                @click="openDetail(item)"
              />
              <base-button
                icon="fas fa-trash"
                color="outline-danger"
                title="Delete"
                @click="deleteItem(item)"
              />
            </base-button-group>
          </template>

        </base-table>
      </base-panel>

      <!-- ── Detail modal ───────────────────────────────────────────────────── -->
      <base-modal ref="modalRef" :title="selected?.id || ''" icon="fas fa-list-alt" size="xl">

        <div v-if="selected" class="mb-3">
          <table class="table table-sm table-borderless mb-0" style="width:auto;">
            <tbody>
              <tr>
                <td class="text-muted pr-3">Namespace</td>
                <td><code>{{ selected.namespace }}</code></td>
              </tr>
              <tr v-if="selected.metadata?.workdir">
                <td class="text-muted pr-3">Workdir</td>
                <td><code>{{ selected.metadata.workdir }}</code></td>
              </tr>
            </tbody>
          </table>
        </div>

        <h6 class="font-weight-bold mb-2">
          Tasks <span class="badge badge-secondary ml-1">{{ detailTasks.length }}</span>
        </h6>

        <div v-if="detailTasks.length === 0" class="text-muted small">No tasks defined.</div>

        <base-table v-else :columns="taskColumns" :items="detailTasks">

          <template #cell(id)="{ item }">
            <div :style="'padding-left:' + (item._level * 20) + 'px'" class="py-1 text-nowrap">
              <span v-if="item._level > 0" class="text-muted mr-1" style="font-family:monospace;">└─</span>
              <code class="text-dark">{{ item.id }}</code>
            </div>
          </template>

          <template #cell(type)="{ item }">
            <span :class="['badge', item.type.startsWith('ref:') ? 'badge-primary' : item.type === 'inline' ? 'badge-info' : 'badge-secondary']">
              {{ item.type }}
            </span>
          </template>

          <template #cell(resources)="{ item }">
            <code class="text-muted small">{{ item.resources }}</code>
          </template>

        </base-table>

        <template #footer>
          <base-button
            label="Close"
            icon="fas fa-times"
            color="outline-secondary"
            @click="modalRef.close()"
          />
        </template>
      </base-modal>

      <confirm-dialog title="Confirm" ref="confirmRef" />
    </base-page>
  `,
};
