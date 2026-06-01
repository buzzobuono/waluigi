import { api }          from '../api.js';
import { nsStore }      from '../store.js';
import BasePage          from './BasePage.js';
import BasePanel         from './BasePanel.js';
import BaseTable         from './BaseTable.js';
import BaseButton        from './BaseButton.js';
import BaseButtonGroup   from './BaseButtonGroup.js';
import BaseModal         from './BaseModal.js';
import BaseInput         from './BaseInput.js';
import ConfirmDialog     from './ConfirmDialog.js';

const { ref, computed, watch, onMounted } = Vue;

const COLUMNS = [
  { key: 'id',      label: 'Name' },
  { key: 'mode',    label: 'Type' },
  { key: 'preview', label: 'Command / Script' },
  { key: 'res',     label: 'Resources' },
  { key: 'actions', label: '',  class: 'text-right pr-3' },
];

function specToForm(spec) {
  const hasScript = !!spec.script;
  return {
    mode:      hasScript ? 'script' : 'command',
    command:   spec.command  || '',
    script:    spec.script   || '',
    resources: Object.entries(spec.resources || { coin: 1 }).map(([k, v]) => ({ k, v: String(v) })),
  };
}

function formToSpec(form) {
  const spec = {};
  if (form.mode === 'script') {
    spec.script = form.script;
  } else {
    spec.command = form.command;
  }
  const res = {};
  for (const { k, v } of form.resources) {
    const key = k.trim();
    if (key) res[key] = parseFloat(v) || 1;
  }
  if (Object.keys(res).length) spec.resources = res;
  return spec;
}

function resLabel(spec) {
  const r = spec?.resources;
  if (!r || !Object.keys(r).length) return '—';
  return Object.entries(r).map(([k, v]) => `${k}: ${v}`).join(', ');
}

export default {
  name: 'TaskDefinitions',
  components: { BasePage, BasePanel, BaseTable, BaseButton, BaseButtonGroup, BaseModal, BaseInput, ConfirmDialog },

  setup() {
    const items      = ref([]);
    const loading    = ref(false);
    const saving     = ref(false);
    const pageError  = ref(null);
    const formError  = ref(null);
    const mode       = ref('create');   // 'create' | 'edit'
    const modalRef   = ref(null);
    const confirmRef = ref(null);

    const form = ref({
      name:      '',
      mode:      'command',
      command:   '',
      script:    '',
      resources: [{ k: 'coin', v: '1' }],
    });

    const modalTitle = computed(() =>
      mode.value === 'edit' ? `Edit — ${form.value.name}` : 'New Task Definition'
    );

    // ── load ──────────────────────────────────────────────────────────────────

    async function load() {
      if (!nsStore.selected) { items.value = []; return; }
      loading.value  = true;
      pageError.value = null;
      try {
        items.value = await api.taskDefinitions(nsStore.selected);
      } catch (e) {
        pageError.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    watch(() => nsStore.selected, load);
    onMounted(load);

    // ── create ────────────────────────────────────────────────────────────────

    function openCreate() {
      mode.value      = 'create';
      formError.value = null;
      form.value      = { name: '', mode: 'command', command: '', script: '', resources: [{ k: 'coin', v: '1' }] };
      modalRef.value?.open();
    }

    // ── edit ──────────────────────────────────────────────────────────────────

    function openEdit(item) {
      mode.value      = 'edit';
      formError.value = null;
      form.value      = { name: item.id, ...specToForm(item.spec || {}) };
      modalRef.value?.open();
    }

    // ── resources rows ────────────────────────────────────────────────────────

    function addResource()       { form.value.resources.push({ k: '', v: '1' }); }
    function removeResource(i)   { form.value.resources.splice(i, 1); }

    // ── submit ────────────────────────────────────────────────────────────────

    async function submitForm() {
      formError.value = null;
      const name = form.value.name.trim();
      if (!name) { formError.value = 'Name is required.'; return; }
      if (form.value.mode === 'command' && !form.value.command.trim()) {
        formError.value = 'Command is required.'; return;
      }
      if (form.value.mode === 'script' && !form.value.script.trim()) {
        formError.value = 'Script is required.'; return;
      }

      saving.value = true;
      try {
        await api.upsertTaskDefinition(nsStore.selected, {
          kind:     'TaskDefinition',
          metadata: { name, namespace: nsStore.selected },
          spec:     formToSpec(form.value),
        });
        modalRef.value?.close();
        await load();
      } catch (e) {
        formError.value = e.message;
      } finally {
        saving.value = false;
      }
    }

    // ── delete ────────────────────────────────────────────────────────────────

    function deleteItem(item) {
      confirmRef.value.ask(
        `Delete task definition "<b>${item.id}</b>"?<br><small class="text-muted">Jobs that reference it via taskRef will fail.</small>`,
        async (confirmed) => {
          if (!confirmed) return;
          try {
            await api.deleteTaskDefinition(nsStore.selected, item.id);
            await load();
          } catch (e) {
            pageError.value = e.message;
          }
        }
      );
    }

    const hasNs = computed(() => !!nsStore.selected);

    return {
      items, loading, saving, pageError, formError,
      mode, modalTitle, form, columns: COLUMNS,
      modalRef, confirmRef, hasNs,
      load, openCreate, openEdit, submitForm,
      addResource, removeResource, deleteItem, resLabel,
    };
  },

  template: `
    <base-page
      title="Task Definitions"
      subtitle="Reusable task templates referenced via taskRef"
      icon="fas fa-cubes"
      :loading="loading"
      :error="pageError"
    >
      <template #actions>
        <base-button
          icon="fas fa-plus"
          color="primary"
          label="New Definition"
          class="mr-2"
          :disabled="loading || !hasNs"
          @click="openCreate"
        />
        <base-button
          icon="fas fa-sync-alt"
          color="outline-primary"
          label="Refresh"
          :loading="loading"
          @click="load"
        />
      </template>

      <div v-if="!hasNs" class="alert alert-warning">
        <i class="fas fa-info-circle mr-2"></i>Select a namespace to view task definitions.
      </div>

      <base-panel v-else :no-padding="true">
        <base-table :columns="columns" :items="items">

          <template #cell(id)="{ item }">
            <code class="text-dark">{{ item.id }}</code>
          </template>

          <template #cell(mode)="{ item }">
            <span :class="['badge', item.spec?.script ? 'badge-info' : 'badge-secondary']">
              {{ item.spec?.script ? 'script' : 'command' }}
            </span>
          </template>

          <template #cell(preview)="{ item }">
            <code class="text-muted small" style="white-space:pre-wrap;word-break:break-all;">
              {{ item.spec?.script
                  ? item.spec.script.trim().split('\\n').slice(0,2).join(' · ').substring(0,80) + (item.spec.script.trim().split('\\n').length > 2 ? ' …' : '')
                  : (item.spec?.command || '—') }}
            </code>
          </template>

          <template #cell(res)="{ item }">
            <span class="text-muted small">{{ resLabel(item.spec) }}</span>
          </template>

          <template #cell(actions)="{ item }">
            <base-button-group>
              <base-button
                icon="fas fa-pencil-alt"
                color="outline-secondary"
                title="Edit"
                @click="openEdit(item)"
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

      <!-- ── Create / Edit modal ────────────────────────────────────────────── -->
      <base-modal ref="modalRef" :title="modalTitle" icon="fas fa-cube" size="lg">

        <!-- Name -->
        <div class="form-group">
          <label class="font-weight-bold">
            Name <span v-if="mode === 'create'" class="text-danger">*</span>
          </label>
          <base-input
            v-if="mode === 'create'"
            v-model="form.name"
            placeholder="e.g. run-etl, send-report"
          />
          <div v-else>
            <code>{{ form.name }}</code>
            <small class="text-muted d-block">Name cannot be changed after creation.</small>
          </div>
        </div>

        <!-- Mode toggle -->
        <div class="form-group">
          <label class="font-weight-bold">Execution type</label>
          <div>
            <div class="form-check form-check-inline">
              <input class="form-check-input" type="radio" id="mode-cmd"    value="command" v-model="form.mode" />
              <label class="form-check-label" for="mode-cmd">Command</label>
            </div>
            <div class="form-check form-check-inline">
              <input class="form-check-input" type="radio" id="mode-script" value="script"  v-model="form.mode" />
              <label class="form-check-label" for="mode-script">Inline script</label>
            </div>
          </div>
        </div>

        <!-- Command -->
        <div v-if="form.mode === 'command'" class="form-group">
          <label class="font-weight-bold">
            Command <span class="text-danger">*</span>
          </label>
          <base-input
            v-model="form.command"
            placeholder="e.g. python /app/tasks/run_etl.py"
          />
          <small class="text-muted">Shell command executed by the Worker.</small>
        </div>

        <!-- Script -->
        <div v-if="form.mode === 'script'" class="form-group">
          <label class="font-weight-bold">
            Script <span class="text-danger">*</span>
          </label>
          <textarea
            class="form-control form-control-sm"
            style="font-family:monospace;font-size:12px;min-height:140px;"
            v-model="form.script"
            placeholder="import os&#10;print('hello')"
            spellcheck="false"
          ></textarea>
          <small class="text-muted">Python code executed inline via <code>WALUIGI_SCRIPT</code>.</small>
        </div>

        <!-- Resources -->
        <div class="form-group mb-0">
          <label class="font-weight-bold">Resources</label>
          <div
            v-for="(row, i) in form.resources"
            :key="i"
            class="d-flex align-items-center mb-1"
            style="gap:6px;"
          >
            <base-input v-model="row.k" placeholder="name"   style="width:120px;" />
            <span class="text-muted">:</span>
            <base-input v-model="row.v" placeholder="amount" style="width:80px;" />
            <base-button
              icon="fas fa-times"
              color="outline-danger"
              size="sm"
              :disabled="form.resources.length === 1"
              @click="removeResource(i)"
            />
          </div>
          <base-button
            icon="fas fa-plus"
            color="outline-secondary"
            size="sm"
            label="Add resource"
            class="mt-1"
            @click="addResource"
          />
        </div>

        <div v-if="formError" class="alert alert-danger mt-3 mb-0 py-2 small">
          <i class="fas fa-exclamation-circle mr-1"></i> {{ formError }}
        </div>

        <template #footer>
          <base-button
            label="Cancel"
            icon="fas fa-times"
            color="outline-secondary"
            @click="modalRef.close()"
          />
          <base-button
            :label="mode === 'edit' ? 'Save' : 'Create'"
            :icon="mode === 'edit' ? 'fas fa-save' : 'fas fa-check'"
            color="primary"
            :loading="saving"
            @click="submitForm"
          />
        </template>
      </base-modal>

      <confirm-dialog title="Confirm" ref="confirmRef" />
    </base-page>
  `,
};
