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

const { ref, computed, watch } = Vue;

const COLUMNS = [
  { key: 'id',       label: 'Name' },
  { key: 'mode',     label: 'Type' },
  { key: 'affinity', label: 'Affinity' },
  { key: 'preview',  label: 'Command / Script' },
  { key: 'actions',  label: '',  class: 'text-right pr-3' },
];

function specToForm(spec) {
  const hasScript = !!spec.script;
  return {
    mode:     hasScript ? 'script' : 'command',
    command:  spec.command || '',
    script:   spec.script  || '',
    affinity: (spec.affinity || []).join(', '),
  };
}

function formToSpec(form) {
  const affinity = form.affinity
    ? form.affinity.split(',').map(s => s.trim()).filter(Boolean)
    : [];
  const spec = form.mode === 'script' ? { script: form.script } : { command: form.command };
  if (affinity.length) spec.affinity = affinity;
  return spec;
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
      name:     '',
      mode:     'command',
      command:  '',
      script:   '',
      affinity: '',
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

    watch(() => nsStore.selected, load, { immediate: true });

    // ── create ────────────────────────────────────────────────────────────────

    function openCreate() {
      mode.value      = 'create';
      formError.value = null;
      form.value      = { name: '', mode: 'command', command: '', script: '', affinity: '' };
      modalRef.value?.open();
    }

    // ── edit ──────────────────────────────────────────────────────────────────

    function openEdit(item) {
      mode.value      = 'edit';
      formError.value = null;
      form.value      = { name: item.id, ...specToForm(item.spec || {}) };
      modalRef.value?.open();
    }

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
      load, openCreate, openEdit, submitForm, deleteItem,
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
          class="ml-auto"
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

          <template #cell(affinity)="{ item }">
            <span v-if="item.spec?.affinity?.length">
              <span
                v-for="tag in item.spec.affinity"
                :key="tag"
                class="badge badge-primary mr-1"
              >{{ tag }}</span>
            </span>
            <span v-else class="text-muted">—</span>
          </template>

          <template #cell(preview)="{ item }">
            <code class="text-muted small" style="white-space:pre-wrap;word-break:break-all;">
              {{ item.spec?.script
                  ? item.spec.script.trim().split('\\n').slice(0,2).join(' · ').substring(0,80) + (item.spec.script.trim().split('\\n').length > 2 ? ' …' : '')
                  : (item.spec?.command || '—') }}
            </code>
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

        <!-- Affinity -->
        <div class="form-group">
          <label class="font-weight-bold">Affinity</label>
          <base-input
            v-model="form.affinity"
            placeholder="e.g. python, gpu"
          />
          <small class="text-muted">Comma-separated capability tags. Workers must have all listed tags to run this task.</small>
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
