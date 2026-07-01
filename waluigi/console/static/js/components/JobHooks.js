import { api }        from '../api.js';
import { nsStore }    from '../store.js';
import BasePage        from './BasePage.js';
import BasePanel       from './BasePanel.js';
import BaseTable       from './BaseTable.js';
import BaseButton      from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseModal       from './BaseModal.js';
import BaseInput       from './BaseInput.js';
import ConfirmDialog   from './ConfirmDialog.js';

const { ref, computed, watch } = Vue;

const COLUMNS = [
  { key: 'id',          label: 'Name' },
  { key: 'watch_job',   label: 'Watches Job' },
  { key: 'events',      label: 'Events' },
  { key: 'trigger_job', label: 'Triggers Job' },
  { key: 'enabled',     label: 'Enabled' },
  { key: 'actions',     label: '', class: 'text-right pr-3' },
];

const EXEC_POLICIES    = ['Ephemeral', 'Stateful'];
const CONCURR_POLICIES = ['Forbid', 'Replace', 'Allow'];
const ALL_EVENTS       = ['success', 'failure'];

function emptyForm() {
  return {
    name:              '',
    watchJob:          '',
    events:            ['success', 'failure'],
    triggerJob:        '',
    executionPolicy:   'Ephemeral',
    concurrencyPolicy: 'Allow',
    enabled:           true,
    params:            [],   // [{key, value}]
  };
}

function hookToForm(h) {
  const spec    = h.spec    || {};
  const watch   = spec.watch   || {};
  const trigger = spec.trigger || {};
  return {
    name:              h.id,
    watchJob:          watch.job    || '',
    events:            (watch.events || []).slice(),
    triggerJob:        (trigger.jobRef || {}).name || '',
    executionPolicy:   trigger.executionPolicy   || 'Ephemeral',
    concurrencyPolicy: trigger.concurrencyPolicy || 'Allow',
    enabled:           h.enabled !== false,
    params:            Object.entries(trigger.params || {}).map(([k, v]) => ({ key: k, value: v })),
  };
}

function formToBody(form, namespace) {
  const params = Object.fromEntries(
    form.params.filter(r => r.key.trim()).map(r => [r.key.trim(), r.value])
  );
  const spec = {
    watch: {
      job:    form.watchJob.trim(),
      events: form.events.slice(),
    },
    trigger: {
      jobRef:            { name: form.triggerJob.trim() },
      executionPolicy:   form.executionPolicy,
      concurrencyPolicy: form.concurrencyPolicy,
    },
    enabled: form.enabled,
  };
  if (Object.keys(params).length) spec.trigger.params = params;
  return {
    kind:     'JobHook',
    metadata: { name: form.name.trim(), namespace },
    spec,
  };
}

export default {
  name: 'JobHooks',
  components: { BasePage, BasePanel, BaseTable, BaseButton, BaseButtonGroup, BaseModal, BaseInput, ConfirmDialog },

  setup() {
    const items      = ref([]);
    const jobDefs    = ref([]);
    const loading    = ref(false);
    const saving     = ref(false);
    const toggling   = ref(null);
    const pageError  = ref(null);
    const formError  = ref(null);
    const editMode   = ref(false);
    const modalRef   = ref(null);
    const confirmRef = ref(null);
    const form       = ref(emptyForm());

    const modalTitle = computed(() =>
      editMode.value ? `Edit — ${form.value.name}` : 'New Job Hook'
    );
    const hasNs = computed(() => !!nsStore.selected);

    // ── load ──────────────────────────────────────────────────────────────────

    async function load() {
      if (!nsStore.selected) { items.value = []; jobDefs.value = []; return; }
      loading.value   = true;
      pageError.value = null;
      try {
        const [hooks, defs] = await Promise.all([
          api.jobHooks(nsStore.selected),
          api.jobDefinitions(nsStore.selected),
        ]);
        items.value   = hooks;
        jobDefs.value = defs;
      } catch (e) {
        pageError.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    watch(() => nsStore.selected, load, { immediate: true });

    // ── create / edit modal ───────────────────────────────────────────────────

    function openCreate() {
      editMode.value  = false;
      formError.value = null;
      form.value      = emptyForm();
      modalRef.value?.open();
    }

    function openEdit(item) {
      editMode.value  = true;
      formError.value = null;
      form.value      = hookToForm(item);
      modalRef.value?.open();
    }

    function toggleEvent(ev) {
      const list = form.value.events;
      const idx  = list.indexOf(ev);
      if (idx >= 0) list.splice(idx, 1);
      else           list.push(ev);
    }

    // ── params row helpers ─────────────────────────────────────────────────────

    function addParam()     { form.value.params.push({ key: '', value: '' }); }
    function removeParam(i) { form.value.params.splice(i, 1); }

    // ── submit ────────────────────────────────────────────────────────────────

    async function submitForm() {
      formError.value = null;
      const f = form.value;
      if (!f.name.trim())       { formError.value = 'Name is required.';              return; }
      if (!f.watchJob.trim())   { formError.value = 'Watched Job Definition is required.'; return; }
      if (!f.events.length)     { formError.value = 'Select at least one event.';     return; }
      if (!f.triggerJob.trim()) { formError.value = 'Trigger Job Definition is required.'; return; }

      saving.value = true;
      try {
        await api.upsertJobHook(nsStore.selected, formToBody(f, nsStore.selected));
        modalRef.value?.close();
        await load();
      } catch (e) {
        formError.value = e.message;
      } finally {
        saving.value = false;
      }
    }

    // ── enable / disable ──────────────────────────────────────────────────────

    async function toggleEnabled(item) {
      toggling.value = item.id;
      try {
        if (item.enabled) {
          await api.disableJobHook(nsStore.selected, item.id);
        } else {
          await api.enableJobHook(nsStore.selected, item.id);
        }
        await load();
      } catch (e) {
        pageError.value = e.message;
      } finally {
        toggling.value = null;
      }
    }

    // ── delete ────────────────────────────────────────────────────────────────

    function deleteItem(item) {
      confirmRef.value.ask(
        `Delete job hook "<b>${item.id}</b>"?`,
        async (confirmed) => {
          if (!confirmed) return;
          try {
            await api.deleteJobHook(nsStore.selected, item.id);
            await load();
          } catch (e) {
            pageError.value = e.message;
          }
        }
      );
    }

    return {
      items, jobDefs, loading, saving, toggling, pageError, formError,
      editMode, modalTitle, form, columns: COLUMNS,
      modalRef, confirmRef, hasNs,
      allEvents: ALL_EVENTS, execPolicies: EXEC_POLICIES, concurrPolicies: CONCURR_POLICIES,
      load, openCreate, openEdit, submitForm, toggleEnabled, deleteItem,
      toggleEvent, addParam, removeParam,
    };
  },

  template: `
    <base-page
      title="Job Hooks"
      subtitle="Event-driven triggers: fire a job when another job succeeds or fails"
      icon="fas fa-bell"
      :loading="loading"
      :error="pageError"
    >
      <template #actions>
        <base-button
          icon="fas fa-plus"
          color="primary"
          label="New Job Hook"
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
        <i class="fas fa-info-circle mr-2"></i>Select a namespace to view job hooks.
      </div>

      <base-panel v-else :no-padding="true">
        <base-table :columns="columns" :items="items">

          <template #cell(id)="{ item }">
            <code class="text-dark">{{ item.id }}</code>
          </template>

          <template #cell(watch_job)="{ item }">
            <code class="text-muted small">{{ item.spec?.watch?.job || '—' }}</code>
          </template>

          <template #cell(events)="{ item }">
            <span
              v-for="ev in (item.spec?.watch?.events || [])"
              :key="ev"
              :class="['badge mr-1', ev === 'success' ? 'badge-success' : 'badge-danger']"
            >{{ ev }}</span>
          </template>

          <template #cell(trigger_job)="{ item }">
            <code class="text-muted small">{{ item.spec?.trigger?.jobRef?.name || '—' }}</code>
          </template>

          <template #cell(enabled)="{ item }">
            <span
              :class="['badge', item.enabled ? 'badge-success' : 'badge-secondary']"
              style="cursor:pointer;"
              :title="item.enabled ? 'Click to disable' : 'Click to enable'"
              @click="toggleEnabled(item)"
            >
              <i v-if="toggling === item.id" class="fas fa-spinner fa-spin mr-1"></i>
              {{ item.enabled ? 'yes' : 'no' }}
            </span>
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
      <base-modal ref="modalRef" :title="modalTitle" icon="fas fa-bell" size="lg" :scrollable="true">

        <!-- Name -->
        <div class="form-group">
          <label class="font-weight-bold">Name <span class="text-danger">*</span></label>
          <base-input
            v-if="!editMode"
            v-model="form.name"
            placeholder="e.g. notify-on-etl-done"
          />
          <div v-else>
            <code>{{ form.name }}</code>
            <small class="text-muted d-block">Name cannot be changed after creation.</small>
          </div>
        </div>

        <!-- Watch section -->
        <div class="card card-outline card-secondary mb-3">
          <div class="card-header py-2">
            <h6 class="mb-0"><i class="fas fa-eye mr-2 text-muted"></i>Watch</h6>
          </div>
          <div class="card-body pb-1">

            <!-- Watched Job Definition -->
            <div class="form-group">
              <label class="font-weight-bold">Job Definition <span class="text-danger">*</span></label>
              <select class="form-control form-control-sm" v-model="form.watchJob">
                <option value="" disabled>— select a job definition to watch —</option>
                <option v-for="d in jobDefs" :key="d.id" :value="d.id">{{ d.id }}</option>
              </select>
              <small class="text-muted">Watches all runs of this job definition (Ephemeral and Stateful).</small>
            </div>

            <!-- Events checkboxes -->
            <div class="form-group mb-0">
              <label class="font-weight-bold">Events <span class="text-danger">*</span></label>
              <div class="d-flex gap-3 mt-1">
                <div
                  v-for="ev in allEvents"
                  :key="ev"
                  class="form-check mr-3"
                >
                  <input
                    class="form-check-input"
                    type="checkbox"
                    :id="'ev-' + ev"
                    :checked="form.events.includes(ev)"
                    @change="toggleEvent(ev)"
                  />
                  <label class="form-check-label" :for="'ev-' + ev">
                    <span :class="['badge', ev === 'success' ? 'badge-success' : 'badge-danger']">{{ ev }}</span>
                  </label>
                </div>
              </div>
            </div>

          </div>
        </div>

        <!-- Trigger section -->
        <div class="card card-outline card-secondary mb-3">
          <div class="card-header py-2">
            <h6 class="mb-0"><i class="fas fa-bolt mr-2 text-muted"></i>Trigger</h6>
          </div>
          <div class="card-body pb-1">

            <!-- Trigger Job Definition -->
            <div class="form-group">
              <label class="font-weight-bold">Job Definition <span class="text-danger">*</span></label>
              <select class="form-control form-control-sm" v-model="form.triggerJob">
                <option value="" disabled>— select a job definition to trigger —</option>
                <option v-for="d in jobDefs" :key="d.id" :value="d.id">{{ d.id }}</option>
              </select>
            </div>

            <!-- Policies row -->
            <div class="row">
              <div class="col-6">
                <div class="form-group">
                  <label class="font-weight-bold">Execution Policy</label>
                  <select class="form-control form-control-sm" v-model="form.executionPolicy">
                    <option v-for="p in execPolicies" :key="p" :value="p">{{ p }}</option>
                  </select>
                  <small class="text-muted">
                    <b>Ephemeral</b>: new job per event.<br>
                    <b>Stateful</b>: reuse same job id.
                  </small>
                </div>
              </div>
              <div class="col-6">
                <div class="form-group">
                  <label class="font-weight-bold">Concurrency Policy</label>
                  <select class="form-control form-control-sm" v-model="form.concurrencyPolicy">
                    <option v-for="p in concurrPolicies" :key="p" :value="p">{{ p }}</option>
                  </select>
                  <small class="text-muted">
                    <b>Forbid</b>: skip if running.<br>
                    <b>Replace</b>: cancel and restart.<br>
                    <b>Allow</b>: run regardless.
                  </small>
                </div>
              </div>
            </div>

            <!-- Params -->
            <div class="form-group mb-0">
              <label class="font-weight-bold">Params</label>
              <small class="text-muted d-block mb-1">
                Use <code>\${event.status}</code>, <code>\${event.job_id}</code>,
                <code>\${event.job_name}</code>, <code>\${event.namespace}</code>,
                <code>\${event.failed_tasks}</code> as placeholders.
              </small>
              <table v-if="form.params.length" class="table table-sm table-bordered mb-2">
                <thead class="thead-light">
                  <tr>
                    <th style="width:35%">Key</th>
                    <th style="width:60%">Value / Template</th>
                    <th style="width:5%"></th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(row, i) in form.params" :key="i">
                    <td><input class="form-control form-control-sm" v-model="row.key"   placeholder="key" /></td>
                    <td><input class="form-control form-control-sm" v-model="row.value" placeholder="\${event.status}" /></td>
                    <td class="text-center">
                      <base-button icon="fas fa-times" color="outline-danger" @click="removeParam(i)" />
                    </td>
                  </tr>
                </tbody>
              </table>
              <base-button icon="fas fa-plus" color="outline-secondary" label="Add param" @click="addParam" />
            </div>

          </div>
        </div>

        <!-- Enabled -->
        <div class="form-group">
          <div class="form-check">
            <input class="form-check-input" type="checkbox" id="hook-enabled" v-model="form.enabled" />
            <label class="form-check-label font-weight-bold" for="hook-enabled">Enabled</label>
          </div>
          <small class="text-muted">Disabled hooks are stored but never fired.</small>
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
            :label="editMode ? 'Save' : 'Create'"
            :icon="editMode ? 'fas fa-save' : 'fas fa-check'"
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
