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
  { key: 'id',         label: 'Name' },
  { key: 'policy',     label: 'Policy' },
  { key: 'schedule',   label: 'Schedule' },
  { key: 'tz',         label: 'TZ' },
  { key: 'enabled',    label: 'Enabled' },
  { key: 'last_fire',  label: 'Last Fire' },
  { key: 'actions',    label: '', class: 'text-right pr-3' },
];

const EXEC_POLICIES    = ['Ephemeral', 'Stateful'];
const CONCURR_POLICIES = ['Forbid', 'Replace', 'Allow'];

function emptyForm() {
  return {
    name:              '',
    jobRef:            '',
    schedule:          '',
    timezone:          'UTC',
    executionPolicy:   'Ephemeral',
    concurrencyPolicy: 'Forbid',
    enabled:           true,
    params:            [],   // [{key, value}]
    attributes:        [],   // [{key, value}]
  };
}

function cronToForm(cj) {
  const spec = cj.spec || {};
  return {
    name:              cj.id,
    jobRef:            (spec.jobRef || {}).name || '',
    schedule:          spec.schedule || '',
    timezone:          spec.timezone || 'UTC',
    executionPolicy:   spec.executionPolicy || 'Ephemeral',
    concurrencyPolicy: spec.concurrencyPolicy || 'Forbid',
    enabled:           cj.enabled !== false,
    params:     Object.entries(spec.params     || {}).map(([k, v]) => ({ key: k, value: v })),
    attributes: Object.entries(spec.attributes || {}).map(([k, v]) => ({ key: k, value: v })),
  };
}

function formToBody(form, namespace) {
  const params     = Object.fromEntries(form.params.filter(r => r.key.trim()).map(r => [r.key.trim(), r.value]));
  const attributes = Object.fromEntries(form.attributes.filter(r => r.key.trim()).map(r => [r.key.trim(), r.value]));
  const spec = {
    schedule:          form.schedule.trim(),
    timezone:          form.timezone.trim() || 'UTC',
    executionPolicy:   form.executionPolicy,
    concurrencyPolicy: form.concurrencyPolicy,
    enabled:           form.enabled,
    jobRef:            { name: form.jobRef.trim() },
  };
  if (Object.keys(params).length)     spec.params     = params;
  if (Object.keys(attributes).length) spec.attributes = attributes;
  return {
    kind:     'CronJob',
    metadata: { name: form.name.trim(), namespace },
    spec,
  };
}

export default {
  name: 'CronJobs',
  components: { BasePage, BasePanel, BaseTable, BaseButton, BaseButtonGroup, BaseModal, BaseInput, ConfirmDialog },

  setup() {
    const items      = ref([]);
    const jobDefs    = ref([]);
    const loading    = ref(false);
    const saving     = ref(false);
    const toggling   = ref(null);   // id of cron being toggled
    const pageError  = ref(null);
    const formError  = ref(null);
    const editMode   = ref(false);
    const modalRef   = ref(null);
    const confirmRef = ref(null);
    const form       = ref(emptyForm());

    const modalTitle = computed(() =>
      editMode.value ? `Edit — ${form.value.name}` : 'New CronJob'
    );

    // ── load ──────────────────────────────────────────────────────────────────

    async function load() {
      if (!nsStore.selected) { items.value = []; jobDefs.value = []; return; }
      loading.value   = true;
      pageError.value = null;
      try {
        const [crons, defs] = await Promise.all([
          api.cronJobs(nsStore.selected),
          api.jobDefinitions(nsStore.selected),
        ]);
        items.value   = crons;
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
      form.value      = cronToForm(item);
      modalRef.value?.open();
    }

    // ── params / attributes row helpers ───────────────────────────────────────

    function addParam()       { form.value.params.push({ key: '', value: '' }); }
    function removeParam(i)   { form.value.params.splice(i, 1); }
    function addAttr()        { form.value.attributes.push({ key: '', value: '' }); }
    function removeAttr(i)    { form.value.attributes.splice(i, 1); }

    // ── submit ────────────────────────────────────────────────────────────────

    async function submitForm() {
      formError.value = null;
      const f = form.value;
      if (!f.name.trim())    { formError.value = 'Name is required.';         return; }
      if (!f.schedule.trim()){ formError.value = 'Schedule is required.';     return; }
      if (!f.jobRef.trim())  { formError.value = 'Job Definition is required.'; return; }

      saving.value = true;
      try {
        await api.upsertCronJob(nsStore.selected, formToBody(f, nsStore.selected));
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
          await api.disableCronJob(nsStore.selected, item.id);
        } else {
          await api.enableCronJob(nsStore.selected, item.id);
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
        `Delete cron job "<b>${item.id}</b>"?`,
        async (confirmed) => {
          if (!confirmed) return;
          try {
            await api.deleteCronJob(nsStore.selected, item.id);
            await load();
          } catch (e) {
            pageError.value = e.message;
          }
        }
      );
    }

    const hasNs = computed(() => !!nsStore.selected);

    return {
      items, jobDefs, loading, saving, toggling, pageError, formError,
      editMode, modalTitle, form, columns: COLUMNS,
      modalRef, confirmRef, hasNs,
      execPolicies: EXEC_POLICIES, concurrPolicies: CONCURR_POLICIES,
      load, openCreate, openEdit, submitForm, toggleEnabled, deleteItem,
      addParam, removeParam, addAttr, removeAttr,
    };
  },

  template: `
    <base-page
      title="Cron Jobs"
      subtitle="Scheduled jobs fired automatically by the Boss scheduler"
      icon="fas fa-clock"
      :loading="loading"
      :error="pageError"
    >
      <template #actions>
        <base-button
          icon="fas fa-plus"
          color="primary"
          label="New CronJob"
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
        <i class="fas fa-info-circle mr-2"></i>Select a namespace to view cron jobs.
      </div>

      <base-panel v-else :no-padding="true">
        <base-table :columns="columns" :items="items">

          <template #cell(id)="{ item }">
            <code class="text-dark">{{ item.id }}</code>
          </template>

          <template #cell(policy)="{ item }">
            <span :class="['badge', (item.spec?.executionPolicy || 'Ephemeral') === 'Stateful' ? 'badge-primary' : 'badge-secondary']">
              {{ item.spec?.executionPolicy || 'Ephemeral' }}
            </span>
          </template>

          <template #cell(schedule)="{ item }">
            <code class="text-muted small">{{ item.spec?.schedule || '—' }}</code>
          </template>

          <template #cell(tz)="{ item }">
            <span class="small text-muted">{{ item.spec?.timezone || 'UTC' }}</span>
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

          <template #cell(last_fire)="{ item }">
            <span class="small text-muted">{{ item.last_fire ? item.last_fire.substring(0,19) : '—' }}</span>
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
      <base-modal ref="modalRef" :title="modalTitle" icon="fas fa-clock" size="lg" :scrollable="true">

        <!-- Name -->
        <div class="form-group">
          <label class="font-weight-bold">Name <span class="text-danger">*</span></label>
          <base-input
            v-if="!editMode"
            v-model="form.name"
            placeholder="e.g. daily-etl"
          />
          <div v-else>
            <code>{{ form.name }}</code>
            <small class="text-muted d-block">Name cannot be changed after creation.</small>
          </div>
        </div>

        <!-- Job Definition -->
        <div class="form-group">
          <label class="font-weight-bold">Job Definition <span class="text-danger">*</span></label>
          <select class="form-control form-control-sm" v-model="form.jobRef">
            <option value="" disabled>— select a job definition —</option>
            <option v-for="d in jobDefs" :key="d.id" :value="d.id">{{ d.id }}</option>
          </select>
          <small class="text-muted">References a Job Definition by name (jobRef).</small>
        </div>

        <!-- Schedule -->
        <div class="form-group">
          <label class="font-weight-bold">Schedule <span class="text-danger">*</span></label>
          <base-input
            v-model="form.schedule"
            placeholder="e.g. 0 6 * * *  (every day at 06:00)"
          />
          <small class="text-muted">Standard 5-field cron expression.</small>
        </div>

        <!-- Timezone -->
        <div class="form-group">
          <label class="font-weight-bold">Timezone</label>
          <base-input
            v-model="form.timezone"
            placeholder="UTC"
          />
          <small class="text-muted">IANA timezone name (e.g. Europe/Rome). Default: UTC.</small>
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
                <b>Ephemeral</b>: new job each fire.<br>
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

        <!-- Enabled -->
        <div class="form-group">
          <div class="form-check">
            <input class="form-check-input" type="checkbox" id="cron-enabled" v-model="form.enabled" />
            <label class="form-check-label font-weight-bold" for="cron-enabled">Enabled</label>
          </div>
          <small class="text-muted">Disabled cron jobs are stored but never fired.</small>
        </div>

        <!-- Params -->
        <div class="form-group">
          <label class="font-weight-bold">Params</label>
          <small class="text-muted d-block mb-1">
            Values containing <code>%</code> are treated as strftime formats (e.g. <code>%Y-%m-%d</code>).
          </small>
          <table v-if="form.params.length" class="table table-sm table-bordered mb-2">
            <thead class="thead-light">
              <tr>
                <th style="width:40%">Key</th>
                <th style="width:55%">Value / Format</th>
                <th style="width:5%"></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, i) in form.params" :key="i">
                <td><input class="form-control form-control-sm" v-model="row.key"   placeholder="key" /></td>
                <td><input class="form-control form-control-sm" v-model="row.value" placeholder="%Y-%m-%d or static" /></td>
                <td class="text-center">
                  <base-button icon="fas fa-times" color="outline-danger" @click="removeParam(i)" />
                </td>
              </tr>
            </tbody>
          </table>
          <base-button icon="fas fa-plus" color="outline-secondary" label="Add param" @click="addParam" />
        </div>

        <!-- Attributes -->
        <div class="form-group">
          <label class="font-weight-bold">Attributes</label>
          <table v-if="form.attributes.length" class="table table-sm table-bordered mb-2">
            <thead class="thead-light">
              <tr>
                <th style="width:40%">Key</th>
                <th style="width:55%">Value</th>
                <th style="width:5%"></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, i) in form.attributes" :key="i">
                <td><input class="form-control form-control-sm" v-model="row.key"   placeholder="key" /></td>
                <td><input class="form-control form-control-sm" v-model="row.value" placeholder="value" /></td>
                <td class="text-center">
                  <base-button icon="fas fa-times" color="outline-danger" @click="removeAttr(i)" />
                </td>
              </tr>
            </tbody>
          </table>
          <base-button icon="fas fa-plus" color="outline-secondary" label="Add attribute" @click="addAttr" />
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
