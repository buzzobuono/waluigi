import { api } from '../api.js';
import BasePage      from './BasePage.js';
import BasePanel     from './BasePanel.js';
import BaseButton    from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseTable     from './BaseTable.js';
import BaseModal     from './BaseModal.js';
import BaseInput     from './BaseInput.js';
import BaseInfoBox   from './BaseInfoBox.js';
import ConfirmDialog from './ConfirmDialog.js';

const { ref, computed, onMounted, watch } = Vue;

const PII_TYPES = ['none', 'direct', 'indirect', 'sensitive'];

const STATUS_BADGE = {
  inferred:  'badge-secondary',
  draft:     'badge-warning',
  published: 'badge-success',
};

const SCHEMA_COLUMNS = [
  { key: 'column_name',   label: 'Column' },
  { key: 'physical_type', label: 'Physical type' },
  { key: 'logical_type',  label: 'Logical type' },
  { key: 'description',   label: 'Description' },
  { key: 'nullable',      label: 'Nullable' },
  { key: 'pii',           label: 'PII' },
  { key: 'status',        label: 'Status' },
  { key: 'actions',       label: '', class: 'text-right pr-3' },
];

const EXP_COLUMNS = [
  { key: 'rule_id',   label: 'Rule' },
  { key: 'inputs',    label: 'Inputs' },
  { key: 'params',    label: 'Params' },
  { key: 'tolerance', label: 'Tolerance', class: 'text-center' },
  { key: 'actions',   label: '',          class: 'text-right pr-3' },
];

export default {
  name: 'DatasetSchema',
  components: {
    BasePage, BasePanel, BaseButton, BaseButtonGroup,
    BaseTable, BaseModal, BaseInput, BaseInfoBox, ConfirmDialog,
  },

  setup() {
    const route  = VueRouter.useRoute();
    const router = VueRouter.useRouter();

    const datasetId = computed(() => {
      const raw = route.params.id;
      const joined = Array.isArray(raw) ? raw.join('/') : String(raw || '');
      return joined.replace(/^\/|\/$/g, '');
    });

    const schemaData     = ref(null);
    const expectations   = ref([]);
    const availableRules = ref([]);
    const loading        = ref(false);
    const saving         = ref(false);
    const expSaving      = ref(false);
    const pageError      = ref(null);
    const formError      = ref(null);
    const expError       = ref(null);

    // schema edit modal
    const modalRef          = ref(null);
    const confirmPublishRef = ref(null);
    const confirmDeleteRef  = ref(null);
    const editCol           = ref(null);
    const pendingDelete     = ref(null);

    const form = ref({
      logical_type: '',
      description:  '',
      nullable:     true,
      pii:          false,
      pii_type:     'none',
      pii_notes:    '',
    });

    // expectation modal
    const expModalRef  = ref(null);
    const expEditId    = ref(null);
    const expForm      = ref({ rule_id: '', inputs: {}, params: {}, tolerance: 1.0, position: 0 });
    const confirmExpDeleteRef = ref(null);

    const selectedRule = computed(() =>
      availableRules.value.find(r => r.id === expForm.value.rule_id) || null
    );

    watch(() => expForm.value.rule_id, (newId) => {
      const rule = availableRules.value.find(r => r.id === newId);
      if (!rule) return;
      // Preserve current values where keys match the new rule's schema.
      // This keeps saved values intact when opening edit, and resets to ''
      // only for keys the current form doesn't already have.
      const curInputs = expForm.value.inputs || {};
      const newInputs = {};
      for (const key of Object.keys(rule.inputs_schema || {})) {
        newInputs[key] = curInputs[key] ?? '';
      }
      const curParams = expForm.value.params || {};
      const newParams = {};
      for (const key of Object.keys(rule.params_schema || {})) {
        newParams[key] = curParams[key] ?? '';
      }
      expForm.value.inputs = newInputs;
      expForm.value.params = newParams;
    });

    async function loadAll() {
      if (!datasetId.value) return;
      loading.value   = true;
      pageError.value = null;
      try {
        const [schemaRes, expRes, rulesRes] = await Promise.all([
          api.catalogDatasetSchema(datasetId.value),
          api.datasetExpectations(datasetId.value),
          api.dqRules(),
        ]);
        schemaData.value   = schemaRes.data || null;
        expectations.value = expRes.data || [];
        availableRules.value = rulesRes.data || [];
      } catch (e) {
        pageError.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    function openEdit(col) {
      editCol.value   = col.column_name;
      formError.value = null;
      form.value = {
        logical_type: col.logical_type || '',
        description:  col.description  || '',
        nullable:     col.nullable !== false,
        pii:          !!col.pii,
        pii_type:     col.pii_type  || 'none',
        pii_notes:    col.pii_notes || '',
      };
      modalRef.value?.open();
    }

    async function submitEdit() {
      formError.value = null;
      saving.value    = true;
      try {
        const body = {
          logical_type: form.value.logical_type || null,
          description:  form.value.description  || null,
          nullable:     form.value.nullable,
          pii:          form.value.pii,
          pii_type:     form.value.pii_type  || null,
          pii_notes:    form.value.pii_notes || null,
        };
        const res = await api.catalogSchemaUpdateColumn(datasetId.value, editCol.value, body);
        if (res.diagnostic?.result === 'KO') {
          formError.value = res.diagnostic?.messages?.[0] || 'Error saving column';
          return;
        }
        modalRef.value?.close();
        await loadAll();
      } catch (e) {
        formError.value = e.message;
      } finally {
        saving.value = false;
      }
    }

    function askApproveColumn(col) {
      confirmPublishRef.value?.ask(
        `Approve column "${col.column_name}"? It will be promoted to "published".`,
        async (ok) => { if (ok) await approveColumn(col.column_name); }
      );
    }

    async function approveColumn(columnName) {
      saving.value = true;
      try {
        const res = await api.catalogSchemaApproveColumn(datasetId.value, columnName);
        if (res.diagnostic?.result === 'KO') {
          pageError.value = res.diagnostic?.messages?.[0] || 'Error approving column';
          return;
        }
        await loadAll();
      } catch (e) {
        pageError.value = e.message;
      } finally {
        saving.value = false;
      }
    }

    function askDeleteColumn(col) {
      pendingDelete.value = col.column_name;
      confirmDeleteRef.value?.ask(
        `Delete column "${col.column_name}" from the schema? This cannot be undone.`,
        async (ok) => { if (ok) await deleteColumn(col.column_name); }
      );
    }

    async function deleteColumn(columnName) {
      saving.value = true;
      try {
        const res = await api.catalogSchemaDeleteColumn(datasetId.value, columnName);
        if (res.diagnostic?.result === 'KO') {
          pageError.value = res.diagnostic?.messages?.[0] || 'Error deleting column';
          return;
        }
        await loadAll();
      } catch (e) {
        pageError.value = e.message;
      } finally {
        saving.value    = false;
        pendingDelete.value = null;
      }
    }

    function askPublishAll() {
      confirmPublishRef.value?.ask(
        'Promote ALL remaining columns to "published"?',
        async (ok) => { if (ok) await publishAll(); }
      );
    }

    async function publishAll() {
      saving.value = true;
      try {
        await api.catalogSchemaPublish(datasetId.value, { published_by: 'admin' });
        await loadAll();
      } catch (e) {
        pageError.value = e.message;
      } finally {
        saving.value = false;
      }
    }

    function openAddExpectation() {
      expEditId.value = null;
      expError.value  = null;
      expForm.value   = { rule_id: '', inputs: {}, params: {}, tolerance: 1.0, position: expectations.value.length };
      expModalRef.value?.open();
    }

    function openEditExpectation(exp) {
      expEditId.value = exp.id;
      expError.value  = null;
      expForm.value = {
        rule_id:   exp.rule_id,
        inputs:    { ...exp.inputs },
        params:    { ...exp.params },
        tolerance: exp.tolerance,
        position:  exp.position,
      };
      expModalRef.value?.open();
    }

    async function submitExpectation() {
      expError.value  = null;
      expSaving.value = true;
      try {
        const body = {
          rule_id:   expForm.value.rule_id,
          inputs:    expForm.value.inputs,
          params:    expForm.value.params,
          tolerance: parseFloat(expForm.value.tolerance) || 1.0,
          position:  parseInt(expForm.value.position)    || 0,
        };
        let res;
        if (expEditId.value !== null) {
          res = await api.updateExpectation(datasetId.value, expEditId.value, body);
        } else {
          res = await api.addExpectation(datasetId.value, body);
        }
        if (res.diagnostic?.result === 'KO') {
          expError.value = res.diagnostic?.messages?.[0] || 'Error saving expectation';
          return;
        }
        expModalRef.value?.close();
        await loadAll();
      } catch (e) {
        expError.value = e.message;
      } finally {
        expSaving.value = false;
      }
    }

    function askDeleteExpectation(exp) {
      confirmExpDeleteRef.value?.ask(
        `Delete expectation "${exp.rule_id}"?`,
        async (ok) => { if (ok) await deleteExpectation(exp.id); }
      );
    }

    async function deleteExpectation(expId) {
      try {
        await api.deleteExpectation(datasetId.value, expId);
        await loadAll();
      } catch (e) {
        pageError.value = e.message;
      }
    }

    onMounted(loadAll);

    return {
      datasetId, schemaData, expectations, availableRules,
      loading, saving, expSaving,
      pageError, formError, expError,
      SCHEMA_COLUMNS, EXP_COLUMNS, STATUS_BADGE, PII_TYPES,
      modalRef, confirmPublishRef, confirmDeleteRef,
      editCol, pendingDelete, form,
      expModalRef, expEditId, expForm, selectedRule, confirmExpDeleteRef,
      openEdit, submitEdit,
      askApproveColumn, askDeleteColumn, askPublishAll,
      openAddExpectation, openEditExpectation, submitExpectation, askDeleteExpectation,
      goBack: () => router.go(-1),
      goToRules: () => router.push('/dq/rules'),
    };
  },

  template: `
    <base-page
      title="Schema"
      :subtitle="datasetId"
      icon="fas fa-project-diagram"
      :loading="loading">

      <template #actions>
        <div v-if="schemaData" class="row w-100 m-0">
          <div class="col-6 col-md-3 col-xl-2 px-1">
            <base-info-box label="Total"     :value="schemaData.summary.total"     icon="fas fa-list"      color="primary"   />
          </div>
          <div class="col-6 col-md-3 col-xl-2 px-1">
            <base-info-box label="Inferred"  :value="schemaData.summary.inferred"  icon="fas fa-robot"     color="secondary" />
          </div>
          <div class="col-6 col-md-3 col-xl-2 px-1">
            <base-info-box label="Draft"     :value="schemaData.summary.draft"     icon="fas fa-pen"       color="warning"   />
          </div>
          <div class="col-6 col-md-3 col-xl-2 px-1">
            <base-info-box label="Published" :value="schemaData.summary.published" icon="fas fa-check"     color="success"   />
          </div>
          <div class="col-6 col-md-3 col-xl-2 px-1">
            <base-info-box label="PII"       :value="schemaData.summary.pii"       icon="fas fa-user-lock" color="danger"    />
          </div>
        </div>
        <base-button
          label="Back"
          icon="fas fa-arrow-left"
          color="outline-secondary"
          class="ml-auto"
          @click="goBack"
        />
        <base-button
          label="Publish All"
          icon="fas fa-check-double"
          color="outline-success"
          class="ml-2"
          :disabled="saving || !schemaData"
          @click="askPublishAll"
        />
      </template>

      <div v-if="pageError" class="alert alert-danger">{{ pageError }}</div>

      <!-- DQ Expectations -->
      <base-panel title="DQ Expectations" icon="fa-shield-alt" :no-padding="true">

        <template #tools>
          <base-button
            icon="fas fa-external-link-alt"
            color="outline-secondary"
            title="Browse available rules"
            @click="goToRules"
          />
          <base-button
            icon="fas fa-plus"
            label="Add"
            color="outline-primary"
            class="ml-1"
            @click="openAddExpectation"
          />
        </template>

        <base-table :columns="EXP_COLUMNS" :items="expectations">

          <template #cell(rule_id)="{ item }">
            <code class="small">{{ item.rule_id }}</code>
          </template>

          <template #cell(inputs)="{ item }">
            <span v-for="(col, ph) in item.inputs" :key="ph" class="d-block small">
              <code>{{ ph }}</code><span class="text-muted mx-1">→</span>{{ col }}
            </span>
            <span v-if="!Object.keys(item.inputs).length" class="text-muted small">—</span>
          </template>

          <template #cell(params)="{ item }">
            <span v-for="(val, name) in item.params" :key="name" class="d-block small">
              <code>{{ name }}</code><span class="text-muted">: {{ val }}</span>
            </span>
            <span v-if="!Object.keys(item.params).length" class="text-muted small">—</span>
          </template>

          <template #cell(tolerance)="{ item }">
            <span class="badge badge-secondary">
              {{ item.tolerance === 1 ? '100%' : (item.tolerance * 100).toFixed(0) + '%' }}
            </span>
          </template>

          <template #cell(actions)="{ item }">
            <base-button-group>
              <base-button
                icon="fas fa-pencil-alt"
                color="outline-primary"
                title="Edit"
                @click="openEditExpectation(item)"
              />
              <base-button
                icon="fas fa-trash"
                color="outline-danger"
                title="Delete"
                @click="askDeleteExpectation(item)"
              />
            </base-button-group>
          </template>

        </base-table>

        <div v-if="!expectations.length" class="p-3 text-muted small text-center">
          No expectations configured — quality checks will be skipped at commit.
        </div>

      </base-panel>

      <!-- schema columns -->
      <base-panel v-if="schemaData" title="Columns" icon="fa-table" :no-padding="true">
        <base-table :columns="SCHEMA_COLUMNS" :items="schemaData.columns">

          <template #cell(nullable)="{ item }">
            <span v-if="item.nullable" class="text-muted">✓</span>
            <span v-else class="badge badge-secondary">NOT NULL</span>
          </template>

          <template #cell(pii)="{ item }">
            <span v-if="item.pii" class="badge badge-danger">
              {{ item.pii_type && item.pii_type !== 'none' ? item.pii_type : 'PII' }}
            </span>
            <span v-else class="text-muted">—</span>
          </template>

          <template #cell(status)="{ item }">
            <span :class="['badge', STATUS_BADGE[item.status] || 'badge-secondary']">
              {{ item.status }}
            </span>
          </template>

          <template #cell(actions)="{ item }">
            <base-button-group>
              <base-button
                icon="fas fa-pencil-alt"
                color="outline-primary"
                title="Edit"
                @click="openEdit(item)"
              />
              <base-button
                v-if="item.status !== 'published'"
                icon="fas fa-check"
                color="outline-success"
                title="Approve column"
                :disabled="saving"
                @click="askApproveColumn(item)"
              />
              <base-button
                icon="fas fa-trash"
                color="outline-danger"
                title="Delete column"
                :disabled="saving"
                @click="askDeleteColumn(item)"
              />
            </base-button-group>
          </template>

        </base-table>
      </base-panel>

      <!-- edit column modal -->
      <base-modal ref="modalRef" size="md" icon="fas fa-pencil-alt"
                  :title="'Edit — ' + editCol" :scrollable="true">

        <div v-if="formError" class="alert alert-danger mb-3">{{ formError }}</div>

        <div class="form-group">
          <label class="small text-muted">Logical type</label>
          <base-input v-model="form.logical_type" placeholder="e.g. date, email, currency…" />
        </div>

        <div class="form-group">
          <label class="small text-muted">Description</label>
          <textarea class="form-control form-control-sm" rows="3"
                    v-model="form.description" placeholder="Human-readable description"></textarea>
        </div>

        <div class="form-group form-check">
          <input type="checkbox" class="form-check-input" id="ck-nullable" v-model="form.nullable" />
          <label class="form-check-label small" for="ck-nullable">Nullable</label>
        </div>

        <div class="form-group form-check">
          <input type="checkbox" class="form-check-input" id="ck-pii" v-model="form.pii" />
          <label class="form-check-label small" for="ck-pii">PII flag</label>
        </div>

        <div v-if="form.pii" class="form-group">
          <label class="small text-muted">PII type</label>
          <select class="form-control form-control-sm" v-model="form.pii_type">
            <option v-for="t in PII_TYPES" :key="t" :value="t">{{ t }}</option>
          </select>
        </div>

        <div v-if="form.pii" class="form-group">
          <label class="small text-muted">PII notes</label>
          <base-input v-model="form.pii_notes" placeholder="e.g. masked in production" />
        </div>

        <template #footer>
          <base-button
            label="Save"
            icon="fas fa-save"
            color="primary"
            :disabled="saving"
            :loading="saving"
            @click="submitEdit"
          />
          <base-button
            label="Close"
            icon="fas fa-times"
            color="outline-secondary"
            class="ml-auto"
            @click="modalRef && modalRef.close()"
          />
        </template>
      </base-modal>

      <!-- add/edit expectation modal -->
      <base-modal ref="expModalRef" size="md" icon="fas fa-shield-alt"
                  :title="expEditId !== null ? 'Edit Expectation' : 'Add Expectation'"
                  :scrollable="true">

        <div v-if="expError" class="alert alert-danger mb-3">{{ expError }}</div>

        <div class="form-group">
          <label class="small text-muted">Rule</label>
          <select class="form-control form-control-sm" v-model="expForm.rule_id">
            <option value="" disabled>Select a rule…</option>
            <option v-for="r in availableRules" :key="r.id" :value="r.id">
              {{ r.id }}
            </option>
          </select>
          <small v-if="selectedRule" class="text-muted">{{ selectedRule.description }}</small>
        </div>

        <template v-if="selectedRule">
          <div v-if="Object.keys(selectedRule.inputs_schema).length" class="mb-3">
            <label class="small text-muted d-block mb-1">Inputs
              <span class="text-secondary">(format: dataset.column — use "this" for the current dataset)</span>
            </label>
            <div v-for="(desc, name) in selectedRule.inputs_schema" :key="name" class="form-group mb-2">
              <label class="small"><code>{{ name }}</code> <span class="text-muted">— {{ desc }}</span></label>
              <base-input v-model="expForm.inputs[name]" :placeholder="'e.g. this.' + name" />
            </div>
          </div>

          <div v-if="Object.keys(selectedRule.params_schema).length" class="mb-3">
            <label class="small text-muted d-block mb-1">Parameters</label>
            <div v-for="(desc, name) in selectedRule.params_schema" :key="name" class="form-group mb-2">
              <label class="small"><code>{{ name }}</code> <span class="text-muted">— {{ desc }}</span></label>
              <base-input v-model="expForm.params[name]" :placeholder="desc" />
            </div>
          </div>
        </template>

        <div class="form-row">
          <div class="form-group col-6">
            <label class="small text-muted">Tolerance (0–1)</label>
            <input type="number" class="form-control form-control-sm"
                   v-model="expForm.tolerance" min="0" max="1" step="0.01" />
          </div>
          <div class="form-group col-6">
            <label class="small text-muted">Position</label>
            <input type="number" class="form-control form-control-sm"
                   v-model="expForm.position" min="0" step="1" />
          </div>
        </div>

        <template #footer>
          <base-button
            label="Save"
            icon="fas fa-save"
            color="primary"
            :disabled="expSaving || !expForm.rule_id"
            :loading="expSaving"
            @click="submitExpectation"
          />
          <base-button
            label="Close"
            icon="fas fa-times"
            color="outline-secondary"
            class="ml-auto"
            @click="expModalRef && expModalRef.close()"
          />
        </template>
      </base-modal>

      <confirm-dialog ref="confirmPublishRef" />
      <confirm-dialog ref="confirmDeleteRef" />
      <confirm-dialog ref="confirmExpDeleteRef" />

    </base-page>
  `
};
