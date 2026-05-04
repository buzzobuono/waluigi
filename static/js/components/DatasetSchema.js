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

const { ref, computed, onMounted } = Vue;

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

const SUITE_COLUMNS = [
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

    const schemaData    = ref(null);
    const dataset       = ref(null);
    const suiteRules    = ref([]);
    const loading       = ref(false);
    const saving        = ref(false);
    const dqSaving      = ref(false);
    const pageError     = ref(null);
    const formError     = ref(null);
    const dqSuitePath   = ref('');

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

    async function loadSuiteRules(path) {
      suiteRules.value = [];
      if (!path) return;
      try {
        const res = await api.dqSuite(path);
        suiteRules.value = res.data || [];
      } catch { /* best-effort */ }
    }

    async function loadAll() {
      if (!datasetId.value) return;
      loading.value   = true;
      pageError.value = null;
      try {
        const [schemaRes, datasetRes] = await Promise.all([
          api.catalogDatasetSchema(datasetId.value),
          api.catalogDataset(datasetId.value),
        ]);
        schemaData.value  = schemaRes.data || null;
        dataset.value     = datasetRes.data || null;
        dqSuitePath.value = dataset.value?.dq_suite || '';
        await loadSuiteRules(dataset.value?.dq_suite);
      } catch (e) {
        pageError.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    async function saveDqSuite() {
      dqSaving.value  = true;
      pageError.value = null;
      try {
        const res = await api.catalogDatasetUpdate(datasetId.value, {
          dq_suite: dqSuitePath.value.trim() || null,
        });
        if (res.diagnostic?.result === 'KO') {
          pageError.value = res.diagnostic?.messages?.[0] || 'Error saving DQ suite';
          return;
        }
        dataset.value     = res.data;
        dqSuitePath.value = res.data?.dq_suite || '';
        await loadSuiteRules(res.data?.dq_suite);
      } catch (e) {
        pageError.value = e.message;
      } finally {
        dqSaving.value = false;
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

    onMounted(loadAll);

    return {
      datasetId, schemaData, dataset, suiteRules, loading, saving, dqSaving,
      pageError, formError, dqSuitePath,
      SCHEMA_COLUMNS, SUITE_COLUMNS, STATUS_BADGE, PII_TYPES,
      modalRef, confirmPublishRef, confirmDeleteRef,
      editCol, pendingDelete, form,
      openEdit, submitEdit,
      askApproveColumn, askDeleteColumn, askPublishAll,
      saveDqSuite,
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

      <!-- DQ suite config -->
      <base-panel title="Data Quality Suite" icon="fa-shield-alt">
        <div class="form-group mb-2">
          <label class="small text-muted">Suite path (YAML file on the server)</label>
          <div class="input-group input-group-sm">
            <base-input
              v-model="dqSuitePath"
              placeholder="/rules/suites/my_suite.yaml — leave empty to disable"
            />
            <div class="input-group-append">
              <base-button
                label="Save"
                icon="fas fa-save"
                color="primary"
                :disabled="dqSaving"
                :loading="dqSaving"
                @click="saveDqSuite"
              />
            </div>
          </div>
        </div>
        <div v-if="dataset && dataset.dq_suite">
          <span class="badge badge-success mr-2">
            <i class="fas fa-check-circle mr-1"></i>DQ active
          </span>
          <code class="small">{{ dataset.dq_suite }}</code>
        </div>
        <div v-else class="text-muted small">No DQ suite configured — quality checks will be skipped at commit.</div>
      </base-panel>

      <!-- suite rules -->
      <base-panel v-if="suiteRules.length" title="Suite Rules" icon="fa-list-ul" :no-padding="true">
        <base-table :columns="SUITE_COLUMNS" :items="suiteRules">

          <template #cell(rule_id)="{ item }">
            <code class="small">{{ item.rule_id }}</code>
            <div v-if="item.description" class="text-muted small">{{ item.description }}</div>
            <span v-if="!item.found" class="badge badge-danger mt-1">not found in catalogue</span>
          </template>

          <template #cell(inputs)="{ item }">
            <span v-for="(col, ph) in item.inputs" :key="ph" class="d-block small">
              <code>{{ ph }}</code>
              <span class="text-muted mx-1">→</span>
              <span class="text-muted">{{ col }}</span>
            </span>
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
            <base-button
              icon="fas fa-external-link-alt"
              color="outline-secondary"
              title="Browse all rules"
              @click="goToRules"
            />
          </template>

        </base-table>
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

      <confirm-dialog ref="confirmPublishRef" />
      <confirm-dialog ref="confirmDeleteRef" />

    </base-page>
  `
};
