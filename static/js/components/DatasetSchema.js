import { api } from '../api.js';
import BasePage      from './BasePage.js';
import BasePanel     from './BasePanel.js';
import BaseButton    from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseTable     from './BaseTable.js';
import BaseModal     from './BaseModal.js';
import BaseInput     from './BaseInput.js';
import ConfirmDialog from './ConfirmDialog.js';

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

export default {
  name: 'DatasetSchema',
  components: { BasePage, BasePanel, BaseButton, BaseButtonGroup, BaseTable, BaseModal, BaseInput, ConfirmDialog },

  setup() {
    const route  = VueRouter.useRoute();
    const router = VueRouter.useRouter();

    const datasetId = Vue.computed(() => {
      const raw = route.params.id;
      const joined = Array.isArray(raw) ? raw.join('/') : String(raw || '');
      return joined.replace(/^\/|\/$/g, '');
    });

    const schemaData       = Vue.ref(null);
    const loading          = Vue.ref(false);
    const saving           = Vue.ref(false);
    const pageError        = Vue.ref(null);
    const formError        = Vue.ref(null);

    const modalRef          = Vue.ref(null);
    const confirmPublishRef = Vue.ref(null);
    const confirmDeleteRef  = Vue.ref(null);

    const editCol      = Vue.ref(null);
    const pendingDelete = Vue.ref(null);

    const form = Vue.ref({
      logical_type: '',
      description:  '',
      nullable:     true,
      pii:          false,
      pii_type:     'none',
      pii_notes:    '',
    });

    async function loadSchema() {
      if (!datasetId.value) return;
      loading.value   = true;
      pageError.value = null;
      try {
        const res = await api.catalogDatasetSchema(datasetId.value);
        schemaData.value = res.data || null;
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
        pii_type:     col.pii_type     || 'none',
        pii_notes:    col.pii_notes    || '',
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
          pii_notes:    form.value.pii_notes  || null,
        };
        const res = await api.catalogSchemaUpdateColumn(datasetId.value, editCol.value, body);
        if (res.diagnostic?.result === 'KO') {
          formError.value = res.diagnostic?.messages?.[0] || 'Error saving column';
          return;
        }
        modalRef.value?.close();
        await loadSchema();
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
      saving.value    = true;
      pageError.value = null;
      try {
        const res = await api.catalogSchemaApproveColumn(datasetId.value, columnName);
        if (res.diagnostic?.result === 'KO') {
          pageError.value = res.diagnostic?.messages?.[0] || 'Error approving column';
          return;
        }
        await loadSchema();
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
      saving.value    = true;
      pageError.value = null;
      try {
        const res = await api.catalogSchemaDeleteColumn(datasetId.value, columnName);
        if (res.diagnostic?.result === 'KO') {
          pageError.value = res.diagnostic?.messages?.[0] || 'Error deleting column';
          return;
        }
        await loadSchema();
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
      saving.value    = true;
      pageError.value = null;
      try {
        await api.catalogSchemaPublish(datasetId.value, { published_by: 'admin' });
        await loadSchema();
      } catch (e) {
        pageError.value = e.message;
      } finally {
        saving.value = false;
      }
    }

    Vue.onMounted(loadSchema);

    return {
      datasetId, schemaData, loading, saving, pageError, formError,
      SCHEMA_COLUMNS, STATUS_BADGE, PII_TYPES,
      modalRef, confirmPublishRef, confirmDeleteRef,
      editCol, pendingDelete, form,
      openEdit, submitEdit,
      askApproveColumn, askDeleteColumn, askPublishAll,
      goBack: () => router.go(-1),
    };
  },

  template: `
    <base-page
      title="Schema"
      :subtitle="datasetId"
      icon="fas fa-project-diagram"
      :loading="loading">

      <template #actions>
        <base-button
          label="Back"
          icon="fas fa-arrow-left"
          color="outline-secondary"
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

      <!-- summary -->
      <base-panel v-if="schemaData" title="Summary">
        <div class="d-flex flex-wrap p-3" style="gap:1.5rem;">
          <div class="text-center">
            <div class="display-4 font-weight-bold">{{ schemaData.summary.total }}</div>
            <small class="text-muted">Total</small>
          </div>
          <div class="text-center">
            <div class="display-4 font-weight-bold text-secondary">{{ schemaData.summary.inferred }}</div>
            <small class="text-muted">Inferred</small>
          </div>
          <div class="text-center">
            <div class="display-4 font-weight-bold text-warning">{{ schemaData.summary.draft }}</div>
            <small class="text-muted">Draft</small>
          </div>
          <div class="text-center">
            <div class="display-4 font-weight-bold text-success">{{ schemaData.summary.published }}</div>
            <small class="text-muted">Published</small>
          </div>
          <div class="text-center">
            <div class="display-4 font-weight-bold text-danger">{{ schemaData.summary.pii }}</div>
            <small class="text-muted">PII</small>
          </div>
        </div>
      </base-panel>

      <!-- schema table -->
      <base-panel v-if="schemaData" title="Columns" :no-padding="true">
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

      <!-- edit modal -->
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

        <div class="form-group" v-if="form.pii">
          <label class="small text-muted">PII type</label>
          <select class="form-control form-control-sm" v-model="form.pii_type">
            <option v-for="t in PII_TYPES" :key="t" :value="t">{{ t }}</option>
          </select>
        </div>

        <div class="form-group" v-if="form.pii">
          <label class="small text-muted">PII notes</label>
          <base-input v-model="form.pii_notes" placeholder="e.g. masked in production" />
        </div>

        <template #footer>
          <base-button
            label="Save"
            icon="fas fa-save"
            color="primary"
            :disabled="saving"
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
