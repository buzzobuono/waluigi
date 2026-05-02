import BasePage       from './BasePage.js';
import BasePanel      from './BasePanel.js';
import BaseTable      from './BaseTable.js';
import BaseButton     from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseModal      from './BaseModal.js';
import BaseInput      from './BaseInput.js';
import ConfirmDialog  from './ConfirmDialog.js';

const { ref, computed, onMounted } = Vue;

const SOURCE_TYPES = ['local', 's3', 'sql', 'sftp', 'api'];

const TYPE_COLORS = {
  local: 'success', s3: 'warning', sql: 'primary', sftp: 'info', api: 'secondary',
};

const CONFIG_FIELDS = {
  local: [
    { key: 'data_path', label: 'Data Path', placeholder: '/path/to/data' },
  ],
  s3: [
    { key: 'bucket',       label: 'Bucket',               placeholder: 'my-bucket' },
    { key: 'endpoint_url', label: 'Endpoint URL (MinIO)',  placeholder: 'http://minio:9000' },
    { key: 'region',       label: 'Region',               placeholder: 'us-east-1' },
  ],
  sql: [
    { key: 'url', label: 'url', placeholder: 'postgresql://user:pass@host/db' },
  ],
  sftp: [
    { key: 'host',     label: 'Host',     placeholder: 'sftp.example.com' },
    { key: 'port',     label: 'Port',     placeholder: '22' },
    { key: 'username', label: 'Username', placeholder: 'user' },
    { key: 'password', label: 'Password', placeholder: '(leave blank to use key)' },
    { key: 'key_path', label: 'Key Path', placeholder: '/home/user/.ssh/id_rsa' },
  ],
  api: [
    { key: 'base_url', label: 'Base URL', placeholder: 'https://api.example.com' },
  ],
};

const COLUMNS = [
  { key: 'id',          label: 'ID' },
  { key: 'type',        label: 'Type' },
  { key: 'description', label: 'Description' },
  { key: 'actions',     label: '', class: 'text-right pr-3' },
];

function buildConfig(rawConfig, type) {
  const config = Object.fromEntries(
    Object.entries(rawConfig).filter(([, v]) => v && String(v).trim() !== '')
  );
  if (type === 'sftp' && config.port) config.port = parseInt(config.port, 10) || 22;
  return config;
}

export default {
  name: 'Sources',
  components: { BasePage, BasePanel, BaseTable, BaseButton, BaseButtonGroup, BaseModal, BaseInput, ConfirmDialog },

  setup() {
    const sources     = ref([]);
    const loading     = ref(false);
    const saving      = ref(false);
    const pageError   = ref(null);
    const formError   = ref(null);

    // 'create' | 'edit'
    const mode        = ref('create');

    const modalRef    = ref(null);
    const confirmRef  = ref(null);

    const form = ref({ id: '', type: 'local', description: '', config: {} });

    const configFields = computed(() => CONFIG_FIELDS[form.value.type] || []);

    const modalTitle = computed(() =>
      mode.value === 'edit' ? `Edit Source — ${form.value.id}` : 'New Source'
    );

    // ── load ──────────────────────────────────────────────────────────────────

    async function loadSources() {
      loading.value   = true;
      pageError.value = null;
      try {
        const r    = await fetch('/catalog/sources');
        const json = await r.json();
        if (!r.ok) throw new Error(json.diagnostic?.messages?.[0] || `HTTP ${r.status}`);
        sources.value = json.data || [];
      } catch (e) {
        pageError.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    // ── create ────────────────────────────────────────────────────────────────

    function openCreate() {
      mode.value      = 'create';
      form.value      = { id: '', type: 'local', description: '', config: {} };
      formError.value = null;
      modalRef.value?.open();
    }

    function onTypeChange() {
      form.value.config = {};
    }

    async function submitCreate() {
      formError.value = null;
      const id = form.value.id.trim();
      if (!id) { formError.value = 'ID is required.'; return; }

      saving.value = true;
      try {
        const r = await fetch('/catalog/sources', {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({
            id,
            type:        form.value.type,
            description: form.value.description || '',
            config:      buildConfig(form.value.config, form.value.type),
          }),
        });
        const json = await r.json();
        if (json.diagnostic?.result === 'KO') {
          formError.value = json.diagnostic?.messages?.[0] || `Error ${r.status}`;
          return;
        }
        modalRef.value?.close();
        await loadSources();
      } catch (e) {
        formError.value = e.message;
      } finally {
        saving.value = false;
      }
    }

    // ── edit ──────────────────────────────────────────────────────────────────

    function openEdit(src) {
      mode.value = 'edit';
      form.value = {
        id:          src.id,
        type:        src.type,
        description: src.description || '',
        config:      src.config ? { ...src.config } : {},
      };
      if (src.type === 'sftp' && form.value.config.port !== undefined) {
        form.value.config.port = String(form.value.config.port);
      }
      formError.value = null;
      modalRef.value?.open();
    }

    async function submitEdit() {
      formError.value = null;
      saving.value    = true;
      try {
        const r = await fetch(`/catalog/sources/${encodeURIComponent(form.value.id)}`, {
          method:  'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({
            description: form.value.description || '',
            config:      buildConfig(form.value.config, form.value.type),
          }),
        });
        const json = await r.json();
        if (json.diagnostic?.result === 'KO') {
          formError.value = json.diagnostic?.messages?.[0] || `Error ${r.status}`;
          return;
        }
        modalRef.value?.close();
        await loadSources();
      } catch (e) {
        formError.value = e.message;
      } finally {
        saving.value = false;
      }
    }

    function submitForm() {
      mode.value === 'edit' ? submitEdit() : submitCreate();
    }

    // ── delete ────────────────────────────────────────────────────────────────

    function deleteSource(src) {
      confirmRef.value.ask(
        `Delete source "<b>${src.id}</b>"?<br><small class="text-muted">Datasets linked to this source may become unreadable.</small>`,
        async (ok) => {
          if (!ok) return;
          try {
            await fetch(`/catalog/sources/${encodeURIComponent(src.id)}`, { method: 'DELETE' });
          } catch { /* best-effort */ }
          await loadSources();
        }
      );
    }

    onMounted(loadSources);

    return {
      sources, loading, saving, pageError, formError,
      mode, modalTitle,
      modalRef, confirmRef,
      form, configFields, columns: COLUMNS,
      SOURCE_TYPES, TYPE_COLORS,
      loadSources, openCreate, openEdit, onTypeChange, submitForm, deleteSource,
    };
  },

  template: `
    <base-page
      title="Sources"
      subtitle="Storage and connection sources"
      icon="fas fa-plug"
      :loading="loading"
      :error="pageError"
    >
      <template #actions>
        <base-button
          icon="fas fa-plus"
          color="primary"
          label="New Source"
          class="mr-2"
          :disabled="loading"
          @click="openCreate"
        />
        <base-button
          icon="fas fa-sync-alt"
          color="outline-primary"
          label="Refresh"
          :loading="loading"
          @click="loadSources"
        />
      </template>

      <base-panel :no-padding="true">
        <base-table :columns="columns" :items="sources">

          <template #cell(id)="{ item }">
            <code class="text-dark">{{ item.id }}</code>
          </template>

          <template #cell(type)="{ item }">
            <span :class="['badge', 'badge-' + (TYPE_COLORS[item.type] || 'secondary')]">
              {{ item.type }}
            </span>
          </template>

          <template #cell(description)="{ item }">
            <span class="text-muted">{{ item.description || '—' }}</span>
          </template>

          <template #cell(actions)="{ item }">
            <base-button-group>
              <base-button
                icon="fas fa-pencil-alt"
                color="outline-secondary"
                title="Edit source"
                @click="openEdit(item)"
              />
              <base-button
                icon="fas fa-trash"
                color="outline-danger"
                title="Delete source"
                @click="deleteSource(item)"
              />
            </base-button-group>
          </template>

        </base-table>
      </base-panel>

      <!-- ── Create / Edit modal ────────────────────────────────────────────── -->
      <base-modal ref="modalRef" :title="modalTitle" icon="fa-plug" size="lg">

        <div class="form-group">
          <label class="font-weight-bold">
            ID <span v-if="mode === 'create'" class="text-danger">*</span>
          </label>
          <base-input
            v-if="mode === 'create'"
            v-model="form.id"
            placeholder="e.g. pg-dwh, s3-data-lake, local-data"
            @keyup.enter="submitForm"
          />
          <div v-else class="d-flex align-items-center">
            <code class="mr-2">{{ form.id }}</code>
            <span :class="['badge', 'badge-' + (TYPE_COLORS[form.type] || 'secondary')]">
              {{ form.type }}
            </span>
          </div>
          <small v-if="mode === 'create'" class="text-muted">
            Unique identifier used to reference this source in datasets.
          </small>
          <small v-else class="text-muted">
            ID and type cannot be changed after creation.
          </small>
        </div>

        <div v-if="mode === 'create'" class="form-group">
          <label class="font-weight-bold">
            Type <span class="text-danger">*</span>
          </label>
          <select class="form-control form-control-sm" v-model="form.type" @change="onTypeChange">
            <option v-for="t in SOURCE_TYPES" :key="t" :value="t">{{ t }}</option>
          </select>
        </div>

        <div class="form-group">
          <label class="font-weight-bold">Description</label>
          <base-input
            v-model="form.description"
            placeholder="Optional description"
            @keyup.enter="submitForm"
          />
        </div>

        <template v-if="configFields.length">
          <hr class="my-3" />
          <p class="font-weight-bold mb-2">
            <i class="fas fa-cog mr-1 text-muted"></i> Configuration
          </p>
          <div v-for="field in configFields" :key="field.key" class="form-group mb-2">
            <label class="small text-muted mb-1">{{ field.label }}</label>
            <base-input
              v-model="form.config[field.key]"
              :placeholder="field.placeholder"
              @keyup.enter="submitForm"
            />
          </div>
        </template>

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
