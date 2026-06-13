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
  { key: 'name',    label: 'Group' },
  { key: 'keys',    label: 'Keys' },
  { key: 'updated', label: 'Updated' },
  { key: 'actions', label: '', class: 'text-right pr-3' },
];

export default {
  name: 'Secrets',
  components: { BasePage, BasePanel, BaseTable, BaseButton, BaseButtonGroup, BaseModal, BaseInput, ConfirmDialog },

  setup() {
    const items     = ref([]);
    const loading   = ref(false);
    const saving    = ref(false);
    const pageError = ref(null);
    const formError = ref(null);
    const mode      = ref('create');
    const modalRef  = ref(null);
    const confirmRef = ref(null);

    const form = ref({ name: '', pairs: [{ key: '', value: '' }] });

    const modalTitle = computed(() =>
      mode.value === 'edit' ? `Edit — ${form.value.name}` : 'New Secret Group'
    );

    const hasNs = computed(() => !!nsStore.selected);

    function fmtDate(iso) {
      if (!iso) return '—';
      return iso.slice(0, 16).replace('T', ' ');
    }

    // ── load ──────────────────────────────────────────────────────────────────

    async function load() {
      if (!nsStore.selected) { items.value = []; return; }
      loading.value  = true;
      pageError.value = null;
      try {
        const names = await api.secrets(nsStore.selected);
        items.value = await Promise.all(
          names.map(n =>
            api.secretKeys(nsStore.selected, n)
              .catch(() => ({ name: n, keys: [], updatedate: null }))
          )
        );
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
      form.value      = { name: '', pairs: [{ key: '', value: '' }] };
      modalRef.value?.open();
    }

    // ── edit ──────────────────────────────────────────────────────────────────

    function openEdit(item) {
      mode.value      = 'edit';
      formError.value = null;
      form.value = {
        name:  item.name,
        pairs: (item.keys || []).length
          ? item.keys.map(k => ({ key: k, value: '' }))
          : [{ key: '', value: '' }],
      };
      modalRef.value?.open();
    }

    // ── pairs helpers ──────────────────────────────────────────────────────────

    function addPair()     { form.value.pairs.push({ key: '', value: '' }); }
    function removePair(i) { form.value.pairs.splice(i, 1); }

    // ── submit ────────────────────────────────────────────────────────────────

    async function submitForm() {
      formError.value = null;
      const name = form.value.name.trim();
      if (!name) { formError.value = 'Group name is required.'; return; }
      const pairs = form.value.pairs.filter(p => p.key.trim());
      if (!pairs.length) { formError.value = 'At least one key is required.'; return; }
      const data = Object.fromEntries(pairs.map(p => [p.key.trim(), p.value]));

      saving.value = true;
      try {
        await api.upsertSecret(nsStore.selected, name, data);
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
        `Delete secret group "<b>${item.name}</b>"?<br><small class="text-muted">All keys in this group will be removed.</small>`,
        async (confirmed) => {
          if (!confirmed) return;
          try {
            await api.deleteSecret(nsStore.selected, item.name);
            await load();
          } catch (e) {
            pageError.value = e.message;
          }
        }
      );
    }

    return {
      nsStore, items, loading, saving, pageError, formError,
      mode, modalTitle, form, columns: COLUMNS, hasNs,
      modalRef, confirmRef, fmtDate,
      load, openCreate, openEdit, addPair, removePair, submitForm, deleteItem,
    };
  },

  template: `
    <base-page
      title="Secrets"
      subtitle="Namespace-scoped secrets injected as WALUIGI_SECRET_* env vars"
      icon="fas fa-key"
      :loading="loading"
      :error="pageError"
    >
      <template #actions>
        <base-button
          icon="fas fa-plus"
          color="primary"
          label="New Group"
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
        <i class="fas fa-info-circle mr-2"></i>Select a namespace to view secrets.
      </div>

      <base-panel v-else :no-padding="true">
        <base-table :columns="columns" :items="items">

          <template #cell(name)="{ item }">
            <i class="fas fa-lock text-warning mr-1"></i>
            <code class="text-dark">{{ item.name }}</code>
          </template>

          <template #cell(keys)="{ item }">
            <span
              v-for="k in (item.keys || [])" :key="k"
              class="badge badge-secondary mr-1">
              {{ k }}
            </span>
            <span v-if="!(item.keys || []).length" class="text-muted small">—</span>
          </template>

          <template #cell(updated)="{ item }">
            <small class="text-muted">{{ fmtDate(item.updatedate) }}</small>
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
      <base-modal ref="modalRef" :title="modalTitle" icon="fas fa-key" size="lg">

        <!-- Group name (create only) -->
        <div v-if="mode === 'create'" class="form-group">
          <label class="font-weight-bold">
            Group name <span class="text-danger">*</span>
          </label>
          <base-input v-model="form.name" placeholder="e.g. openai, database, sftp" />
          <small class="text-muted">Logical name — groups related secrets together.</small>
        </div>

        <!-- Warning in edit mode -->
        <div v-if="mode === 'edit'" class="alert alert-warning py-2 small">
          <i class="fas fa-exclamation-triangle mr-1"></i>
          Re-enter all values. Blank values will be saved as empty strings and will overwrite existing ones.
        </div>

        <!-- Key / value pairs -->
        <div class="form-group mb-1">
          <label class="font-weight-bold">
            Keys &amp; Values <span class="text-danger">*</span>
          </label>
          <div v-for="(pair, i) in form.pairs" :key="i" class="d-flex mb-2">
            <base-input
              v-model="pair.key"
              placeholder="KEY_NAME"
              class="mr-2"
              style="flex:1;font-family:monospace"
            />
            <input
              v-model="pair.value"
              type="password"
              class="form-control form-control-sm mr-2"
              placeholder="value"
              style="flex:1.5;font-family:monospace"
            />
            <base-button
              icon="fas fa-times"
              color="outline-danger"
              size="sm"
              :disabled="form.pairs.length === 1"
              title="Remove"
              @click="removePair(i)"
            />
          </div>
          <base-button
            icon="fas fa-plus"
            label="Add key"
            size="sm"
            color="outline-secondary"
            @click="addPair"
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
