import { api }      from '../api.js';
import { nsStore }  from '../store.js';
import BasePage     from './BasePage.js';
import BasePanel    from './BasePanel.js';
import BaseButton   from './BaseButton.js';
import BaseModal    from './BaseModal.js';
import BaseInput    from './BaseInput.js';
import ConfirmDialog from './ConfirmDialog.js';

const { ref, watch } = Vue;

export default {
  name: 'Secrets',
  components: { BasePage, BasePanel, BaseButton, BaseModal, BaseInput, ConfirmDialog },

  setup() {
    const secrets     = ref([]);   // [{name, keys, createdate, updatedate}]
    const loading     = ref(false);
    const saving      = ref(false);
    const error       = ref(null);
    const formError   = ref(null);
    const modalRef    = ref(null);
    const confirmRef  = ref(null);
    const editName    = ref(null);  // null = create mode, string = edit mode

    // form state: group name + list of {key, value} rows
    const form = ref({ name: '', pairs: [{ key: '', value: '' }] });

    async function load() {
      if (!nsStore.selected) { secrets.value = []; return; }
      loading.value = true;
      error.value   = null;
      try {
        const names = await api.secrets(nsStore.selected);
        const details = await Promise.all(
          names.map(n => api.secretKeys(nsStore.selected, n).catch(() => ({ name: n, keys: [] })))
        );
        secrets.value = details;
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    function openCreate() {
      editName.value  = null;
      formError.value = null;
      form.value      = { name: '', pairs: [{ key: '', value: '' }] };
      modalRef.value?.open();
    }

    function openEdit(s) {
      editName.value  = s.name;
      formError.value = null;
      // We can only show keys (no values) — start with blank values for the user to fill
      form.value = {
        name:  s.name,
        pairs: s.keys.length
          ? s.keys.map(k => ({ key: k, value: '' }))
          : [{ key: '', value: '' }],
      };
      modalRef.value?.open();
    }

    function addPair()  { form.value.pairs.push({ key: '', value: '' }); }
    function removePair(i) { form.value.pairs.splice(i, 1); }

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

    function confirmDelete(s) {
      confirmRef.value.ask(
        `Delete secret group <b>${s.name}</b>?<br><small class="text-muted">All keys in this group will be removed.</small>`,
        async (ok) => {
          if (!ok) return;
          try {
            await api.deleteSecret(nsStore.selected, s.name);
            await load();
          } catch (e) {
            error.value = e.message;
          }
        }
      );
    }

    watch(() => nsStore.selected, load, { immediate: true });

    return {
      nsStore, secrets, loading, saving, error, formError,
      form, editName, modalRef, confirmRef,
      load, openCreate, openEdit, addPair, removePair, submitForm, confirmDelete,
    };
  },

  template: `
    <base-page
      title="Secrets"
      :subtitle="nsStore.selected ? 'Namespace: ' + nsStore.selected : ''"
      icon="fas fa-key"
      :loading="loading && !secrets.length"
      :error="error">

      <template #actions>
        <base-button icon="fas fa-plus" label="New Secret Group" color="primary" @click="openCreate" />
        <base-button icon="fas fa-sync-alt" color="outline-primary" label="Refresh"
                     :loading="loading" @click="load" />
      </template>

      <div v-if="!secrets.length && !loading" class="text-muted mt-3 text-center">
        No secrets configured — click <strong>New Secret Group</strong>.
      </div>

      <div v-else class="row">
        <div class="col-12 col-md-6 col-xl-4" v-for="s in secrets" :key="s.name">
          <base-panel>
            <template #title>
              <i class="fas fa-lock text-warning mr-2"></i>
              <span class="font-weight-bold">{{ s.name }}</span>
            </template>
            <template #tools>
              <base-button icon="fas fa-pencil-alt" size="sm" title="Edit" @click="openEdit(s)" />
              <base-button icon="fas fa-trash" size="sm" title="Delete" @click="confirmDelete(s)" />
            </template>
            <div class="card-body pt-0 pb-2">
              <div v-if="s.keys && s.keys.length" class="mt-1">
                <span
                  v-for="k in s.keys" :key="k"
                  class="badge badge-secondary mr-1 mb-1"
                  style="font-size:0.78rem; letter-spacing:0.03em">
                  {{ k }}
                </span>
              </div>
              <div v-else class="text-muted small">No keys</div>
              <div class="text-muted small mt-2">
                <i class="fas fa-clock mr-1"></i>Updated {{ s.updatedate ? s.updatedate.slice(0,16).replace('T',' ') : '—' }}
              </div>
            </div>
          </base-panel>
        </div>
      </div>

      <!-- Create / Edit modal -->
      <base-modal ref="modalRef"
                  :title="editName ? 'Edit Secret Group: ' + editName : 'New Secret Group'">
        <div class="form-group" v-if="!editName">
          <label class="font-weight-bold">Group name <span class="text-danger">*</span></label>
          <base-input v-model="form.name" placeholder="e.g. openai, database, sftp" />
          <small class="text-muted">Logical name — groups related secrets together.</small>
        </div>

        <div class="form-group mb-1">
          <label class="font-weight-bold">Keys &amp; Values</label>
          <div v-if="editName" class="alert alert-info py-2 mb-2" style="font-size:0.85rem">
            <i class="fas fa-info-circle mr-1"></i>
            Leave a value blank to keep the existing one.
          </div>
          <div v-for="(pair, i) in form.pairs" :key="i"
               class="d-flex align-items-center mb-2 gap-2">
            <base-input v-model="pair.key"   placeholder="KEY_NAME"
                        style="flex:1; font-family:monospace" />
            <input v-model="pair.value" type="password"
                   placeholder="value"
                   class="form-control"
                   style="flex:1.5; font-family:monospace" />
            <button class="btn btn-sm btn-outline-danger" @click="removePair(i)"
                    :disabled="form.pairs.length === 1" title="Remove row">
              <i class="fas fa-times"></i>
            </button>
          </div>
          <base-button icon="fas fa-plus" label="Add key" size="sm"
                       color="outline-secondary" @click="addPair" />
        </div>

        <div v-if="formError" class="alert alert-danger mt-3 mb-0 py-2">
          <i class="fas fa-exclamation-circle mr-1"></i> {{ formError }}
        </div>

        <template #footer>
          <base-button label="Cancel" @click="modalRef.close()" />
          <base-button :label="editName ? 'Save' : 'Create'"
                       color="primary" :loading="saving" @click="submitForm" />
        </template>
      </base-modal>

      <confirm-dialog title="Confirm delete" ref="confirmRef" />
    </base-page>
  `
};
