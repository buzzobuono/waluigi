import { api, getToken } from '../api.js';
import BasePage       from './BasePage.js';
import BasePanel      from './BasePanel.js';
import BaseTable      from './BaseTable.js';
import BaseButton     from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseModal      from './BaseModal.js';
import BaseInput      from './BaseInput.js';
import ConfirmDialog  from './ConfirmDialog.js';

const { ref, onMounted } = Vue;

function decodeToken(token) {
  try { return JSON.parse(atob(token.split('.')[1])); } catch { return null; }
}

const COLUMNS = [
  { key: 'userid',     label: 'User ID' },
  { key: 'username',   label: 'Display Name' },
  { key: 'namespaces', label: 'Namespaces' },
  { key: 'createdate', label: 'Created' },
  { key: 'actions',    label: '', class: 'text-right pr-3' },
];

export default {
  name: 'AdminUsers',
  components: { BasePage, BasePanel, BaseTable, BaseButton, BaseButtonGroup, BaseModal, BaseInput, ConfirmDialog },

  setup() {
    const payload   = decodeToken(getToken());
    const isAdmin   = payload?.namespaces === "*";

    const users      = ref([]);
    const loading    = ref(false);
    const saving     = ref(false);
    const pageError  = ref(null);
    const formError  = ref(null);
    const editError  = ref(null);
    const modalRef   = ref(null);
    const editRef    = ref(null);
    const confirmRef = ref(null);

    const form     = ref({ userid: '', username: '', password: '', namespaces: '' });
    const editForm = ref({ userid: '', namespaces: '' });

    function fmtDate(d) {
      return d ? d.slice(0, 19).replace('T', ' ') : '—';
    }

    function parseNs(raw) {
      return raw.split(/[\n,]+/).map(s => s.trim()).filter(Boolean);
    }

    async function loadUsers() {
      if (!isAdmin) return;
      loading.value   = true;
      pageError.value = null;
      try {
        const res   = await api.adminUsers();
        users.value = res.data || [];
      } catch (e) {
        pageError.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    function openCreate() {
      form.value      = { userid: '', username: '', password: '', namespaces: '' };
      formError.value = null;
      modalRef.value?.open();
    }

    async function submitCreate() {
      formError.value = null;
      if (!form.value.userid.trim() || !form.value.password.trim()) {
        formError.value = 'User ID and password are required.';
        return;
      }
      saving.value = true;
      try {
        await api.adminCreateUser({
          userid:     form.value.userid.trim(),
          username:   form.value.username.trim() || form.value.userid.trim(),
          password:   form.value.password,
          namespaces: parseNs(form.value.namespaces),
        });
        modalRef.value?.close();
        await loadUsers();
      } catch (e) {
        formError.value = e.message;
      } finally {
        saving.value = false;
      }
    }

    function openEdit(u) {
      const ns = Array.isArray(u.namespaces) ? u.namespaces.join('\n') : '';
      editForm.value  = { userid: u.userid, namespaces: ns };
      editError.value = null;
      editRef.value?.open();
    }

    async function submitEdit() {
      editError.value = null;
      saving.value    = true;
      try {
        await api.adminUpdateUser(editForm.value.userid, {
          namespaces: parseNs(editForm.value.namespaces),
        });
        editRef.value?.close();
        await loadUsers();
      } catch (e) {
        editError.value = e.message;
      } finally {
        saving.value = false;
      }
    }

    function deleteUser(u) {
      confirmRef.value.ask(
        `Delete user <b>${u.userid}</b>?`,
        async (confirmed) => {
          if (!confirmed) return;
          try {
            await api.adminDeleteUser(u.userid);
            await loadUsers();
          } catch (e) {
            pageError.value = e.message;
          }
        }
      );
    }

    onMounted(loadUsers);

    return {
      isAdmin, users, loading, saving, pageError, formError, editError,
      form, editForm, modalRef, editRef, confirmRef, columns: COLUMNS,
      fmtDate, loadUsers, openCreate, submitCreate, openEdit, submitEdit, deleteUser,
    };
  },

  template: `
    <base-page title="Users" subtitle="Console user management" icon="fas fa-users">

      <template #actions>
        <template v-if="isAdmin">
          <base-button icon="fas fa-plus" color="primary" label="New User"
                       class="mr-2" :disabled="loading" @click="openCreate" />
          <base-button icon="fas fa-sync-alt" color="outline-primary" label="Refresh"
                       :loading="loading" @click="loadUsers" />
        </template>
      </template>

      <div v-if="!isAdmin" class="alert alert-danger">
        <i class="fas fa-lock mr-2"></i>Access restricted to administrators.
      </div>

      <template v-if="isAdmin">
        <base-panel :no-padding="true">
          <base-table :columns="columns" :items="users">

            <template #cell(userid)="{ item }">
              <code>{{ item.userid }}</code>
            </template>

            <template #cell(namespaces)="{ item }">
              <span v-if="!item.namespaces || !item.namespaces.length"
                    class="text-muted small">—</span>
              <span v-for="ns in item.namespaces" :key="ns"
                    class="badge badge-info mr-1">{{ ns }}</span>
            </template>

            <template #cell(createdate)="{ item }">
              <span class="text-muted small">{{ fmtDate(item.createdate) }}</span>
            </template>

            <template #cell(actions)="{ item }">
              <base-button-group>
                <base-button icon="fas fa-layer-group" color="outline-info"
                             title="Edit namespaces" @click="openEdit(item)" />
                <base-button icon="fas fa-trash" color="outline-danger"
                             title="Delete user" @click="deleteUser(item)" />
              </base-button-group>
            </template>

          </base-table>
        </base-panel>

        <!-- Create user modal -->
        <base-modal ref="modalRef" title="New User" icon="fas fa-user-plus">
          <div class="form-group">
            <label class="small font-weight-bold">User ID <span class="text-danger">*</span></label>
            <base-input v-model="form.userid" placeholder="e.g. john.doe" />
            <small class="text-muted">Used for login.</small>
          </div>
          <div class="form-group">
            <label class="small font-weight-bold">Display Name</label>
            <base-input v-model="form.username" placeholder="e.g. John Doe (defaults to User ID)" />
          </div>
          <div class="form-group">
            <label class="small font-weight-bold">Password <span class="text-danger">*</span></label>
            <input v-model="form.password" type="password"
                   class="form-control form-control-sm" placeholder="Password" />
          </div>
          <div class="form-group mb-0">
            <label class="small font-weight-bold">Namespaces</label>
            <textarea v-model="form.namespaces" rows="3"
                      class="form-control form-control-sm font-monospace"
                      placeholder="One namespace per line (e.g. analytics)" />
            <small class="text-muted">Leave blank for no access. One per line.</small>
          </div>
          <div v-if="formError" class="alert alert-danger mt-3 mb-0 py-2 small">
            <i class="fas fa-exclamation-circle mr-1"></i>{{ formError }}
          </div>
          <template #footer>
            <base-button label="Cancel" color="outline-secondary" @click="modalRef?.close()" />
            <base-button label="Create" icon="fas fa-check" color="primary"
                         :loading="saving" class="ml-2" @click="submitCreate" />
          </template>
        </base-modal>

        <!-- Edit namespaces modal -->
        <base-modal ref="editRef" title="Edit Namespaces" icon="fas fa-layer-group">
          <p class="mb-2 text-muted small">
            Namespaces for <code>{{ editForm.userid }}</code>
          </p>
          <textarea v-model="editForm.namespaces" rows="5"
                    class="form-control form-control-sm font-monospace"
                    placeholder="One namespace per line" />
          <small class="text-muted">Leave blank to revoke all namespace access.</small>
          <div v-if="editError" class="alert alert-danger mt-3 mb-0 py-2 small">
            <i class="fas fa-exclamation-circle mr-1"></i>{{ editError }}
          </div>
          <template #footer>
            <base-button label="Cancel" color="outline-secondary" @click="editRef?.close()" />
            <base-button label="Save" icon="fas fa-check" color="primary"
                         :loading="saving" class="ml-2" @click="submitEdit" />
          </template>
        </base-modal>

        <confirm-dialog title="Confirm" ref="confirmRef" />
      </template>

    </base-page>
  `,
};
