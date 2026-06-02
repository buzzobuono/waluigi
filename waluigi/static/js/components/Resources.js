import { api } from '../api.js';
import { nsStore } from '../store.js';
import BasePage      from './BasePage.js';
import BasePanel     from './BasePanel.js';
import BaseButton    from './BaseButton.js';
import BaseModal     from './BaseModal.js';
import BaseInput     from './BaseInput.js';
import ConfirmDialog from './ConfirmDialog.js';

const { ref, watch } = Vue;

export default {
  name: 'Resources',
  components: { BasePage, BasePanel, BaseButton, BaseModal, BaseInput, ConfirmDialog },

  setup() {
    const resources = ref([]);
    const loading   = ref(false);
    const saving    = ref(false);
    const error     = ref(null);
    const formError = ref(null);
    const modalRef  = ref(null);
    const confirmRef = ref(null);

    const editMode  = ref('create');   // 'create' | 'edit'
    const form      = ref({ name: '', amount: '' });

    async function load() {
      if (!nsStore.selected) { resources.value = []; return; }
      loading.value = true;
      error.value   = null;
      try {
        resources.value = await api.resources(nsStore.selected);
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    function pct(r)   { return r.amount > 0 ? Math.round(r.usage / r.amount * 100) : 0; }
    function color(r) {
      const p = pct(r);
      return p > 80 ? 'danger' : p > 50 ? 'warning' : 'success';
    }

    function openCreate() {
      editMode.value  = 'create';
      formError.value = null;
      form.value      = { name: '', amount: '' };
      modalRef.value?.open();
    }

    function openEdit(r) {
      editMode.value  = 'edit';
      formError.value = null;
      form.value      = { name: r.name, amount: String(r.amount) };
      modalRef.value?.open();
    }

    async function submitForm() {
      formError.value = null;
      const name   = form.value.name.trim();
      const amount = parseFloat(form.value.amount);
      if (!name)           { formError.value = 'Name is required.';            return; }
      if (isNaN(amount) || amount <= 0) { formError.value = 'Amount must be a positive number.'; return; }

      saving.value = true;
      try {
        const spec = Object.fromEntries(resources.value.map(r => [r.name, r.amount]));
        spec[name] = amount;
        await api.applyResources(nsStore.selected, spec);
        modalRef.value?.close();
        await load();
      } catch (e) {
        formError.value = e.message;
      } finally {
        saving.value = false;
      }
    }

    function confirmDelete(r) {
      if (r.usage > 0) {
        error.value = `Cannot delete "${r.name}": ${r.usage} units currently in use.`;
        return;
      }
      confirmRef.value.ask(
        `Delete resource <b>${r.name}</b>?`,
        async (ok) => {
          if (!ok) return;
          try {
            const spec = Object.fromEntries(resources.value.map(x => [x.name, x.amount]));
            delete spec[r.name];
            await api.applyResources(nsStore.selected, spec);
            await load();
          } catch (e) {
            error.value = e.message;
          }
        }
      );
    }

    watch(() => nsStore.selected, load, { immediate: true });

    return {
      nsStore, resources, loading, saving, error, formError,
      editMode, form, modalRef, confirmRef,
      load, pct, color, openCreate, openEdit, submitForm, confirmDelete,
    };
  },

  template: `
    <base-page
      title="Resources"
      :subtitle="nsStore.selected ? 'Namespace: ' + nsStore.selected : ''"
      icon="fas fa-chart-bar"
      :loading="loading && !resources.length"
      :error="error">

      <template #actions>
        <base-button
          icon="fas fa-plus"
          label="Add Resource"
          color="primary"
          class="mr-2"
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

      <div v-if="!resources.length && !loading" class="text-muted mt-3 text-center">
        No resources configured — click <strong>Add Resource</strong> to define resource pools for this namespace.
      </div>

      <div v-else class="row">
        <div class="col-12 col-sm-6 col-md-4" v-for="r in resources" :key="r.name">
          <base-panel>
            <template #title>
              <h3 class="card-title">{{ r.name }}</h3>
            </template>

            <template #tools>
              <span :class="['badge', 'bg-'+color(r), 'ml-auto', 'mr-2']">{{ pct(r) }}%</span>
              <base-button icon="fas fa-pencil-alt" color="outline-secondary" size="sm" title="Edit" @click="openEdit(r)" />
              <base-button icon="fas fa-trash"      color="outline-danger"    size="sm" title="Delete" class="ml-1" @click="confirmDelete(r)" />
            </template>

            <div class="card-body p-0">
              <div class="d-flex justify-content-between mb-2">
                <span>Usage: <b>{{ r.usage }}</b> / {{ r.amount }}</span>
                <span>Available: <b>{{ r.amount - r.usage }}</b></span>
              </div>
              <div class="progress shadow">
                <div
                  class="progress-bar"
                  :class="'bg-'+color(r)"
                  :style="'width:'+pct(r)+'%'"
                ></div>
              </div>
            </div>
          </base-panel>
        </div>
      </div>

      <!-- Add / Edit modal -->
      <base-modal ref="modalRef"
        :title="editMode === 'edit' ? 'Edit Resource' : 'Add Resource'"
        icon="fas fa-chart-bar"
        size="sm">

        <div class="form-group">
          <label class="font-weight-bold">Name</label>
          <base-input
            v-if="editMode === 'create'"
            v-model="form.name"
            placeholder="e.g. coin, gpu, slot"
          />
          <div v-else>
            <code>{{ form.name }}</code>
            <small class="text-muted d-block">Name cannot be changed.</small>
          </div>
        </div>

        <div class="form-group mb-0">
          <label class="font-weight-bold">Total amount</label>
          <base-input
            v-model="form.amount"
            placeholder="e.g. 10"
            type="number"
            min="1"
          />
          <small class="text-muted">Total units available in this namespace's resource pool.</small>
        </div>

        <div v-if="formError" class="alert alert-danger mt-3 mb-0 py-2 small">
          <i class="fas fa-exclamation-circle mr-1"></i> {{ formError }}
        </div>

        <template #footer>
          <base-button label="Cancel" icon="fas fa-times" color="outline-secondary"
                       @click="modalRef.close()" />
          <base-button
            :label="editMode === 'edit' ? 'Save' : 'Add'"
            :icon="editMode === 'edit' ? 'fas fa-save' : 'fas fa-plus'"
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
