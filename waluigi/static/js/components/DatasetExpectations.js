import { api } from '../api.js';
import { nsStore } from '../store.js';
import BasePage        from './BasePage.js';
import BasePanel       from './BasePanel.js';
import BaseButton      from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseTable       from './BaseTable.js';
import BaseModal       from './BaseModal.js';
import BaseInput       from './BaseInput.js';
import ConfirmDialog   from './ConfirmDialog.js';

const { ref, computed, watch } = Vue;

const EXP_COLUMNS = [
  { key: 'rule_id',   label: 'Rule' },
  { key: 'inputs',    label: 'Inputs' },
  { key: 'params',    label: 'Params' },
  { key: 'tolerance', label: 'Tolerance', class: 'text-center' },
  { key: 'actions',   label: '',          class: 'text-right pr-3' },
];

export default {
  name: 'DatasetExpectations',
  components: {
    BasePage, BasePanel, BaseButton, BaseButtonGroup,
    BaseTable, BaseModal, BaseInput, ConfirmDialog,
  },

  setup() {
    const route  = VueRouter.useRoute();
    const router = VueRouter.useRouter();

    const datasetId = computed(() => {
      const raw = route.params.id;
      return (Array.isArray(raw) ? raw.join('/') : String(raw || '')).replace(/^\/|\/$/g, '');
    });

    const expectations   = ref([]);
    const availableRules = ref([]);
    const loading        = ref(false);
    const expSaving      = ref(false);
    const pageError      = ref(null);
    const expError       = ref(null);

    const expModalRef        = ref(null);
    const expEditId          = ref(null);
    const confirmExpDeleteRef = ref(null);
    const expForm = ref({ rule_id: '', inputs: {}, params: {}, tolerance: 1.0, position: 0 });

    const selectedRule = computed(() =>
      availableRules.value.find(r => r.id === expForm.value.rule_id) || null
    );

    watch(() => expForm.value.rule_id, (newId) => {
      const rule = availableRules.value.find(r => r.id === newId);
      if (!rule) return;
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

    async function load() {
      if (!datasetId.value || !nsStore.selected) return;
      loading.value   = true;
      pageError.value = null;
      try {
        const [expRes, rulesRes] = await Promise.all([
          api.datasetExpectations(nsStore.selected, datasetId.value),
          api.dqRules(),
        ]);
        expectations.value   = expRes.data   || [];
        availableRules.value = rulesRes.data  || [];
      } catch (e) {
        pageError.value = e.message;
      } finally {
        loading.value = false;
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
      expForm.value   = { rule_id: exp.rule_id, inputs: { ...exp.inputs }, params: { ...exp.params }, tolerance: exp.tolerance, position: exp.position };
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
          res = await api.updateExpectation(nsStore.selected, datasetId.value, expEditId.value, body);
        } else {
          res = await api.addExpectation(nsStore.selected, datasetId.value, body);
        }
        if (res.diagnostic?.result === 'KO') {
          expError.value = res.diagnostic?.messages?.[0] || 'Error saving expectation';
          return;
        }
        expModalRef.value?.close();
        await load();
      } catch (e) {
        expError.value = e.message;
      } finally {
        expSaving.value = false;
      }
    }

    function askDeleteExpectation(exp) {
      confirmExpDeleteRef.value?.ask(
        `Delete expectation "${exp.rule_id}"?`,
        async (ok) => { if (ok) { await api.deleteExpectation(nsStore.selected, datasetId.value, exp.id); await load(); } }
      );
    }

    watch(() => nsStore.selected, load, { immediate: true });

    return {
      datasetId, expectations, availableRules,
      loading, expSaving, pageError, expError,
      EXP_COLUMNS,
      expModalRef, expEditId, expForm, selectedRule, confirmExpDeleteRef,
      openAddExpectation, openEditExpectation, submitExpectation, askDeleteExpectation,
      goBack:    () => router.go(-1),
      goToRules: () => router.push('/dq/rules'),
      load,
    };
  },

  template: `
    <base-page
      title="DQ Expectations"
      :subtitle="datasetId"
      icon="fas fa-shield-alt"
      :loading="loading && !expectations.length"
      :error="pageError">

      <template #actions>
        <base-button icon="fas fa-arrow-left" label="Back"    color="outline-secondary" @click="goBack" />
        <base-button icon="fas fa-sync-alt"   label="Refresh" color="outline-primary"   class="ml-2" :loading="loading" @click="load" />
      </template>

      <base-panel :no-padding="true">
        <template #tools>
          <base-button icon="fas fa-external-link-alt" color="outline-secondary" title="Browse rules" @click="goToRules" />
          <base-button icon="fas fa-plus" label="Add" color="outline-primary" class="ml-1" @click="openAddExpectation" />
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
              <base-button icon="fas fa-pencil-alt" color="outline-primary" title="Edit"   @click="openEditExpectation(item)" />
              <base-button icon="fas fa-trash"      color="outline-danger"  title="Delete" @click="askDeleteExpectation(item)" />
            </base-button-group>
          </template>

        </base-table>

        <div v-if="!loading && !expectations.length" class="p-3 text-muted small text-center">
          No expectations configured — quality checks will be skipped at commit.
        </div>
      </base-panel>

      <!-- add/edit modal -->
      <base-modal ref="expModalRef" size="md" icon="fas fa-shield-alt"
                  :title="expEditId !== null ? 'Edit Expectation' : 'Add Expectation'"
                  :scrollable="true">

        <div v-if="expError" class="alert alert-danger mb-3">{{ expError }}</div>

        <div class="form-group">
          <label class="small text-muted">Rule</label>
          <select class="form-control form-control-sm" v-model="expForm.rule_id">
            <option value="" disabled>Select a rule…</option>
            <option v-for="r in availableRules" :key="r.id" :value="r.id">{{ r.id }}</option>
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
            <input type="number" class="form-control form-control-sm" v-model="expForm.tolerance" min="0" max="1" step="0.01" />
          </div>
          <div class="form-group col-6">
            <label class="small text-muted">Position</label>
            <input type="number" class="form-control form-control-sm" v-model="expForm.position" min="0" step="1" />
          </div>
        </div>

        <template #footer>
          <base-button label="Save" icon="fas fa-save" color="primary"
                       :disabled="expSaving || !expForm.rule_id" :loading="expSaving"
                       @click="submitExpectation" />
          <base-button label="Close" icon="fas fa-times" color="outline-secondary"
                       class="ml-auto" @click="expModalRef && expModalRef.close()" />
        </template>
      </base-modal>

      <confirm-dialog ref="confirmExpDeleteRef" />
    </base-page>
  `,
};
