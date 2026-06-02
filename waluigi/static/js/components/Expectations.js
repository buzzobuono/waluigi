import { api }    from '../api.js';
import BasePage   from './BasePage.js';
import BasePanel  from './BasePanel.js';
import BaseButton from './BaseButton.js';
import BaseTable  from './BaseTable.js';
import BaseModal  from './BaseModal.js';

const { ref, onMounted } = Vue;

const COLUMNS = [
  { key: 'id',            label: 'Rule ID' },
  { key: 'description',   label: 'Description' },
  { key: 'inputs_schema', label: 'Inputs' },
  { key: 'params_schema', label: 'Params' },
  { key: 'actions',       label: '', class: 'text-right pr-3' },
];

export default {
  name: 'Expectations',
  components: { BasePage, BasePanel, BaseButton, BaseTable, BaseModal },

  setup() {
    const rules     = ref([]);
    const loading   = ref(false);
    const pageError = ref(null);
    const selected  = ref(null);
    const modalRef  = ref(null);

    async function loadRules() {
      loading.value   = true;
      pageError.value = null;
      try {
        const res = await api.dqRules();
        rules.value = res.data || [];
      } catch (e) {
        pageError.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    function openDetail(rule) {
      selected.value = rule;
      modalRef.value?.open();
    }

    onMounted(loadRules);

    return { rules, loading, pageError, selected, modalRef, COLUMNS, loadRules, openDetail };
  },

  template: `
    <base-page
      title="Expectations"
      subtitle="Available rules catalogue"
      icon="fas fa-shield-alt"
      :loading="loading"
      :error="pageError">

      <template #actions>
        <base-button icon="fas fa-sync-alt" color="outline-primary" label="Refresh" class="ml-auto" @click="loadRules" />
      </template>

      <base-panel :no-padding="true">
        <base-table :columns="COLUMNS" :items="rules">

          <template #cell(id)="{ item }">
            <code class="small">{{ item.id }}</code>
          </template>

          <template #cell(inputs_schema)="{ item }">
            <span v-for="(desc, name) in item.inputs_schema" :key="name"
                  class="badge badge-secondary mr-1">{{ name }}</span>
          </template>

          <template #cell(params_schema)="{ item }">
            <span v-for="(desc, name) in item.params_schema" :key="name"
                  class="badge badge-info mr-1">{{ name }}</span>
            <span v-if="!Object.keys(item.params_schema).length" class="text-muted small">—</span>
          </template>

          <template #cell(actions)="{ item }">
            <base-button icon="fas fa-eye" color="outline-primary" title="View details"
                         @click="openDetail(item)" />
          </template>

        </base-table>
      </base-panel>

      <!-- detail modal -->
      <base-modal ref="modalRef" size="lg" icon="fas fa-shield-alt"
                  :title="selected ? selected.id : ''" :scrollable="true">
        <template v-if="selected">

          <div class="mb-3">
            <label class="small text-muted d-block mb-1">Description</label>
            <span>{{ selected.description || '—' }}</span>
          </div>

          <div class="mb-3">
            <label class="small text-muted d-block mb-1">Formula</label>
            <pre class="bg-light p-2 rounded small mb-0" style="white-space:pre-wrap;">{{ selected.formula }}</pre>
          </div>

          <div class="mb-3">
            <label class="small text-muted d-block mb-1">Inputs</label>
            <table class="table table-sm table-bordered mb-0">
              <tr v-for="(desc, name) in selected.inputs_schema" :key="name">
                <td class="w-25"><code>{{ name }}</code></td>
                <td class="text-muted">{{ desc }}</td>
              </tr>
            </table>
          </div>

          <div v-if="Object.keys(selected.params_schema).length" class="mb-3">
            <label class="small text-muted d-block mb-1">Parameters</label>
            <table class="table table-sm table-bordered mb-0">
              <tr v-for="(desc, name) in selected.params_schema" :key="name">
                <td class="w-25"><code>{{ name }}</code></td>
                <td class="text-muted">{{ desc }}</td>
              </tr>
            </table>
          </div>

        </template>

        <template #footer>
          <base-button label="Close" icon="fas fa-times" color="outline-secondary"
                       @click="modalRef && modalRef.close()" />
        </template>
      </base-modal>

    </base-page>
  `
};
