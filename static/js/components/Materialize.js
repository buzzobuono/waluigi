import { api }       from '../api.js';
import BaseModal     from './BaseModal.js';
import BaseButton    from './BaseButton.js';
import BaseInput     from './BaseInput.js';

const { defineComponent, ref } = Vue;

export default defineComponent({
  name: 'Materialize',
  components: { BaseModal, BaseButton, BaseInput },
  emits: ['done'],

  setup(props, { emit }) {
    const modalRef  = ref(null);
    const loading   = ref(false);
    const result    = ref(null);
    const error     = ref('');

    const datasetId = ref('');
    const baseUrl   = ref('');
    const endpoint  = ref('');
    const params    = ref('');

    function open(folder = '') {
      datasetId.value = folder;
      baseUrl.value   = '';
      endpoint.value  = '';
      params.value    = '';
      result.value    = null;
      error.value     = '';
      modalRef.value?.open();
    }

    async function submit() {
      error.value  = '';
      result.value = null;

      if (!datasetId.value.trim()) {
        error.value = 'Dataset ID is required.';
        return;
      }
      if (!baseUrl.value.trim() || !endpoint.value.trim()) {
        error.value = 'Base URL and Endpoint are required.';
        return;
      }

      let parsedParams = {};
      if (params.value.trim()) {
        try {
          parsedParams = JSON.parse(params.value);
        } catch {
          error.value = 'Params must be valid JSON — e.g. {"status": "available"}';
          return;
        }
      }

      loading.value = true;
      try {
        const res = await api.catalogMaterialize(datasetId.value.trim(), {
          base_url: baseUrl.value.trim().replace(/\/+$/, ''),
          endpoint: endpoint.value.trim().startsWith('/')
            ? endpoint.value.trim()
            : '/' + endpoint.value.trim(),
          params: parsedParams,
        });
        result.value = res.data;
        emit('done', res.data);
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    function reset() {
      result.value = null;
      error.value  = '';
    }

    return {
      modalRef, loading, result, error,
      datasetId, baseUrl, endpoint, params,
      open, submit, reset,
    };
  },

  template: `
    <base-modal ref="modalRef" title="Materialize REST API" icon="fas fa-cloud-download-alt" size="lg">

      <div v-if="result" class="mb-3">
        <div v-if="result.skipped" class="alert alert-warning py-2">
          <i class="fas fa-equals mr-2"></i><strong>Skipped</strong>
          <div class="small mt-1 text-muted">
            Content identical to latest version — no new version created.<br>
            Existing version: <code>{{ result.version ? result.version.slice(0,19) : '' }}</code>
          </div>
        </div>
        <div v-else class="alert alert-success py-2">
          <i class="fas fa-check-circle mr-2"></i><strong>Materialized</strong>
          <div class="small mt-1">
            <div>Version: <code>{{ result.version ? result.version.slice(0,19) : '' }}</code></div>
            <div>Rows: <strong>{{ result.rows }}</strong></div>
            <div class="text-muted">Path: <code>{{ result.path }}</code></div>
          </div>
        </div>
      </div>

      <div v-if="error" class="alert alert-danger py-2 small">
        <i class="fas fa-exclamation-triangle mr-1"></i>{{ error }}
      </div>

      <form v-if="!result" @submit.prevent="submit">
        <div class="form-group">
          <label class="small font-weight-bold">Dataset ID <span class="text-danger">*</span></label>
          <base-input v-model="datasetId" placeholder="e.g. petstore/animals/available_pets" />
        </div>
        <div class="form-group">
          <label class="small font-weight-bold">Base URL <span class="text-danger">*</span></label>
          <base-input v-model="baseUrl" placeholder="e.g. https://api.example.com" />
        </div>
        <div class="form-group">
          <label class="small font-weight-bold">Endpoint <span class="text-danger">*</span></label>
          <base-input v-model="endpoint" placeholder="e.g. /posts or /users" />
        </div>
        <div class="form-group mb-0">
          <label class="small font-weight-bold">Query Params <span class="text-muted font-weight-normal">(JSON, optional)</span></label>
          <base-input v-model="params" placeholder='e.g. {"userId": 1}' />
        </div>
      </form>

      <template #footer>
        <template v-if="!result">
          <base-button label="Cancel"      color="outline-secondary" @click="modalRef?.close()" />
          <base-button label="Materialize" icon="fas fa-cloud-download-alt" color="primary"
                       :loading="loading" :disabled="loading" class="ml-2" @click="submit" />
        </template>
        <template v-else>
          <base-button label="Again" icon="fas fa-redo" color="outline-secondary" @click="reset" />
          <base-button label="Close" color="secondary" class="ml-2" @click="modalRef?.close()" />
        </template>
      </template>

    </base-modal>
  `,
});
