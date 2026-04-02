// components/Materialize.js
// Used inside Catalog.js as a modal triggered by a "Materialize" button

const { defineComponent, ref } = Vue;

export default defineComponent({
  name: 'Materialize',
  emits: ['done'],

  setup(props, { emit }) {
    const visible   = ref(false);
    const loading   = ref(false);
    const result    = ref(null);
    const error     = ref('');

    // form fields
    const namespace = ref('');
    const id        = ref('');
    const baseUrl   = ref('');
    const endpoint  = ref('');
    const params    = ref('');   // JSON string

    function open(ns = '', dsId = '') {
      namespace.value = ns;
      id.value        = dsId;
      baseUrl.value   = '';
      endpoint.value  = '';
      params.value    = '';
      result.value    = null;
      error.value     = '';
      visible.value   = true;
    }

    function close() {
      visible.value = false;
    }

    async function submit() {
      error.value  = '';
      result.value = null;

      if (!namespace.value.trim() || !id.value.trim()) {
        error.value = 'Namespace and Dataset ID are required.';
        return;
      }
      if (!baseUrl.value.trim() || !endpoint.value.trim()) {
        error.value = 'Base URL and endpoint are required.';
        return;
      }

      let parsedParams = {};
      if (params.value.trim()) {
        try {
          parsedParams = JSON.parse(params.value);
        } catch(e) {
          error.value = 'Params must be valid JSON — e.g. {"status": "available"}';
          return;
        }
      }

      loading.value = true;
      try {
        const r = await fetch(
          `/catalog/datasets/${encodeNs(namespace.value)}/${encodeURIComponent(id.value)}/materialize`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              base_url: baseUrl.value.trim().replace(/\/+$/, ''),
              endpoint: endpoint.value.trim().startsWith('/')
                ? endpoint.value.trim()
                : '/' + endpoint.value.trim(),
              params:   parsedParams,
            })
          }
        );
        const data = await r.json();
        if (!r.ok) {
          error.value = data.error || `Error ${r.status}`;
        } else {
          result.value = data;
          emit('done', data);
        }
      } catch(e) {
        error.value = `Network error: ${e.message}`;
      } finally {
        loading.value = false;
      }
    }

    function encodeNs(ns) {
      // keep slashes as path separators
      return ns;
    }

    return {
      visible, loading, result, error,
      namespace, id, baseUrl, endpoint, params,
      open, close, submit,
    };
  },

  template: `
    <div>
      <!-- Modal backdrop -->
      <div v-if="visible"
           style="position:fixed; inset:0; background:rgba(0,0,0,0.6); z-index:1050;"
           @click.self="close">

        <div style="position:fixed; top:50%; left:50%; transform:translate(-50%,-50%);
                    width:min(560px, 95vw); background:#1a0026;
                    border:1px solid #4b0082; border-radius:8px; z-index:1051;
                    box-shadow:0 8px 40px rgba(0,0,0,0.6);">

          <!-- Header -->
          <div style="padding:16px 20px; border-bottom:1px solid #4b0082;
                      display:flex; justify-content:space-between; align-items:center;">
            <h5 style="margin:0; color:#d080ff;">
              <i class="fas fa-cloud-download-alt mr-2"></i>Materialize REST API
            </h5>
            <button @click="close"
                    style="background:none; border:none; color:#e0e0e0; font-size:1.2em; cursor:pointer;">
              &times;
            </button>
          </div>

          <!-- Body -->
          <div style="padding:20px;">

            <!-- Result -->
            <div v-if="result" style="margin-bottom:16px;">
              <div v-if="result.skipped"
                   style="background:#2b0040; border:1px solid #ffc107; border-radius:6px; padding:12px;">
                <i class="fas fa-equals mr-2" style="color:#ffc107;"></i>
                <span style="color:#ffc107; font-weight:bold;">Skipped</span>
                <div style="font-size:0.85em; color:#aaa; margin-top:6px;">
                  Content identical to latest version — no new version created.<br>
                  Existing version: <code style="color:#00d4ff;">{{ result.version ? result.version.slice(0,19) : '' }}</code>
                </div>
              </div>
              <div v-else
                   style="background:#2b0040; border:1px solid #28a745; border-radius:6px; padding:12px;">
                <i class="fas fa-check-circle mr-2" style="color:#28a745;"></i>
                <span style="color:#28a745; font-weight:bold;">Materialized</span>
                <div style="font-size:0.85em; color:#aaa; margin-top:6px;">
                  <div>Version: <code style="color:#00d4ff;">{{ result.version ? result.version.slice(0,19) : '' }}</code></div>
                  <div>Rows: <b style="color:#e0e0e0;">{{ result.rows }}</b></div>
                  <div style="word-break:break-all;">Path: <code style="color:#888; font-size:0.9em;">{{ result.path }}</code></div>
                </div>
              </div>
            </div>

            <!-- Error -->
            <div v-if="error"
                 style="background:#3a0010; border:1px solid #dc3545; border-radius:6px;
                        padding:10px 12px; margin-bottom:16px; color:#ff6b6b; font-size:0.85em;">
              <i class="fas fa-exclamation-triangle mr-2"></i>{{ error }}
            </div>

            <!-- Form -->
            <div v-if="!result">
              <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-bottom:12px;">
                <div>
                  <label style="color:#aaa; font-size:0.82em; display:block; margin-bottom:4px;">
                    Namespace <span style="color:#dc3545;">*</span>
                  </label>
                  <input v-model="namespace" class="form-control form-control-sm"
                         placeholder="e.g. petstore/animals" />
                </div>
                <div>
                  <label style="color:#aaa; font-size:0.82em; display:block; margin-bottom:4px;">
                    Dataset ID <span style="color:#dc3545;">*</span>
                  </label>
                  <input v-model="id" class="form-control form-control-sm"
                         placeholder="e.g. available_pets" />
                </div>
              </div>

              <div style="margin-bottom:12px;">
                <label style="color:#aaa; font-size:0.82em; display:block; margin-bottom:4px;">
                  Base URL <span style="color:#dc3545;">*</span>
                </label>
                <input v-model="baseUrl" class="form-control form-control-sm"
                       placeholder="e.g. https://jsonplaceholder.typicode.com" />
              </div>

              <div style="margin-bottom:12px;">
                <label style="color:#aaa; font-size:0.82em; display:block; margin-bottom:4px;">
                  Endpoint <span style="color:#dc3545;">*</span>
                </label>
                <input v-model="endpoint" class="form-control form-control-sm"
                       placeholder="e.g. /posts or /users" />
              </div>

              <div style="margin-bottom:20px;">
                <label style="color:#aaa; font-size:0.82em; display:block; margin-bottom:4px;">
                  Query Params <span style="color:#666;">(JSON, optional)</span>
                </label>
                <input v-model="params" class="form-control form-control-sm"
                       placeholder='e.g. {"userId": 1}' />
              </div>

              <div style="display:flex; gap:10px; justify-content:flex-end;">
                <button class="btn btn-sm btn-outline-secondary" @click="close">
                  Cancel
                </button>
                <button class="btn btn-sm btn-outline-light" @click="submit" :disabled="loading">
                  <i class="fas mr-1" :class="loading ? 'fa-spinner fa-spin' : 'fa-cloud-download-alt'"></i>
                  {{ loading ? 'Materializing...' : 'Materialize' }}
                </button>
              </div>
            </div>

            <!-- After result: actions -->
            <div v-else style="display:flex; gap:10px; justify-content:flex-end; margin-top:16px;">
              <button class="btn btn-sm btn-outline-secondary" @click="result=null; error=''">
                <i class="fas fa-redo mr-1"></i>Again
              </button>
              <button class="btn btn-sm btn-outline-light" @click="close">
                Close
              </button>
            </div>

          </div>
        </div>
      </div>
    </div>
  `
});