import { api, getToken } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseButton from './BaseButton.js';

const { ref, onMounted } = Vue;

function _isAdmin() {
  try { return JSON.parse(atob(getToken().split('.')[1])).namespaces === '*'; }
  catch { return false; }
}

export default {
  name: 'Resources',
  components: { BasePage, BasePanel, BaseButton },

  setup() {
    const isAdmin   = _isAdmin();
    const resources = ref([]);
    const loading   = ref(false);
    const error     = ref(null);

    async function load() {
      loading.value = true;
      error.value   = null;
      try {
        resources.value = await api.resources();
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

    onMounted(() => { if (isAdmin) load(); });

    return { isAdmin, resources, loading, error, load, pct, color };
  },

  template: `
    <base-page v-if="!isAdmin" title="Resources" icon="fas fa-microchip">
      <div class="alert alert-warning">
        <i class="fas fa-lock mr-2"></i>Access restricted to administrators.
      </div>
    </base-page>

    <base-page v-else
      title="Resources"
      subtitle="Cluster resources consumption"
      icon="fas fa-microchip"
      :loading="loading && !resources.length"
      :error="error">

      <template #actions>
        <base-button
          icon="fas fa-sync-alt"
          color="outline-primary"
          label="Update"
          :loading="loading"
          class="ml-auto"
          @click="load"
        />
      </template>

      <div v-if="!resources.length" class="text-muted mt-3 text-center">
        No resources configured.
      </div>

      <div v-else class="row">
        <div class="col-12 col-sm-6 col-md-4" v-for="r in resources" :key="r.name">
          <base-panel>
            <template #title>
              <h3 class="card-title">{{ r.name }}</h3>
            </template>

            <template #tools>
              <span :class="['badge', 'bg-'+color(r), 'ml-auto']">{{ pct(r) }}%</span>
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
    </base-page>
  `
};
