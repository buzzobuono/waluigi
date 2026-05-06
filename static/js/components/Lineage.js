import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseInput from './BaseInput.js';
import BaseButton from './BaseButton.js';

export default {
  name: 'Lineage',
  components: { BasePage, BasePanel, BaseButton, BaseInput },
  setup() {
    const idInput    = Vue.ref('');
    const verInput   = Vue.ref('');
    const upstream   = Vue.ref([]);
    const downstream = Vue.ref([]);
    const current    = Vue.ref(null);
    const loading    = Vue.ref(false);
    const error      = Vue.ref('');

    const route = VueRouter.useRoute();
    Vue.onMounted(() => {
      if (route.query.id)  idInput.value  = route.query.id;
      if (route.query.ver) verInput.value = route.query.ver;
      if (route.query.id)  search();
    });

    async function search() {
      const id = idInput.value.trim();
      if (!id) { error.value = 'Dataset ID is required.'; return; }

      loading.value    = true;
      error.value      = '';
      upstream.value   = [];
      downstream.value = [];
      current.value    = null;

      try {
        let ver = verInput.value.trim();
        const versionsRes = await api.catalogDatasetVersions(id);
        const versions    = versionsRes?.data || [];

        if (!versions.length) {
          error.value = 'Dataset not found or no committed versions.';
          return;
        }

        if (!ver) {
          ver = versions[0].version;
          verInput.value = ver;
        }

        current.value = versions.find(v => v.version === ver) || { dataset_id: id, version: ver };

        const lineage    = await api.catalogDatasetLineage(id, ver);
        upstream.value   = lineage.data.upstream   || [];
        downstream.value = lineage.data.downstream || [];

      } catch (e) {
        error.value = `Error: ${e.message}`;
      } finally {
        loading.value = false;
      }
    }

    function navigateTo(id, version) {
      if (!id || id.startsWith('__external__/')) return;
      idInput.value  = id;
      verInput.value = (version && version !== 'live') ? version : '';
      search();
    }

    function isExternal(id) {
      return id && id.startsWith('__external__/');
    }

    function displayId(id) {
      return isExternal(id) ? id.replace('__external__/', '') : id;
    }

    return {
      idInput, verInput,
      upstream, downstream, current,
      loading, error,
      search, navigateTo, isExternal, displayId,
    };
  },

  template: `
    <base-page
      title="Lineage"
      subtitle="Explore dataset lineage"
      icon="fas fa-project-diagram"
    >

      <template #actions>
        <div class="row w-100">

          <div class="col-12 col-sm-4 mb-2">
            <label class="text-muted small">Dataset ID</label>
            <BaseInput
              v-model="idInput"
              placeholder="e.g. sales/raw/transactions"
              @keyup.enter="search"
            />
          </div>

          <div class="col-12 col-sm-2 mb-2">
            <label class="text-muted small">Version</label>
            <BaseInput
              v-model="verInput"
              placeholder="latest"
              @keyup.enter="search"
            />
          </div>

          <div class="col-12 col-sm-2 mb-2 d-flex align-items-end">
            <BaseButton
              label="Search"
              icon="fas fa-search"
              color="outline-primary"
              :disabled="loading"
              @click="search"
            />
          </div>

          <div v-if="error" class="col-12 text-danger small mt-1">
            {{ error }}
          </div>

        </div>
      </template>

      <div v-if="loading" class="text-center py-4 text-muted">
        <i class="fas fa-spinner fa-spin mr-2"></i> Loading...
      </div>

      <div v-else-if="current" class="row">

        <!-- Upstream -->
        <div class="col-12 col-md-4 mb-3">
          <base-panel :no-padding="true">
            <template #title>
              <i class="fas fa-arrow-up mr-2"></i>
              Upstream
              <span class="badge badge-info ml-2">{{ upstream.length }}</span>
            </template>

            <div v-if="!upstream.length" class="text-muted p-3 text-center small">
              No upstream — source dataset
            </div>

            <div
              v-for="u in upstream"
              :key="u.dataset_id + '/' + u.version"
              class="p-3 border-bottom"
              :class="isExternal(u.dataset_id) ? 'text-muted' : 'cursor-pointer'"
              @click="navigateTo(u.dataset_id, u.version)"
            >
              <div :class="isExternal(u.dataset_id) ? 'text-muted small' : 'text-info'">
                <i v-if="isExternal(u.dataset_id)" class="fas fa-external-link-alt mr-1"></i>
                {{ displayId(u.dataset_id) }}
              </div>

              <div class="text-secondary small mt-1">
                <span v-if="u.version === 'live'" class="badge badge-light">live</span>
                <span v-else>{{ u.version ? u.version.slice(0, 19) : '—' }}</span>
              </div>

            </div>

          </base-panel>
        </div>

        <!-- Current -->
        <div class="col-12 col-md-4 mb-3">
          <base-panel :no-padding="true">
            <template #title>
              <i class="fas fa-database mr-2"></i>
              Current
            </template>

            <div class="p-3">
              <div class="text-info font-weight-bold">{{ current.dataset_id }}</div>

              <div class="text-secondary small mt-1">{{ current.version ? current.version.slice(0, 19) : '' }}</div>

              <div v-if="current.produced_by_task" class="mt-2 text-muted small">
                Task: <code>{{ current.produced_by_task }}</code>
              </div>
            </div>

          </base-panel>
        </div>

        <!-- Downstream -->
        <div class="col-12 col-md-4 mb-3">
          <base-panel :no-padding="true">
            <template #title>
              <i class="fas fa-arrow-down mr-2"></i>
              Downstream
              <span class="badge badge-success ml-2">{{ downstream.length }}</span>
            </template>

            <div v-if="!downstream.length" class="text-muted p-3 text-center small">
              No downstream — leaf dataset
            </div>

            <div
              v-for="d in downstream"
              :key="d.dataset_id + '/' + d.version"
              class="p-3 border-bottom cursor-pointer"
              @click="navigateTo(d.dataset_id, d.version)"
            >
              <div class="text-info">{{ d.dataset_id }}</div>

              <div class="text-secondary small mt-1">
                {{ d.version ? d.version.slice(0, 19) : '—' }}
              </div>
            </div>

          </base-panel>
        </div>

      </div>

    </base-page>
  `
};
