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
      if (route.query.ns && route.query.id) search();
    });
    
    async function search() {
      const id = idInput.value.trim();
      if (!id) {
        error.value = 'Dataset ID are required.';
        return;
      }
      loading.value    = true;
      error.value      = '';
      upstream.value   = [];
      downstream.value = [];
      current.value    = null;

      try {
        let ver = verInput.value.trim();
        if (!ver) {
          const res = await api.catalogDatasetVersions(id);
          if (!res || !res.data || !res.data.versions || !res.data.versions.length) {
            error.value = 'Dataset not found or no committed versions.';
            return;
          }
          ver = res.data.versions[0].version;
          current.value = res.data.versions[0];
          verInput.value = ver;
        } else {
          current.value = { id: id, version: ver };
        }

        
        const lineage = await api.catalogDatasetLineage(id, ver);
        
        upstream.value   = lineage.data.upstream || [];
        downstream.value = lineage.data.downstream || [];

      } catch(e) {
        error.value = `Error: ${e.message}`;
      } finally {
        loading.value = false;
      }
    }

    function navigateTo(id) {
      idInput.value  = id;
      verInput.value = '';
      search();
    }

    return {
      idInput, verInput,
      upstream, downstream, current,
      loading, error,
      search, navigateTo,
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
              placeholder="e.g. sales_raw"
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
              @click="search"
            />
          </div>

          <div v-if="error" class="col-12 text-danger small">
            {{ error }}
          </div>

        </div>
      </template>

      <div class="row">

        <div class="col-12 col-md-4 mb-3">
          <base-panel :no-padding="true">
            <template #title>
              <i class="fas fa-arrow-up mr-2"></i>
              Upstream
              <span class="badge badge-info ml-2">{{ upstream.length }}</span>
            </template>

            <div v-if="!upstream.length" class="text-muted p-3 text-center">
              No upstream — source dataset
            </div>

            <div
              v-for="u in upstream"
              :key="u.id + '/' + u.version"
              class="p-3 border-bottom cursor-pointer"
              @click="navigateTo(u.namespace, u.id)"
            >
              <div class="text-muted small">
                {{ u.namespace }}
              </div>

              <div class="text-info" >
                {{ u.id }}
              </div>

              <div class="text-secondary small" >
                {{ u.version ? u.version : 'live' }}
              </div>

              <div class="mt-1">
                <span v-if="u.format" class="badge badge-secondary mr-1">
                  {{ u.format }}
                </span>

                <span v-if="u.rows != null" class="text-muted small">
                  {{ u.rows.toLocaleString() }} rows
                </span>
              </div>
            </div>

          </base-panel>
        </div>

        <div class="col-12 col-md-4 mb-3">
          <base-panel :no-padding="true">
            <template #title>
              <i class="fas fa-database mr-2"></i>
              Current
            </template>

            <div v-if="current" class="p-3">

              <div class="text-muted small">
                {{ current.namespace }}
              </div>

              <div class="text-info font-weight-bold mt-1" >
                {{ current.id }}
              </div>

              <div class="text-secondary small mt-1" >
                {{ current.version }}
              </div>

              <div class="mt-2">
                <span v-if="current.format" class="badge badge-secondary mr-1">
                  {{ current.format }}
                </span>

                <span v-if="current.rows != null" class="text-muted small">
                  {{ current.rows.toLocaleString() }} rows
                </span>
              </div>

              <div v-if="current.produced_by_task" class="mt-2 text-muted small">
                Task: <code>{{ current.produced_by_task }}</code>
              </div>

              <div v-if="current.hash" class="mt-1 text-muted small" >
                {{ current.hash.slice(0,16) }}...
              </div>

            </div>

          </base-panel>
        </div>

        <div class="col-12 col-md-4 mb-3">
          <base-panel :no-padding="true">
            <template #title>
              <i class="fas fa-arrow-down mr-2"></i>
              Downstream
              <span class="badge badge-success ml-2">{{ downstream.length }}</span>
            </template>

            <div v-if="!downstream.length" class="text-muted p-3 text-center">
              No downstream — leaf dataset
            </div>

            <div
              v-for="d in downstream"
              :key="d.id + '/' + d.version"
              class="p-3 border-bottom cursor-pointer"
              @click="navigateTo(d.namespace, d.id)"
            >
              <div class="text-muted small">
                {{ d.namespace }}
              </div>

              <div class="text-info" >
                {{ d.id }}
              </div>

              <div class="text-secondary small" >
                {{ d.version }}
              </div>

              <div class="mt-1">
                <span v-if="d.format" class="badge badge-secondary mr-1">
                  {{ d.format }}
                </span>

                <span v-if="d.rows != null" class="text-muted small">
                  {{ d.rows.toLocaleString() }} rows
                </span>
              </div>
            </div>

          </base-panel>
        </div>

      </div>

    </base-page>
  `
};
