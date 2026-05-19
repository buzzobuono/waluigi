import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseTable from './BaseTable.js';
import BaseButton from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseSearch from './BaseSearch.js';

const { defineComponent, ref, computed, watch, onMounted } = Vue;
const { useRouter } = VueRouter;

export default defineComponent({
  name: 'Namespaces',
  components: { BasePage, BasePanel, BaseTable, BaseButton, BaseButtonGroup, BaseSearch },

  setup() {
    const router      = useRouter();
    const items       = ref([]);
    const loading     = ref(false);
    const error       = ref('');
    const filterText  = ref('');
    const currentPage = ref(1);
    const PAGE_SIZE   = 10;

    const columns = [
      { key: 'namespace',  label: 'Namespace' },
      { key: 'task_count', label: 'Tasks', class: 'text-center', style: 'width: 120px;' }
    ];

    async function load() {
      loading.value = true;
      error.value   = '';
      try {
        const data  = await api.namespaces();
        items.value = Array.isArray(data) ? data : [];
        currentPage.value = 1;
      } catch (e) {
        error.value = `API Error: ${e.message}`;
      } finally {
        loading.value = false;
      }
    }

    const filteredItems = computed(() => {
      if (!filterText.value) return items.value;
      const q = filterText.value.toLowerCase();
      return items.value.filter(it => it.namespace.toLowerCase().includes(q));
    });

    watch(filterText, () => { currentPage.value = 1; });

    const totalPages = computed(() => Math.max(1, Math.ceil(filteredItems.value.length / PAGE_SIZE)));

    const pagedItems = computed(() => {
      const start = (currentPage.value - 1) * PAGE_SIZE;
      return filteredItems.value.slice(start, start + PAGE_SIZE);
    });

    const rangeStart = computed(() => (currentPage.value - 1) * PAGE_SIZE + 1);
    const rangeEnd   = computed(() => Math.min(currentPage.value * PAGE_SIZE, filteredItems.value.length));

    function changePage(delta) {
      const next = currentPage.value + delta;
      if (next >= 1 && next <= totalPages.value) currentPage.value = next;
    }

    onMounted(load);

    return {
      items, loading, error, filterText, columns,
      pagedItems, filteredItems, currentPage, totalPages,
      rangeStart, rangeEnd, changePage, load
    };
  },

  template: `
    <base-page
      title="Namespaces"
      subtitle="Available namespaces"
      icon="fas fa-layer-group"
      :loading="loading && !items.length"
      :error="error">

      <template #actions>
        <base-search
          v-model="filterText"
          placeholder="Find namespace..."
        />
        <base-button
          label="Update"
          icon="fas fa-sync-alt"
          color="outline-primary"
          class="ml-auto"
          @click="load"
        />
      </template>

      <base-panel :no-padding="true">

        <template #tools>
          <base-button-group class="ml-auto">
            <base-button
              :disabled="loading || currentPage <= 1"
              icon="fas fa-chevron-left"
              color="outline-primary"
              @click="changePage(-1)"
            />
            <base-button
              :label="String(currentPage) + ' / ' + String(totalPages)"
              :disabled="true"
              color="outline-secondary"
            />
            <base-button
              :disabled="loading || currentPage >= totalPages"
              icon="fas fa-chevron-right"
              color="outline-primary"
              @click="changePage(1)"
            />
          </base-button-group>
        </template>

        <base-table :columns="columns" :items="pagedItems">
          <template #cell(namespace)="{ item }">
            <div>
              <i class="fas fa-folder mr-2 text-warning opacity-75"></i>
              <router-link :to="'/tasks/' + encodeURIComponent(item.namespace)">
                {{ item.namespace }}
              </router-link>
            </div>
          </template>
          <template #cell(task_count)="{ item }">
            <span class="badge badge-info">{{ item.task_count }}</span>
          </template>
        </base-table>

        <template #footer>
          <div class="text-muted small">
            {{ filteredItems.length ? rangeStart + ' – ' + rangeEnd + ' of ' + filteredItems.length : 'No namespaces' }}
          </div>
        </template>

      </base-panel>

    </base-page>
  `
});
