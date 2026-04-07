import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseTable from './BaseTable.js';
import BaseButton from './BaseButton.js';
import BaseSearch from './BaseSearch.js';

const { defineComponent, ref, computed, onMounted } = Vue;
const { useRouter } = VueRouter;

export default defineComponent({
  name: 'Namespaces',
  components: { BasePage, BasePanel, BaseTable, BaseButton, BaseSearch },

  setup() {
    const router = useRouter();
    const items = ref([]);
    const loading = ref(false);
    const error = ref('');
    const filterText = ref('');

    const columns = [
      { key: 'namespace', label: 'Namespace' },
      { key: 'task_count', label: 'Tasks', class: 'text-center', style: 'width: 120px;' }
    ];

    async function load() {
      loading.value = true;
      error.value = '';
      try {
        const data = await api.namespaces();
        items.value = Array.isArray(data) ? data : [];
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

    onMounted(load);

    return {
      items, loading, error, filterText, columns,
      filteredItems, load
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
        <base-table :columns="columns" :items="filteredItems">
          <template #cell(namespace)="{ item }">
            <div>
              <i class="fas fa-folder mr-2 text-warning opacity-75"></i>
              <router-link :to="'/tasks/' + encodeURIComponent(item.namespace)"> 
                {{ item.namespace }}
              </router-link>
            </div>
          </template>
          <template #cell(task_count)="{ item }">
            <span class="badge badge-info">
              {{ item.task_count }}
            </span>
          </template>

        </base-table>
      </base-panel>

    </base-page>
  `
});
