import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseTable from './BaseTable.js';
import BaseInfoBox from './BaseInfoBox.js';
import BaseButton from './BaseButton.js';

const { ref, computed, onMounted } = Vue;

export default {
  name: 'Workers',
  components: { BasePage, BasePanel, BaseTable, BaseInfoBox, BaseButton },

  setup() {
    const workers = ref([]);
    const loading = ref(false);
    const error   = ref(null);

    const columns = [
      { key: 'url', label: 'URL' },
      { key: 'status', label: 'Status' },
      { key: 'slots', label: 'Slots (Used/Max)', class: 'text-center' },
      { key: 'last_seen', label: 'Last Seen' }
    ];

    async function load() {
      loading.value = true;
      error.value   = null;
      try {
        workers.value = await api.workers();
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    const totalSlots = computed(() => workers.value.reduce((s, w) => s + (w.max_slots  || 0), 0));
    const freeSlots  = computed(() => workers.value.reduce((s, w) => s + (w.free_slots || 0), 0));
    const busySlots  = computed(() => totalSlots.value - freeSlots.value);

    onMounted(load);

    return { workers, loading, error, columns, totalSlots, freeSlots, busySlots, load };
  },

  template: `
    <base-page
      title="Workers"
      subtitle="Worker status and slot availability"
      icon="fas fa-server"
      :loading="loading && !workers.length"
      :error="error">

      <template #actions>
        <div class="row w-100 m-0">
          <div class="col-6 col-md-3 px-1">
            <base-info-box label="Workers" :value="workers.length" icon="fas fa-server" color="success" />
          </div>
          <div class="col-6 col-md-3 px-1">
            <base-info-box label="Busy Slots" :value="busySlots" icon="fas fa-th-large" color="danger" />
          </div>
          <div class="col-6 col-md-3 px-1">
            <base-info-box label="Free Slots" :value="freeSlots" icon="fas fa-check-circle" color="warning" />
          </div>
          <div class="col-6 col-md-3 px-1">
            <base-info-box label="Total" :value="totalSlots" icon="fas fa-layer-group" color="info" />
          </div>
        </div>

        <base-button
          icon="fas fa-sync-alt"
          color="outline-primary"
          label="Update"
          class="ml-auto"
          :loading="loading"
          @click="load"
        />
      </template>

      <base-panel :no-padding="true">
        <base-table :columns="columns" :items="workers">

          <template #cell(status)="{ item }">
            <span :class="['badge', item.status === 'ALIVE' ? 'badge-success' : 'badge-danger']">
              {{ item.status || 'ALIVE' }}
            </span>
          </template>

          <template #cell(slots)="{ item }">
            <span class="text-danger font-weight-bold">{{ item.max_slots - item.free_slots }}</span>
            <span class="text-mutedmx-1">/</span>
            <span class="text-info font-weight-bold">{{ item.max_slots }}</span>
          </template>

          <template #cell(last_seen)="{ item }">
            <i class="far fa-clock mr-1"></i>{{ item.last_seen || '—' }}
          </template>

        </base-table>
      </base-panel>

    </base-page>
  `
};
