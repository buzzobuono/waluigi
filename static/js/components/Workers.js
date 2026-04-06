import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseTable from './BaseTable.js';
import BaseInfoBox from './BaseInfoBox.js';
import BaseButton from './BaseButton.js';

export default {
  name: 'Workers',
  props: { 
    workers: { type: Array, default: () => [] },
    loading: { type: Boolean, default: false } 
  },
  components: { BasePage, BasePanel, BaseTable, BaseInfoBox, BaseButton },

  setup() {
    const columns = [
      { key: 'url', label: 'URL' },
      { key: 'status', label: 'Status' },
      { key: 'slots', label: 'Slots (Used/Max)', class: 'text-center' },
      { key: 'last_seen', label: 'Last Seen' }
    ];

    return { columns };
  },

  computed: {
    totalSlots() { return this.workers.reduce((s, w) => s + (w.max_slots || 0), 0); },
    freeSlots()  { return this.workers.reduce((s, w) => s + (w.free_slots || 0), 0); },
    busySlots()  { return this.totalSlots - this.freeSlots; }
  },

  template: `
    <base-page 
      title="Workers" 
      subtitle="Stato dei nodi e degli slot"
      icon="fas fa-server"
      :loading="loading && !workers.length"
    >
      
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
            size="sm"
            label="Update"
            class="ml-auto"
            :loading="loading"
            @click="$emit('refresh')"
          />
        
      </template>

      <base-panel :no-padding="true">
        <base-table :columns="columns" :items="workers">
          
          <template #cell(url)="{ item }">
            <code class="wl-accent">{{ item.url }}</code>
          </template>

          <template #cell(status)="{ item }">
            <span :class="['badge', item.status === 'ALIVE' ? 'badge-success' : 'badge-danger']">
              {{ item.status || 'ALIVE' }}
            </span>
          </template>

          <template #cell(slots)="{ item }">
            <div class="text-center">
              <span class="text-danger font-weight-bold">{{ item.max_slots - item.free_slots }}</span>
              <span class="text-mutedmx-1">/</span>
              <span class="text-info font-weight-bold">{{ item.max_slots }}</span>
            </div>
          </template>

          <template #cell(last_seen)="{ item }">
            <span class="text-muted small">
              <i class="far fa-clock mr-1"></i>{{ item.last_seen || '—' }}
            </span>
          </template>

        </base-table>
      </base-panel>

    </base-page>
  `
};
