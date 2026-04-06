import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseTable from './BaseTable.js';
import BaseInfoBox from './BaseInfoBox.js';
import BaseButton from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';

export default {
  name: 'Jobs',
  props: { 
    jobs: { type: Array, default: () => [] },
    loading: { type: Boolean, default: false }
  },
  components: { 
    BasePage, BasePanel, BaseTable, BaseInfoBox, 
    BaseButton, BaseButtonGroup 
  },
  emits: ['refresh'],

  setup() {
    const columns = [
      { key: 'job_id', label: 'Job ID' },
      { key: 'status', label: 'Status' },
      { key: 'locked_by', label: 'Locked By' },
      { key: 'locked_until', label: 'Locked Until' },
      { key: 'actions', label: 'Actions', class: 'text-right pr-3' }
    ];

    const STATUS_MAP = {
      RUNNING: { color: 'warning', icon: 'fas fa-spinner fa-spin' },
      SUCCESS: { color: 'success', icon: 'fas fa-check' },
      FAILED:  { color: 'danger',  icon: 'fas fa-times' },
      PENDING: { color: 'secondary', icon: 'fas fa-clock' }
    };

    return { columns, STATUS_MAP };
  },

  computed: {
    counts() {
      const c = { RUNNING: 0, SUCCESS: 0, FAILED: 0, PENDING: 0 };
      this.jobs.forEach(j => { if (c[j.status] !== undefined) c[j.status]++; });
      return c;
    }
  },

  methods: {
    async deleteJob(jobId) {
      if (!confirm(`Sei sicuro di voler eliminare il job "${jobId}" e tutti i suoi task?`)) return;
      try {
        await api.deleteJob(jobId);
        this.$emit('refresh');
      } catch (e) {
        alert(`Errore durante l'eliminazione: ${e.message}`);
      }
    }
  },

  template: `
    <base-page 
      title="Jobs" 
      subtitle="Job monitoring and managememt"
      icon="fas fa-briefcase"
      :loading="loading && !jobs.length"
    >
      
      <template #actions>
        <div class="row w-100 m-0">
          <div class="col-6 col-md-3 px-1" v-for="(val, key) in counts" :key="key">
            <base-info-box 
              :label="key" 
              :value="val" 
              :icon="STATUS_MAP[key].icon" 
              :color="STATUS_MAP[key].color" 
            />
          </div>
        </div>
        
         <base-button 
            label="Update"
            icon="fas fa-sync-alt" 
            color="outline-primary" 
            size="sm"
            class="ml-auto"
            :loading="loading"
            @click="$emit('refresh')"
          />
  
      </template>

      <base-panel :no-padding="true">
        <base-table 
          :columns="columns" 
          :items="jobs"
        >
          
          <template #cell(job_id)="{ item }">
            <div class="py-1 text-nowrap">
              <i class="fas fa-project-diagram mr-2 text-muted"></i>
              <router-link 
                :to="'/jobs/' + encodeURIComponent(item.job_id)" 
                class="wl-accent font-weight-bold"
              >
                {{ item.job_id }}
              </router-link>
            </div>
          </template>

          <template #cell(status)="{ item }">
            <span :class="['badge shadow-sm', 'badge-' + STATUS_MAP[item.status].color, item.status === 'RUNNING' ? 'blink' : '']"
                  style="min-width: 80px;">
              {{ item.status }}
            </span>
          </template>

          <template #cell(locked_by)="{ item }">
            <span class="text-muted small">{{ item.locked_by || '—' }}</span>
          </template>

          <template #cell(locked_until)="{ item }">
            <span class="text-muted small">{{ item.locked_until || '—' }}</span>
          </template>

          <template #cell(actions)="{ item }">
            <base-button 
              icon="fas fa-trash" 
              color="outline-danger" 
              title="Delete Job"
              @click.stop="deleteJob(item.job_id)"
            />
          </template>

        </base-table>
      </base-panel>

    </base-page>
  `
};
