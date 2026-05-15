import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseTable from './BaseTable.js';
import BaseInfoBox from './BaseInfoBox.js';
import BaseButton from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import ConfirmDialog from './ConfirmDialog.js';

const { ref, computed, onMounted } = Vue;

export default {
  name: 'Jobs',
  components: {
    BasePage, BasePanel, BaseTable, BaseInfoBox,
    BaseButton, BaseButtonGroup, ConfirmDialog
  },

  setup() {
    const jobs       = ref([]);
    const loading    = ref(false);
    const error      = ref(null);
    const confirmRef = ref(null);

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

    async function load() {
      loading.value = true;
      error.value   = null;
      try {
        jobs.value = await api.jobs();
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    const counts = computed(() => {
      const c = { RUNNING: 0, SUCCESS: 0, FAILED: 0, PENDING: 0 };
      jobs.value.forEach(j => { if (c[j.status] !== undefined) c[j.status]++; });
      return c;
    });

    async function deleteJob(jobId) {
      confirmRef.value.ask(
        f`Delete job "${jobId}"?`,
        async (ok) => {
          if (!ok) return;
          await api.deleteJob(jobId);
          await load();
        }
      );
    }

    onMounted(load);

    return { jobs, loading, error, columns, STATUS_MAP, counts, confirmRef, load, deleteJob };
  },

  template: `
    <base-page
      title="Jobs"
      subtitle="Job monitoring and management"
      icon="fas fa-briefcase"
      :loading="loading && !jobs.length"
      :error="error">

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
          class="ml-auto"
          :loading="loading"
          @click="load"
        />
      </template>

      <base-panel :no-padding="true">
        <base-table
          :columns="columns"
          :items="jobs">

          <template #cell(job_id)="{ item }">
            <div>
              <i class="fas fa-project-diagram mr-2 mr-2 opacity-75"></i>
              <router-link :to="'/jobs/' + encodeURIComponent(item.job_id)" >
                  {{ item.job_id }}
              </router-link>
            </div>
          </template>

          <template #cell(status)="{ item }">
            <span :class="['badge shadow', 'badge-' + STATUS_MAP[item.status].color, item.status === 'RUNNING' ? 'blink' : '']">
              {{ item.status }}
            </span>
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

      <confirm-dialog title="Confirm" ref="confirmRef" />

    </base-page>
  `
};
