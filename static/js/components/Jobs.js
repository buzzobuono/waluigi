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
    const jobs        = ref([]);
    const loading     = ref(false);
    const error       = ref(null);
    const confirmRef  = ref(null);
    const currentPage = ref(1);
    const PAGE_SIZE   = 10;

    const columns = [
      { key: 'job_id',     label: 'Job ID' },
      { key: 'status',     label: 'Status' },
      { key: 'started_at', label: 'Started At' },
      { key: 'locked_by',  label: 'Locked By' },
      { key: 'locked_until', label: 'Locked Until' },
      { key: 'actions',    label: 'Actions', class: 'text-right pr-3' }
    ];

    const STATUS_MAP = {
      RUNNING:   { color: 'warning',   icon: 'fas fa-spinner fa-spin' },
      SUCCESS:   { color: 'success',   icon: 'fas fa-check' },
      FAILED:    { color: 'danger',    icon: 'fas fa-times' },
      PENDING:   { color: 'secondary', icon: 'fas fa-clock' },
      CANCELLED: { color: 'dark',      icon: 'fas fa-ban' },
      PAUSED:    { color: 'info',      icon: 'fas fa-pause' },
    };

    async function load() {
      loading.value = true;
      error.value   = null;
      try {
        jobs.value = await api.jobs();
        currentPage.value = 1;
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    const counts = computed(() => {
      const c = { RUNNING: 0, SUCCESS: 0, FAILED: 0, PENDING: 0, CANCELLED: 0, PAUSED: 0 };
      jobs.value.forEach(j => { if (c[j.status] !== undefined) c[j.status]++; });
      return c;
    });

    const totalPages = computed(() => Math.max(1, Math.ceil(jobs.value.length / PAGE_SIZE)));

    const pagedJobs = computed(() => {
      const start = (currentPage.value - 1) * PAGE_SIZE;
      return jobs.value.slice(start, start + PAGE_SIZE);
    });

    const rangeStart = computed(() => (currentPage.value - 1) * PAGE_SIZE + 1);
    const rangeEnd   = computed(() => Math.min(currentPage.value * PAGE_SIZE, jobs.value.length));

    function changePage(delta) {
      const next = currentPage.value + delta;
      if (next >= 1 && next <= totalPages.value) currentPage.value = next;
    }

    async function pauseJob(jobId) {
      await api.pauseJob(jobId);
      await load();
    }

    async function resumeJob(jobId) {
      await api.resumeJob(jobId);
      await load();
    }

    async function cancelJob(jobId) {
      confirmRef.value.ask(
        `Cancel job "${jobId}"?`,
        async (ok) => {
          if (!ok) return;
          await api.cancelJob(jobId);
          await load();
        }
      );
    }

    async function deleteJob(jobId) {
      confirmRef.value.ask(
        `Delete job "${jobId}"?`,
        async (ok) => {
          if (!ok) return;
          await api.deleteJob(jobId);
          await load();
        }
      );
    }

    onMounted(load);

    return {
      jobs, pagedJobs, loading, error, columns, STATUS_MAP,
      counts, confirmRef, currentPage, totalPages, rangeStart, rangeEnd,
      changePage, load, pauseJob, resumeJob, cancelJob, deleteJob
    };
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

        <base-table
          :columns="columns"
          :items="pagedJobs">

          <template #cell(job_id)="{ item }">
            <div>
              <i class="fas fa-project-diagram mr-2 opacity-75"></i>
              <router-link :to="'/jobs/' + encodeURIComponent(item.job_id)">
                {{ item.job_id }}
              </router-link>
            </div>
          </template>

          <template #cell(status)="{ item }">
            <span :class="['badge shadow', 'badge-' + STATUS_MAP[item.status].color, item.status === 'RUNNING' ? 'blink' : '']">
              {{ item.status }}
            </span>
          </template>

          <template #cell(started_at)="{ item }">
            <span class="text-muted small">
              {{ item.started_at ? new Date(item.started_at + 'Z').toLocaleString() : '—' }}
            </span>
          </template>

          <template #cell(actions)="{ item }">
            <base-button-group>
              <base-button
                v-if="item.status === 'RUNNING' || item.status === 'PENDING'"
                icon="fas fa-pause"
                color="outline-info"
                title="Pause Job"
                @click.stop="pauseJob(item.job_id)"
              />
              <base-button
                v-if="item.status === 'PAUSED'"
                icon="fas fa-play"
                color="outline-info"
                title="Resume Job"
                @click.stop="resumeJob(item.job_id)"
              />
              <base-button
                v-if="item.status === 'RUNNING' || item.status === 'PENDING' || item.status === 'PAUSED'"
                icon="fas fa-ban"
                color="outline-warning"
                title="Cancel Job"
                @click.stop="cancelJob(item.job_id)"
              />
              <base-button
                icon="fas fa-trash"
                color="outline-danger"
                title="Delete Job"
                @click.stop="deleteJob(item.job_id)"
              />
            </base-button-group>
          </template>

        </base-table>

        <template #footer>
          <div class="text-muted small">
            {{ jobs.length ? rangeStart + ' – ' + rangeEnd + ' of ' + jobs.length : 'No jobs' }}
          </div>
        </template>

      </base-panel>

      <confirm-dialog title="Confirm" ref="confirmRef" />

    </base-page>
  `
};
