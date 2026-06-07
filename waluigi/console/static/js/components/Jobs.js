import { api } from '../api.js';
import { nsStore } from '../store.js';
import { JOB_STATUS, JOB_STATUSES } from '../config.js';
import { fmtDt } from '../utils.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseTable from './BaseTable.js';
import BaseInfoBox from './BaseInfoBox.js';
import BaseButton from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import ConfirmDialog from './ConfirmDialog.js';

const { ref, computed, watch } = Vue;

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
      { key: 'job_id',            label: 'Job ID' },
      { key: 'execution_policy',  label: 'Exec' },
      { key: 'concurrency_policy', label: 'Concurrency' },
      { key: 'status',            label: 'Status' },
      { key: 'started_at',        label: 'Started At' },
      { key: 'locked_by',         label: 'Locked By' },
      { key: 'actions',           label: 'Actions', class: 'text-right pr-3' }
    ];

    async function load() {
      if (!nsStore.selected) { jobs.value = []; return; }
      loading.value = true;
      error.value   = null;
      try {
        jobs.value = await api.jobs(nsStore.selected);
        currentPage.value = 1;
      } catch (e) {
        error.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    watch(() => nsStore.selected, load, { immediate: true });

    const counts = computed(() => {
      const c = Object.fromEntries(JOB_STATUSES.map(s => [s.key, 0]));
      jobs.value.forEach(j => { if (c[j.status] !== undefined) c[j.status]++; });
      return c;
    });

    const totalPages = computed(() => Math.max(1, Math.ceil(jobs.value.length / PAGE_SIZE)));
    const pagedJobs  = computed(() => {
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
      try { await api.pauseJob(nsStore.selected, jobId); await load(); }
      catch (e) { error.value = e.message; }
    }
    async function resumeJob(jobId) {
      try { await api.resumeJob(nsStore.selected, jobId); await load(); }
      catch (e) { error.value = e.message; }
    }
    async function cancelJob(jobId) {
      confirmRef.value.ask(`Cancel job "${jobId}"?`, async (ok) => {
        if (!ok) return;
        try { await api.cancelJob(nsStore.selected, jobId); await load(); }
        catch (e) { error.value = e.message; }
      });
    }
    async function deleteJob(jobId) {
      confirmRef.value.ask(`Delete job "${jobId}"?`, async (ok) => {
        if (!ok) return;
        try { await api.deleteJob(nsStore.selected, jobId); await load(); }
        catch (e) { error.value = e.message; }
      });
    }

    return {
      jobs, pagedJobs, loading, error, columns, JOB_STATUS, JOB_STATUSES, nsStore,
      counts, confirmRef, currentPage, totalPages, rangeStart, rangeEnd,
      changePage, load, pauseJob, resumeJob, cancelJob, deleteJob, fmtDt,
    };
  },

  template: `
    <base-page
      title="Jobs"
      :subtitle="nsStore.selected ? 'Namespace: ' + nsStore.selected : 'Select a namespace'"
      icon="fas fa-briefcase"
      :loading="loading && !jobs.length"
      :error="error">

      <template #actions>
        <div class="row w-100 m-0">
          <div class="col-6 col-md-2 px-1" v-for="s in JOB_STATUSES" :key="s.key">
            <base-info-box
              :label="s.key"
              :value="counts[s.key]"
              :icon="s.icon"
              :color="s.color"
            />
          </div>
        </div>
        <base-button
          label="Refresh"
          icon="fas fa-sync-alt"
          color="outline-primary"
          class="ml-auto mt-2"
          :loading="loading"
          @click="load"
        />
      </template>

      <div v-if="!nsStore.selected" class="text-center py-5 text-muted">
        <i class="fas fa-layer-group fa-3x mb-3 opacity-75"></i>
        <p>Select a namespace from the header to view jobs.</p>
      </div>

      <base-panel v-else :no-padding="true">

        <template #tools>
          <base-button-group class="ml-auto">
            <base-button :disabled="loading || currentPage <= 1"
                         icon="fas fa-chevron-left" color="outline-primary"
                         @click="changePage(-1)" />
            <base-button :label="currentPage + ' / ' + totalPages"
                         :disabled="true" color="outline-secondary" />
            <base-button :disabled="loading || currentPage >= totalPages"
                         icon="fas fa-chevron-right" color="outline-primary"
                         @click="changePage(1)" />
          </base-button-group>
        </template>

        <base-table :columns="columns" :items="pagedJobs">

          <template #cell(job_id)="{ item }">
            <div>
              <i class="fas fa-project-diagram mr-2 opacity-75"></i>
              <router-link :to="'/jobs/' + encodeURIComponent(nsStore.selected) + '/' + encodeURIComponent(item.job_id)">
                {{ item.job_id }}
              </router-link>
            </div>
          </template>

          <template #cell(execution_policy)="{ item }">
            <span :class="['badge', item.execution_policy === 'Stateful' ? 'badge-primary' : 'badge-secondary']">
              {{ item.execution_policy || 'Ephemeral' }}
            </span>
          </template>

          <template #cell(concurrency_policy)="{ item }">
            <span class="text-muted small">{{ item.concurrency_policy || 'Forbid' }}</span>
          </template>

          <template #cell(status)="{ item }">
            <span :class="['badge shadow', 'badge-' + (JOB_STATUS[item.status]?.color || 'secondary'), item.status === 'RUNNING' ? 'blink' : '']">
              {{ item.status }}
            </span>
          </template>

          <template #cell(started_at)="{ item }">
            <span class="text-muted small">
              {{ fmtDt(item.started_at) }}
            </span>
          </template>

          <template #cell(actions)="{ item }">
            <base-button-group>
              <base-button v-if="item.status === 'RUNNING' || item.status === 'PENDING'"
                           icon="fas fa-pause" color="outline-info" title="Pause"
                           @click.stop="pauseJob(item.job_id)" />
              <base-button v-if="item.status === 'PAUSED'"
                           icon="fas fa-play" color="outline-info" title="Resume"
                           @click.stop="resumeJob(item.job_id)" />
              <base-button v-if="['RUNNING','PENDING','PAUSED'].includes(item.status)"
                           icon="fas fa-ban" color="outline-warning" title="Cancel"
                           @click.stop="cancelJob(item.job_id)" />
              <base-button icon="fas fa-trash" color="outline-danger" title="Delete"
                           @click.stop="deleteJob(item.job_id)" />
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
