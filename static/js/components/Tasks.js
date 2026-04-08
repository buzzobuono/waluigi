import { api } from '../api.js';
import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseTable from './BaseTable.js';
import BaseButton from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';
import BaseModal from './BaseModal.js';
import LogModal from './LogModal.js';

export default {
  name: 'Tasks',
  props: { tasks: Array, loading: Boolean },
  components: { BasePage, BasePanel, BaseTable, LogModal, BaseButton, BaseButtonGroup, BaseModal },
  emits: ['refresh'],

  setup(props, { emit }) {
    const route = VueRouter.useRoute();
    const logModalRef = Vue.ref(null);
    const baseModalRef = Vue.ref(null);

    const STATUS_COLOR = {
      SUCCESS: '#28a745',
      FAILED:  '#dc3545',
      RUNNING: '#ffc107',
      READY:   '#17a2b8',
      PENDING: '#6c757d',
    };

    const columns = [
      { key: 'id', label: 'Task ID' },
      { key: 'params', label: 'Params' },
      { key: 'status', label: 'Status' },
      { key: 'update', label: 'Last Update' },
      { key: 'actions', label: 'Actions', class: 'text-right pr-3' }
    ];

    const resetTask = async (id) => {
      if (!confirm(`Reset task "${id}"?`)) return;
      await api.resetTask(id);
      emit('refresh');
    };

    const deleteTask = async (id) => {
      if (!confirm(`Delete task "${id}"?`)) return;
      await api.deleteTask(id);
      emit('refresh');
    };

    const resetNs = async (ns) => {
      if (!confirm(`Reset all in "${ns}"?`)) return;
      await api.resetNamespace(ns);
      emit('refresh');
    };

    const deleteNs = async (ns) => {
      if (!confirm(`Delete all in "${ns}"?`)) return;
      await api.deleteNamespace(ns);
      emit('refresh');
    };

    const openLogs = (id) => {
      if (logModalRef.value) logModalRef.value.show(id);
    };

    return { 
      columns, STATUS_COLOR, route, logModalRef, baseModalRef,
      resetTask, deleteTask, resetNs, deleteNs, openLogs 
    };
  },

  computed: {
    filterNs() {
      const p = this.route.params.namespace;
      return p ? (Array.isArray(p) ? p.join('/') : p) : null;
    },

    byNamespace() {
      const map = {};
      const filtered = this.filterNs
        ? (this.tasks || []).filter(t => t.namespace === this.filterNs)
        : (this.tasks || []);

      filtered.forEach(t => {
        const ns = t.namespace || '(none)';
        if (!map[ns]) map[ns] = [];
        map[ns].push(t);
      });
      return map;
    }
  },

  template: `
    <base-page 
      :title="filterNs ? 'Tasks in ' + filterNs : 'All Tasks'"
      :subtitle="filterNs ? 'Namespace View' : 'Global View'"
      icon="fas fa-tasks">
      
      <template #actions>
         <base-button 
            v-if="filterNs" 
            label="Back" 
            icon="fas fa-arrow-left" 
            color="outline-secondary"
            @click="$router.push('/namespaces')"
          />
          
          <base-button 
            label="Update" 
            class="ml-auto"
            icon="fas fa-sync-alt" 
            color="outline-primary" 
            :loading="loading"
            @click="$emit('refresh')"
          />
  
      </template>

      <div v-if="!Object.keys(byNamespace).length" class="text-center py-5 text-muted">
        <i class="fas fa-filter fa-3x mb-3 opacity-75"></i>
        <p>No tasks found for this selection.</p>
      </div>

      <base-panel 
        v-for="(taskList, ns) in byNamespace" 
        :key="ns" 
        :no-padding="true">

        <template #title>
          <i class="fas fa-layer-group mr-2"></i>
          <span class="font-weight-bold">Namespace: </span>
          <code class="ml-2">{{ ns }}</code>
        </template>

        <template #tools>
          <base-button-group class="ml-auto">
            <base-button 
              label="Reset"
              icon="fas fa-history" 
              color="outline-warning" 
              @click="resetNs(ns)"
            />
            <base-button 
              label="Delete"
              icon="fas fa-trash-alt" 
              color="outline-danger" 
              @click="deleteNs(ns)"
            />
          </base-button-group>
        </template>

        <base-table :columns="columns" :items="taskList">
          
          <template #cell(id)="{ item }">
            <a href="#" @click.prevent="openLogs(item.id)">
              {{ item.id }}
            </a>
          </template>

          <template #cell(status)="{ item }">
            <span class="badge shadow" :style="{ background: STATUS_COLOR[item.status], color: '#fff', minWidth: '70px' }">
              {{ item.status }}
            </span>
          </template>

          <template #cell(update)="{ item }">
            <i class="far fa-clock mr-1"></i>{{ item.last_update || '—' }}
          </template>

          <template #cell(actions)="{ item }">
            <base-button-group>
              <base-button 
                icon="fas fa-undo" 
                color="outline-warning" 
                title="Reset Task"
                @click="resetTask(item.id)"
              />
              <base-button 
                icon="fas fa-trash" 
                color="outline-danger" 
                title="Delete Task"
                @click="deleteTask(item.id)"
              />
            </base-button-group>
          </template>

        </base-table>
      </base-panel>

      <log-modal ref="logModalRef" />
      
    </base-page>
  `
};
