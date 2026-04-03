// components/Tasks.js
import { api } from '../api.js';

export default {
  name: 'Tasks',
  props: { tasks: Array },
  emits: ['refresh'],
  inject: ['showLogs'],

  setup() {
    const STATUS_COLOR = {
      SUCCESS: '#28a745',
      FAILED:  '#dc3545',
      RUNNING: '#ffc107',
      READY:   '#17a2b8',
      PENDING: '#6c757d',
    };

    const route = VueRouter.useRoute();

    return { STATUS_COLOR, route };
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
        map[ns].push({
          id:     t.id,
          params: t.params,
          status: t.status,
          update: t.last_update
        });
      });
      return map;
    }
  },

  methods: {
    async resetTask(id) {
      if (!confirm(`Reset task "${id}"?`)) return;
      await api.resetTask(id);
      this.$emit('refresh');
    },
    async deleteTask(id) {
      if (!confirm(`Delete task "${id}"?`)) return;
      await api.deleteTask(id);
      this.$emit('refresh');
    },
    async resetNs(ns) {
      if (!confirm(`Reset all tasks in namespace "${ns}"?`)) return;
      await api.resetNamespace(ns);
      this.$emit('refresh');
    },
    async deleteNs(ns) {
      if (!confirm(`Delete all tasks in namespace "${ns}"?`)) return;
      await api.deleteNamespace(ns);
      this.$emit('refresh');
    }
  },

  template: `
    <div class="tasks-container">

      <div v-if="filterNs" class="d-flex align-items-center mb-3">
        <router-link to="/namespaces" class="btn btn-xs btn-outline-light mr-3">
          <i class="fas fa-arrow-left mr-1"></i>Back
        </router-link>
        <span class="text-muted">
          Filtered by: <code style="color:#ffc107;">{{ filterNs }}</code>
        </span>
      </div>

      <div v-if="!Object.keys(byNamespace).length"
           class="text-center p-5 text-muted card card-outline">
        <i class="fas fa-filter fa-3x mb-3" style="color:#444;"></i>
        <p>No tasks found<span v-if="filterNs"> for namespace <b>{{ filterNs }}</b></span>.</p>
      </div>

      <div v-for="(taskList, ns) in byNamespace" :key="ns" class="card card-outline mb-4">
        <div class="card-header d-flex justify-content-between align-items-center">
          <h3 class="card-title">
            <i class="fas fa-layer-group mr-2 text-warning"></i>
            <span style="color:#eee; font-weight:600;">Namespace: </span>
            <router-link :to="'/tasks/' + ns"
                         style="color:#ffc107; font-size:1.1em; margin-left:5px;">
              {{ ns }}
            </router-link>
          </h3>
          <div class="btn-group">
            <button class="btn btn-xs btn-outline-warning mr-1" @click="resetNs(ns)">
              <i class="fas fa-history mr-1"></i>Reset All
            </button>
            <button class="btn btn-xs btn-outline-danger" @click="deleteNs(ns)">
              <i class="fas fa-trash-alt mr-1"></i>Delete All
            </button>
          </div>
        </div>
        <div class="card-body p-0">
          <div class="table-responsive">
            <table class="table table-sm table-hover mb-0">
              <thead>
                <tr>
                  <th style="width:35%; padding-left:15px;">Task ID</th>
                  <th style="width:25%">Params</th>
                  <th style="width:10%">Status</th>
                  <th style="width:20%">Last Update</th>
                  <th style="width:10%" class="text-right; padding-right:15px;">Actions</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="task in taskList" :key="task.id">
                  <td style="font-family:monospace; font-size:0.85em; padding-left:15px;">
                    <a href="#" @click.prevent="showLogs(task.id)" style="color:#00d4ff; font-weight:500;">
                      {{ task.id }}
                    </a>
                  </td>
                  <td class="text-muted" style="font-size:0.82em;">{{ task.params || '—' }}</td>
                  <td>
                    <span class="badge"
                          :style="'background:' + (STATUS_COLOR[task.status] || '#666') + '; color:#fff; font-size:0.75em; padding:0.35em 0.6em;'">
                      {{ task.status }}
                    </span>
                  </td>
                  <td class="text-muted" style="font-size:0.8em;">{{ task.update || '—' }}</td>
                  <td class="text-right" style="padding-right:15px;">
                    <div class="btn-group">
                      <button class="btn btn-xs btn-outline-warning" title="Reset"
                              @click="resetTask(task.id)">
                        <i class="fas fa-undo"></i>
                      </button>
                      <button class="btn btn-xs btn-outline-danger" title="Delete"
                              @click="deleteTask(task.id)">
                        <i class="fas fa-trash"></i>
                      </button>
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>

    </div>
  `
};
