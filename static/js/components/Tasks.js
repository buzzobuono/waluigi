import { api } from '../api.js';

export default {
  name: 'Tasks',
  props: { tasks: Array },
  emits: ['refresh'],
  inject: ['showLogs'],

  setup() {
    const STATUS_COLOR = {
      SUCCESS: '#28a745',
      FAILED: '#dc3545',
      RUNNING: '#ffc107',
      READY: '#17a2b8',
      PENDING: '#6c757d',
    };
    return { STATUS_COLOR };
  },

  computed: {
    byNamespace() {
      const map = {};
      (this.tasks || []).forEach(t => {
        const ns = t.namespace || '(none)';
        if (!map[ns]) map[ns] = {};
        map[ns][t.id] = {
          id: t.id,
          params: t.params,
          status: t.status,
          update: t.last_update,
          parent: t.parent_id
        };
      });
      return map;
    }
  },

  methods: {
    roots(tasks) {
      return Object.keys(tasks).filter(tid =>
        !tasks[tid].parent ||
        tasks[tid].parent === 'None' ||
        !(tasks[tid].parent in tasks)
      );
    },
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

  components: {
    TaskRow: {
      name: 'TaskRow',
      props: ['tid', 'tasks', 'level', 'statusColors'],
      inject: ['showLogs'],
      computed: {
        task() { return this.tasks[this.tid]; },
        children() {
          return Object.keys(this.tasks).filter(cid =>
            String(this.tasks[cid].parent) === String(this.tid)
          );
        }
      },
      template: `
        <template v-if="task">
          <tr>
            <td style="font-family:monospace; font-size:0.85em;">
              <span style="color:#666; white-space:pre;">{{ '  '.repeat(level) }}{{ level > 0 ? '└─ ' : '' }}</span>
              <a href="#" @click.prevent="showLogs(task.id)" style="color:#00d4ff; font-weight:500;">
                {{ task.id }}
              </a>
            </td>
            <td class="text-muted" style="font-size:0.8em;">{{ task.params || '—' }}</td>
            <td>
              <span class="badge" :style="'background:' + (statusColors[task.status] || '#666') + '; color:#fff;'">
                {{ task.status }}
              </span>
            </td>
            <td class="text-muted" style="font-size:0.8em;">{{ task.update || '—' }}</td>
            <td>
              <div class="btn-group">
                <button class="btn btn-xs btn-outline-warning" title="Reset" @click="$emit('reset', task.id)">
                  <i class="fas fa-undo"></i>
                </button>
                <button class="btn btn-xs btn-outline-danger" title="Delete" @click="$emit('delete', task.id)">
                  <i class="fas fa-trash"></i>
                </button>
              </div>
            </td>
          </tr>
          <task-row
            v-for="cid in children" :key="cid"
            :tid="cid" :tasks="tasks" :level="level + 1"
            :status-colors="statusColors"
            @reset="$emit('reset', $event)"
            @delete="$emit('delete', $event)"
          ></task-row>
        </template>
      `
    }
  },

  template: `
    <div class="tasks-container">
      <div v-if="!Object.keys(byNamespace).length" class="text-center p-5 text-muted">
        <i class="fas fa-inbox fa-3x mb-3"></i>
        <p>No tasks found in this view.</p>
      </div>

      <div v-for="(tasks, ns) in byNamespace" :key="ns" class="card card-outline mb-4">
        <div class="card-header d-flex justify-content-between align-items-center">
          <h3 class="card-title">
            <i class="fas fa-layer-group mr-2 text-warning"></i>
            <span style="color:#eee; font-weight:600;">Namespace: </span>
            <code style="color:#ffc107; font-size:1.1em; margin-left:5px;">{{ ns }}</code>
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
                  <th style="width: 35%">Task ID</th>
                  <th style="width: 25%">Params</th>
                  <th style="width: 10%">Status</th>
                  <th style="width: 20%">Last Update</th>
                  <th style="width: 10%">Actions</th>
                </tr>
              </thead>
              <tbody>
                <task-row
                  v-for="rid in roots(tasks)" :key="rid"
                  :tid="rid" :tasks="tasks" :level="0"
                  :status-colors="STATUS_COLOR"
                  @reset="resetTask($event)"
                  @delete="deleteTask($event)"
                ></task-row>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  `
};