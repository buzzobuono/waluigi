// components/Tasks.js
import { api } from '../api.js';

export default {
  name: 'Tasks',
  props: { tasks: Array },
  emits: ['refresh'],
  inject: ['showLogs'],
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
    children(tid, tasks) {
      return Object.keys(tasks).filter(cid =>
        String(tasks[cid].parent) === String(tid)
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
    },
  },
  // recursive task-row sub-component defined inline
  components: {
    TaskRow: {
      name: 'TaskRow',
      props: { tid: String, tasks: Object, level: { type: Number, default: 0 } },
      emits: ['reset', 'delete'],
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
            <td style="font-size:0.8em;">
              <span class="tree-indent">{{ '\u00a0'.repeat(level * 4) }}{{ level > 0 ? '└─ ' : '' }}</span>
              <a href="#" @click.prevent="showLogs(task.id)" style="color:#00d4ff;">{{ task.id }}</a>
            </td>
            <td style="font-size:0.78em;">{{ task.params || '—' }}</td>
            <td>
              <span :class="['badge', 'badge-'+task.status, task.status==='RUNNING'?'blink':'']">
                {{ task.status }}
              </span>
            </td>
            <td style="font-size:0.78em;">{{ task.update || '—' }}</td>
            <td>
              <button class="btn btn-xs btn-outline-warning mr-1" @click="$emit('reset', task.id)">Reset</button>
              <button class="btn btn-xs btn-outline-danger"       @click="$emit('delete', task.id)">Delete</button>
            </td>
          </tr>
          <task-row
            v-for="cid in children" :key="cid"
            :tid="cid" :tasks="tasks" :level="level + 1"
            @reset="$emit('reset', $event)"
            @delete="$emit('delete', $event)"
          ></task-row>
        </template>
      `
    }
  },
  template: `
    <div>
      <p v-if="!Object.keys(byNamespace).length" class="text-muted mt-3">No tasks found.</p>

      <div v-for="(tasks, ns) in byNamespace" :key="ns" class="card card-outline mb-3">
        <div class="card-header d-flex justify-content-between align-items-center">
          <h3 class="card-title">
            <i class="fas fa-layer-group mr-2"></i>
            <span class="ns-header-yellow">📦 {{ ns }}</span>
          </h3>
          <div>
            <button class="btn btn-xs btn-outline-warning mr-1" @click="resetNs(ns)">Reset</button>
            <button class="btn btn-xs btn-outline-danger"       @click="deleteNs(ns)">Delete</button>
          </div>
        </div>
        <div class="card-body p-0">
          <div class="table-responsive">
            <table class="table table-sm table-hover mb-0">
              <thead>
                <tr>
                  <th>Task ID</th>
                  <th>Params</th>
                  <th>Status</th>
                  <th>Last Update</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                <task-row
                  v-for="rid in roots(tasks)" :key="rid"
                  :tid="rid" :tasks="tasks" :level="0"
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
