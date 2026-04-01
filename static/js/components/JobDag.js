import { api } from '../api.js';

const { defineComponent, ref, onMounted, nextTick, watch } = Vue;

export default defineComponent({
  name: 'JobDag',

  // Sotto-componente per la riga ricorsiva della tabella
  components: {
    TaskRow: {
      name: 'TaskRow',
      props: ['task', 'allTasks', 'level', 'statusColors'],
      template: `
        <template v-if="task">
          <tr>
            <td style="font-family:monospace; font-size:0.85em;">
              <span style="color:#666; white-space:pre;">{{ '  '.repeat(level) }}{{ level > 0 ? '└─ ' : '' }}</span>
              <a href="#" @click.prevent="$root.logModalRef && $root.logModalRef.show(task.id)" style="color:#00d4ff;">
                {{ task.id }}
              </a>
            </td>
            <td>
              <span class="badge" :style="'background:' + (statusColors[task.status] || '#666') + '; color:#fff;'">
                {{ task.status }}
              </span>
            </td>
            <td class="text-muted" style="font-size:0.8em;">{{ task.params || '—' }}</td>
            <td class="text-muted" style="font-size:0.8em;">{{ task.last_update || '—' }}</td>
            <td>
              <button class="btn btn-xs btn-outline-warning mr-1" @click="$emit('reset', task.id)">
                <i class="fas fa-undo"></i>
              </button>
              <button class="btn btn-xs btn-outline-danger" @click="$emit('delete', task.id)">
                <i class="fas fa-trash"></i>
              </button>
            </td>
          </tr>
          <task-row 
            v-for="child in getChildren(task.id)" 
            :key="child.id" 
            :task="child" 
            :all-tasks="allTasks" 
            :level="level + 1"
            :status-colors="statusColors"
            @reset="$emit('reset', $event)"
            @delete="$emit('delete', $event)"
          ></task-row>
        </template>
      `,
      methods: {
        getChildren(tid) {
          return this.allTasks.filter(t => String(t.parent_id) === String(tid));
        }
      }
    }
  },

  setup() {
    const route = VueRouter.useRoute();
    const jobId = ref(decodeURIComponent(route.params.jobId));
    const tasks = ref([]);
    const loading = ref(false);
    const error = ref('');
    const svgRef = ref(null);

    const STATUS_COLOR = {
      SUCCESS: '#28a745',
      FAILED: '#dc3545',
      RUNNING: '#ffc107',
      READY: '#17a2b8',
      PENDING: '#6c757d',
    };

    async function loadTasks() {
      loading.value = true;
      try {
        const data = await api.jobTasks(jobId.value);
        tasks.value = data;
        await nextTick();
        renderDag();
      } catch (e) {
        error.value = `Error: ${e.message}`;
      } finally {
        loading.value = false;
      }
    }

    // Azioni sui task
    async function resetTask(id) {
      if (!confirm(`Reset task "${id}"?`)) return;
      await api.resetTask(id);
      loadTasks();
    }

    async function deleteTask(id) {
      if (!confirm(`Delete task "${id}"?`)) return;
      await api.deleteTask(id);
      loadTasks();
    }

    function renderDag() {
      if (!svgRef.value || !tasks.value.length) return;
      const d3 = window.d3;
      const el = svgRef.value;
      const W = el.clientWidth || 900;
      const H = el.clientHeight || 500;

      const svg = d3.select(el);
      svg.selectAll('*').remove();
      const mainGroup = svg.append('g');

      const nodeMap = {};
      const childrenMap = {};
      tasks.value.forEach(t => { 
        nodeMap[t.id] = t; 
        if (t.parent_id) {
          if (!childrenMap[t.parent_id]) childrenMap[t.parent_id] = [];
          childrenMap[t.parent_id].push(t.id);
        }
      });

      const roots = tasks.value.filter(t => !t.parent_id || !nodeMap[t.parent_id]);
      const levelArr = {};
      const order = {}; 
      let visitCount = 0;

      function traverse(id, l) {
        if (levelArr[id] !== undefined && levelArr[id] >= l) return;
        levelArr[id] = l;
        order[id] = visitCount++; 
        const children = (childrenMap[id] || []).sort();
        children.forEach(childId => traverse(childId, l + 1));
      }
      roots.forEach(r => traverse(r.id, 0));

      const byLevel = {};
      tasks.value.forEach(t => {
        const l = levelArr[t.id] || 0;
        if (!byLevel[l]) byLevel[l] = [];
        byLevel[l].push(t);
      });

      Object.values(byLevel).forEach(nodes => nodes.sort((a, b) => order[a.id] - order[b.id]));

      const levels = Object.keys(byLevel).map(Number).sort((a, b) => a - b);
      const levelH = Math.min(130, (H - 80) / Math.max(levels.length, 1));
      const nodeW = 180;
      const nodeH = 50;

      const pos = {};
      levels.forEach(l => {
        const nodes = byLevel[l];
        nodes.forEach((t, i) => {
          pos[t.id] = { x: (W / (nodes.length + 1)) * (i + 1), y: 50 + l * levelH };
        });
      });

      svg.attr('viewBox', `0 0 ${W} ${H}`);

      mainGroup.append('defs').append('marker')
        .attr('id', 'arrow').attr('viewBox', '0 -5 10 10').attr('refX', 10).attr('markerWidth', 5).attr('markerHeight', 5).attr('orient', 'auto')
        .append('path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#4b0082');

      const links = tasks.value.filter(t => t.parent_id && nodeMap[t.parent_id]);
      mainGroup.append('g').selectAll('path').data(links).enter().append('path')
        .attr('d', d => {
          const s = pos[d.parent_id], t = pos[d.id], mY = (s.y + nodeH + t.y) / 2;
          return `M${s.x},${s.y + nodeH} C${s.x},${mY} ${t.x},${mY} ${t.x},${t.y}`;
        })
        .attr('fill', 'none').attr('stroke', '#4b0082').attr('stroke-width', 1.5).attr('marker-end', 'url(#arrow)');

      const node = mainGroup.append('g').selectAll('g').data(tasks.value).enter().append('g')
        .attr('transform', t => `translate(${(pos[t.id]?.x || 0) - nodeW / 2},${(pos[t.id]?.y || 0)})`)
        .on('click', (ev, t) => window.__showLogs && window.__showLogs(t.id));

      node.append('rect').attr('width', nodeW).attr('height', nodeH).attr('rx', 6)
        .attr('fill', '#1a002e').attr('stroke', t => STATUS_COLOR[t.status] || '#4b0082').attr('stroke-width', 2);

      node.append('text').attr('x', nodeW / 2).attr('y', 22).attr('text-anchor', 'middle').attr('fill', '#00d4ff').attr('font-size', '10px').attr('font-family', 'monospace')
        .text(t => t.id.length > 22 ? t.id.slice(0, 20) + '…' : t.id);

      node.append('text').attr('x', nodeW / 2).attr('y', 38).attr('text-anchor', 'middle').attr('fill', t => STATUS_COLOR[t.status]).attr('font-size', '9px').attr('font-weight', 'bold')
        .text(t => t.status);

      svg.call(d3.zoom().scaleExtent([0.2, 3]).on('zoom', (e) => mainGroup.attr('transform', e.transform)));
    }

    onMounted(loadTasks);
    watch(() => route.params.jobId, (id) => { jobId.value = decodeURIComponent(id); loadTasks(); });

    return { jobId, tasks, loading, error, svgRef, loadTasks, STATUS_COLOR, resetTask, deleteTask };
  },

  computed: {
    rootTasks() {
      const ids = this.tasks.map(t => t.id);
      return this.tasks.filter(t => !t.parent_id || !ids.includes(t.parent_id));
    }
  },

  template: `
    <div>
      <div class="card card-outline mb-3">
        <div class="card-header d-flex justify-content-between align-items-center">
          <h3 class="card-title"><i class="fas fa-project-diagram mr-2"></i>Job: <code class="text-info">{{ jobId }}</code></h3>
          <div class="btn-group">
            <button class="btn btn-xs btn-outline-light" @click="loadTasks" :disabled="loading">
              <i class="fas fa-sync-alt" :class="{'fa-spin': loading}"></i>
            </button>
            <router-link to="/jobs" class="btn btn-xs btn-outline-secondary">Back</router-link>
          </div>
        </div>
      </div>

      <div v-if="error" class="alert alert-danger">{{ error }}</div>

      <div v-if="tasks.length">
        <div style="background:#0f001a; border:1px solid #4b0082; border-radius:8px; overflow:hidden; position:relative;">
          <svg ref="svgRef" style="width:100%; height:450px; display:block;"></svg>
          <div style="position:absolute; bottom:10px; left:10px; display:flex; gap:10px; pointer-events:none;">
            <span v-for="(color, status) in STATUS_COLOR" :key="status" style="font-size:0.7em; color:#aaa;">
              <i class="fas fa-circle" :style="'color:'+color"></i> {{ status }}
            </span>
          </div>
        </div>

        <div class="card card-outline mt-4">
          <div class="card-header"><h3 class="card-title"><i class="fas fa-sitemap mr-2"></i>Hierarchy & Details</h3></div>
          <div class="card-body p-0">
            <div class="table-responsive">
              <table class="table table-sm table-hover mb-0">
                <thead>
                  <tr><th>Task ID (Hierarchy)</th><th>Status</th><th>Params</th><th>Updated</th><th>Actions</th></tr>
                </thead>
                <tbody>
                  <task-row 
                    v-for="rt in rootTasks" 
                    :key="rt.id" 
                    :task="rt" 
                    :all-tasks="tasks" 
                    :level="0"
                    :status-colors="STATUS_COLOR"
                    @reset="resetTask"
                    @delete="deleteTask"
                  ></task-row>
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  `
});