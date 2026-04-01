// components/JobDag.js
import { api } from '../api.js';

const { defineComponent, ref, onMounted, onUnmounted, watch } = Vue;

export default defineComponent({
  name: 'JobDag',

  setup() {
    const route  = VueRouter.useRoute();
    const router = VueRouter.useRouter();
    const jobId  = ref(decodeURIComponent(route.params.jobId));
    const tasks  = ref([]);
    const loading = ref(false);
    const error   = ref('');
    const svgRef  = ref(null);
    let   _timer  = null;

    const STATUS_COLOR = {
      SUCCESS:  '#28a745',
      FAILED:   '#dc3545',
      RUNNING:  '#ffc107',
      READY:    '#17a2b8',
      PENDING:  '#6c757d',
    };

    async function loadTasks() {
      try {
        tasks.value = await api.jobTasks(jobId.value);
        renderDag();
      } catch(e) {
        error.value = `Error loading tasks: ${e.message}`;
      }
    }

    function renderDag() {
      if (!svgRef.value || !tasks.value.length) return;

      const d3 = window.d3;
      const el = svgRef.value;
      const W  = el.clientWidth  || 900;
      const H  = el.clientHeight || 600;

      d3.select(el).selectAll('*').remove();

      // Build node map
      const nodeMap = {};
      tasks.value.forEach(t => { nodeMap[t.id] = t; });

      // Build edges
      const links = tasks.value
        .filter(t => t.parent_id && nodeMap[t.parent_id])
        .map(t => ({ source: t.parent_id, target: t.id }));

      // Assign levels via BFS from roots
      const roots = tasks.value.filter(t => !t.parent_id || !nodeMap[t.parent_id]);
      const level = {};
      roots.forEach(r => { level[r.id] = 0; });
      const queue = [...roots.map(r => r.id)];
      while (queue.length) {
        const cur = queue.shift();
        tasks.value
          .filter(t => t.parent_id === cur)
          .forEach(child => {
            level[child.id] = (level[cur] || 0) + 1;
            queue.push(child.id);
          });
      }

      // Group by level
      const byLevel = {};
      tasks.value.forEach(t => {
        const l = level[t.id] || 0;
        if (!byLevel[l]) byLevel[l] = [];
        byLevel[l].push(t);
      });

      const levels   = Object.keys(byLevel).map(Number).sort((a,b) => a-b);
      const levelH   = Math.min(120, (H - 80) / Math.max(levels.length, 1));
      const nodeW    = 180;
      const nodeH    = 54;

      // Assign positions
      const pos = {};
      levels.forEach(l => {
        const nodes = byLevel[l];
        const total = nodes.length;
        nodes.forEach((t, i) => {
          pos[t.id] = {
            x: (W / (total + 1)) * (i + 1),
            y: 60 + l * levelH
          };
        });
      });

      const svg = d3.select(el)
        .attr('viewBox', `0 0 ${W} ${H}`)
        .attr('preserveAspectRatio', 'xMidYMid meet');

      // Arrow marker
      svg.append('defs').append('marker')
        .attr('id', 'arrow')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 10).attr('refY', 0)
        .attr('markerWidth', 6).attr('markerHeight', 6)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-5L10,0L0,5')
        .attr('fill', '#6f42c1');

      // Edges
      svg.append('g').selectAll('line')
        .data(links)
        .enter().append('line')
        .attr('x1', d => pos[d.source]?.x || 0)
        .attr('y1', d => (pos[d.source]?.y || 0) + nodeH)
        .attr('x2', d => pos[d.target]?.x || 0)
        .attr('y2', d => (pos[d.target]?.y || 0))
        .attr('stroke', '#4b0082')
        .attr('stroke-width', 2)
        .attr('marker-end', 'url(#arrow)');

      // Nodes
      const node = svg.append('g').selectAll('g')
        .data(tasks.value)
        .enter().append('g')
        .attr('transform', t => `translate(${(pos[t.id]?.x || 0) - nodeW/2},${(pos[t.id]?.y || 0)})`)
        .style('cursor', 'pointer')
        .on('click', (event, t) => {
          window.__showLogs && window.__showLogs(t.id);
        });

      // Node background
      node.append('rect')
        .attr('width', nodeW)
        .attr('height', nodeH)
        .attr('rx', 8)
        .attr('fill', '#2b0040')
        .attr('stroke', t => STATUS_COLOR[t.status] || '#4b0082')
        .attr('stroke-width', 2);

      // Status bar
      node.append('rect')
        .attr('width', nodeW)
        .attr('height', 6)
        .attr('rx', 8)
        .attr('fill', t => STATUS_COLOR[t.status] || '#4b0082');

      // Task ID
      node.append('text')
        .attr('x', nodeW / 2)
        .attr('y', 26)
        .attr('text-anchor', 'middle')
        .attr('fill', '#00d4ff')
        .attr('font-size', '11px')
        .attr('font-family', 'monospace')
        .text(t => t.id.length > 22 ? t.id.slice(0, 20) + '…' : t.id);

      // Status label
      node.append('text')
        .attr('x', nodeW / 2)
        .attr('y', 42)
        .attr('text-anchor', 'middle')
        .attr('fill', t => STATUS_COLOR[t.status] || '#aaa')
        .attr('font-size', '10px')
        .text(t => t.status);

      // Zoom + pan
      const zoom = d3.zoom()
        .scaleExtent([0.3, 3])
        .on('zoom', (event) => {
          svg.selectAll('g').attr('transform', event.transform);
        });
      d3.select(el).call(zoom);
    }

    onMounted(() => {
      loadTasks();
      _timer = setInterval(loadTasks, 5000);
    });

    onUnmounted(() => {
      clearInterval(_timer);
    });

    watch(() => route.params.jobId, (newId) => {
      jobId.value = decodeURIComponent(newId);
      loadTasks();
    });

    return { jobId, tasks, loading, error, svgRef, loadTasks };
  },

  template: `
    <div>
      <div class="card card-outline mb-3">
        <div class="card-header d-flex justify-content-between align-items-center">
          <h3 class="card-title">
            <i class="fas fa-project-diagram mr-2"></i>
            <span style="color:#aaa; font-size:0.85em;">Job:</span>
            <code style="color:#00d4ff; font-size:0.9em;">{{ jobId }}</code>
          </h3>
          <div>
            <button class="btn btn-xs btn-outline-light mr-2" @click="loadTasks">
              <i class="fas fa-sync-alt"></i>
            </button>
            <router-link to="/jobs" class="btn btn-xs btn-outline-secondary">
              <i class="fas fa-arrow-left mr-1"></i>Back
            </router-link>
          </div>
        </div>
      </div>

      <div v-if="error" class="alert alert-danger">{{ error }}</div>

      <div v-if="!tasks.length && !error" class="text-muted text-center mt-5">
        <i class="fas fa-spinner fa-spin mr-2"></i>Loading DAG...
      </div>

      <!-- Legend -->
      <div v-if="tasks.length" class="mb-2 d-flex flex-wrap" style="gap:12px;">
        <span v-for="(color, status) in {SUCCESS:'#28a745', FAILED:'#dc3545', RUNNING:'#ffc107', READY:'#17a2b8', PENDING:'#6c757d'}"
              :key="status" style="font-size:0.8em; display:flex; align-items:center; gap:5px;">
          <span :style="'display:inline-block;width:12px;height:12px;border-radius:3px;background:'+color"></span>
          {{ status }}
        </span>
        <span style="font-size:0.78em; color:#888; margin-left:auto;">
          Click a node to view logs &nbsp;•&nbsp; Scroll to zoom &nbsp;•&nbsp; Drag to pan
        </span>
      </div>

      <!-- DAG canvas -->
      <div v-if="tasks.length"
           style="background:#12001e; border:1px solid #4b0082; border-radius:8px; overflow:hidden;">
        <svg ref="svgRef" style="width:100%; height:600px; display:block;"></svg>
      </div>

      <!-- Task list below -->
      <div v-if="tasks.length" class="card card-outline mt-3">
        <div class="card-header">
          <h3 class="card-title"><i class="fas fa-tasks mr-2"></i>Tasks ({{ tasks.length }})</h3>
        </div>
        <div class="card-body p-0">
          <div class="table-responsive">
            <table class="table table-sm table-hover mb-0">
              <thead>
                <tr><th>Task ID</th><th>Status</th><th>Params</th><th>Last Update</th></tr>
              </thead>
              <tbody>
                <tr v-for="t in tasks" :key="t.id"
                    style="cursor:pointer;" @click="$root.logModalRef && $root.logModalRef.show(t.id)">
                  <td style="font-family:monospace; font-size:0.8em; color:#00d4ff;">{{ t.id }}</td>
                  <td>
                    <span :class="['badge', 'badge-'+t.status, t.status==='RUNNING'?'blink':'']">
                      {{ t.status }}
                    </span>
                  </td>
                  <td style="font-size:0.78em;">{{ t.params || '—' }}</td>
                  <td style="font-size:0.78em;">{{ t.last_update || '—' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  `
});
