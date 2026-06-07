import { fmtDt } from '../utils.js';

export default {
  name: 'DagChart',
  emits: ['show-logs', 'reset', 'delete'],
  props: {
    tasks: { type: Array, required: true },
    colors: {
      type: Object,
      default: () => ({
        SUCCESS: '#28a745',
        FAILED:  '#dc3545',
        RUNNING: '#ffc107',
        READY:   '#17a2b8',
        PENDING: '#6c757d'
      })
    }
  },

  setup(props) {
    const svgRef       = Vue.ref(null);
    const containerRef = Vue.ref(null);
    const menuRef      = Vue.ref(null);
    const menu         = Vue.ref({ visible: false, x: 0, y: 0, task: null });
    const infoTask     = Vue.ref(null);
    let longPressTimer = null;
    let touchOrigin    = null;

    function parseKV(str) {
      if (!str) return {};
      try {
        const v = JSON.parse(str);
        if (v && typeof v === 'object' && !Array.isArray(v)) return v;
      } catch {}
      return str ? { value: str } : {};
    }

    function openMenu(clientX, clientY, t) {
      const rect = containerRef.value.getBoundingClientRect();
      menu.value = { visible: true, x: clientX - rect.left, y: clientY - rect.top, task: t };
    }

    function closeMenu() { menu.value.visible = false; }

    function showInfo(t) { infoTask.value = t; closeMenu(); }

    function fitHeight() {
      if (!containerRef.value) return;
      const top = containerRef.value.getBoundingClientRect().top + window.scrollY;
      containerRef.value.style.height = `calc(100vh - ${Math.round(top) + 12}px)`;
    }

    Vue.onMounted(() => { Vue.nextTick(fitHeight); window.addEventListener('resize', fitHeight); });
    Vue.onUnmounted(() => { window.removeEventListener('resize', fitHeight); });

    Vue.watch(() => menu.value.visible, (v) => {
      if (!v) return;
      Vue.nextTick(() => {
        if (!menuRef.value || !containerRef.value) return;
        const mw = menuRef.value.offsetWidth;
        const mh = menuRef.value.offsetHeight;
        const cw = containerRef.value.offsetWidth;
        const ch = containerRef.value.offsetHeight;
        if (menu.value.x + mw > cw) menu.value.x = Math.max(4, cw - mw - 4);
        if (menu.value.y + mh > ch) menu.value.y = Math.max(4, ch - mh - 4);
      });
    });

    const renderDag = () => {
      if (!svgRef.value || !props.tasks.length) return;
      const d3  = window.d3;
      const el  = svgRef.value;

      const nodeW  = 190;
      const nodeH  = 58;
      const padX   = 40;
      const padY   = 40;
      const gapX   = 70;
      const gapY   = 24;
      const levelW = nodeW + gapX;

      const svg = d3.select(el);
      svg.selectAll('*').remove();
      const g = svg.append('g');

      const nodeMap    = {};
      const childrenOf = {};
      props.tasks.forEach(t => {
        nodeMap[t.id] = t;
        if (t.parent_id) {
          if (!childrenOf[t.parent_id]) childrenOf[t.parent_id] = [];
          childrenOf[t.parent_id].push(t.id);
        }
      });

      const level = {};
      const visitOrder = {};
      let seq = 0;
      const roots = props.tasks.filter(t => !t.parent_id || !nodeMap[t.parent_id]);

      const traverse = (id, l) => {
        if (level[id] !== undefined && level[id] >= l) return;
        level[id]      = l;
        visitOrder[id] = seq++;
        (childrenOf[id] || []).sort().forEach(cid => traverse(cid, l + 1));
      };
      roots.forEach(r => traverse(r.id, 0));

      const byLevel = {};
      props.tasks.forEach(t => {
        const l = level[t.id] ?? 0;
        if (!byLevel[l]) byLevel[l] = [];
        byLevel[l].push(t);
      });
      Object.values(byLevel).forEach(arr => arr.sort((a, b) => visitOrder[a.id] - visitOrder[b.id]));

      const levels     = Object.keys(byLevel).map(Number).sort((a, b) => a - b);
      const maxLevel   = Math.max(...levels);
      const maxColSize = Math.max(...Object.values(byLevel).map(a => a.length));

      const totalW = padX * 2 + nodeW + maxLevel * levelW;
      const totalH = padY * 2 + maxColSize * nodeH + (maxColSize - 1) * gapY;

      svg.attr('viewBox', `0 0 ${totalW} ${totalH}`)
         .attr('preserveAspectRatio', 'xMidYMid meet');

      const pos = {};
      levels.forEach(l => {
        const nodes  = byLevel[l];
        const cx     = padX + (maxLevel - l) * levelW + nodeW / 2;
        const colH   = nodes.length * nodeH + (nodes.length - 1) * gapY;
        const startY = (totalH - colH) / 2;
        nodes.forEach((t, i) => {
          pos[t.id] = { x: cx, y: startY + i * (nodeH + gapY) + nodeH / 2 };
        });
      });

      g.append('defs').append('marker')
        .attr('id', 'arrowR')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 10).attr('refY', 0)
        .attr('markerWidth', 6).attr('markerHeight', 6)
        .attr('orient', 'auto')
        .append('path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#007bff');

      const links = props.tasks.filter(t => t.parent_id && nodeMap[t.parent_id]);
      g.append('g').selectAll('path').data(links).enter().append('path')
        .attr('d', d => {
          const cp = pos[d.parent_id];
          const cc = pos[d.id];
          const x1 = cc.x + nodeW / 2;
          const y1 = cc.y;
          const x2 = cp.x - nodeW / 2;
          const y2 = cp.y;
          const mx = (x1 + x2) / 2;
          return `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`;
        })
        .attr('fill', 'none')
        .attr('stroke', '#007bff')
        .attr('stroke-width', 1.5)
        .attr('marker-end', 'url(#arrowR)');

      const node = g.append('g').selectAll('g').data(props.tasks).enter().append('g')
        .attr('transform', t => {
          const p = pos[t.id] || { x: 0, y: 0 };
          return `translate(${p.x - nodeW / 2},${p.y - nodeH / 2})`;
        })
        .style('cursor', 'pointer');

      node
        .on('click', (ev, t) => { ev.stopPropagation(); openMenu(ev.clientX, ev.clientY, t); })
        .on('contextmenu', (ev, t) => { ev.preventDefault(); ev.stopPropagation(); openMenu(ev.clientX, ev.clientY, t); });

      node
        .on('touchstart', (ev, t) => {
          ev.stopPropagation();
          const touch = ev.touches[0];
          touchOrigin = { x: touch.clientX, y: touch.clientY };
          longPressTimer = setTimeout(() => { openMenu(touch.clientX, touch.clientY, t); touchOrigin = null; }, 500);
        })
        .on('touchend',  ()    => { clearTimeout(longPressTimer); touchOrigin = null; })
        .on('touchmove', (ev)  => {
          if (!touchOrigin) return;
          const t = ev.touches[0];
          if (Math.hypot(t.clientX - touchOrigin.x, t.clientY - touchOrigin.y) > 8) {
            clearTimeout(longPressTimer); touchOrigin = null;
          }
        });

      node.append('rect')
        .attr('width', nodeW).attr('height', nodeH).attr('rx', 4)
        .attr('fill', '#ffffff')
        .attr('stroke', t => props.colors[t.status] || '#6c757d')
        .attr('stroke-width', 2);

      node.append('rect')
        .attr('width', 4).attr('height', nodeH).attr('rx', 2)
        .attr('fill', t => props.colors[t.status] || '#6c757d');

      node.append('text')
        .attr('x', nodeW / 2 + 2).attr('y', 23)
        .attr('text-anchor', 'middle')
        .attr('fill', '#343a40')
        .attr('font-size', '11px').attr('font-weight', '600')
        .attr('font-family', '"Source Sans Pro", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif')
        .text(t => t.id.length > 24 ? t.id.slice(0, 22) + '…' : t.id);

      node.append('text')
        .attr('x', nodeW / 2 + 2).attr('y', 42)
        .attr('text-anchor', 'middle')
        .attr('fill', t => props.colors[t.status] || '#6c757d')
        .attr('font-size', '10px').attr('font-weight', 'bold')
        .text(t => t.status);

      svg.call(d3.zoom().scaleExtent([0.2, 4]).on('zoom', e => g.attr('transform', e.transform)));
    };

    Vue.onMounted(renderDag);
    Vue.watch(() => props.tasks, () => Vue.nextTick(renderDag), { deep: true });

    return { svgRef, containerRef, menuRef, menu, infoTask, closeMenu, showInfo, parseKV, fmtDt };
  },

  template: `
    <div ref="containerRef"
         @click="closeMenu"
         style="background:#f4f6f9; border:1px solid #007bff; border-top:3px solid #007bff; border-radius:4px; overflow:hidden; position:relative; user-select:none; min-height:300px;">

      <svg ref="svgRef" style="width:100%; height:100%; display:block;"></svg>

      <!-- Hint -->
      <div style="position:absolute; bottom:10px; right:10px; pointer-events:none; background:rgba(255,255,255,0.85); padding:3px 8px; border-radius:4px; border:1px solid #dee2e6; font-size:0.72rem; color:#888;">
        <i class="fas fa-hand-pointer mr-1"></i>tap node for actions
      </div>

      <!-- Info panel -->
      <div v-if="infoTask"
           @click.stop
           style="position:absolute; top:10px; right:10px; width:260px; max-height:65%; display:flex; flex-direction:column; background:#fff; border:1px solid rgba(0,0,0,.15); border-radius:4px; box-shadow:0 6px 16px rgba(0,0,0,.18); z-index:1040; overflow:hidden;">

        <!-- Header -->
        <div style="flex-shrink:0; padding:8px 10px 6px; background:#f8f9fa; border-bottom:1px solid #dee2e6; display:flex; align-items:flex-start; justify-content:space-between; gap:6px;">
          <div style="min-width:0;">
            <div style="font-size:0.78rem; font-weight:700; color:#343a40; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">
              <i class="fas fa-project-diagram mr-1 text-primary"></i>{{ infoTask.id }}
            </div>
            <div style="margin-top:3px; display:flex; align-items:center; gap:6px;">
              <span class="badge" :style="{ background: colors[infoTask.status]||'#6c757d', color:'#fff', fontSize:'0.68rem' }">
                {{ infoTask.status }}
              </span>
              <span style="font-size:0.72rem; color:#888;">{{ fmtDt(infoTask.last_update) }}</span>
            </div>
          </div>
          <button @click="infoTask = null"
                  style="flex-shrink:0; background:none; border:none; font-size:1.1rem; line-height:1; color:#6c757d; cursor:pointer; padding:0 2px;">&times;</button>
        </div>

        <!-- Scrollable body -->
        <div style="overflow-y:auto; flex:1;">

          <!-- Params -->
          <template v-if="Object.keys(parseKV(infoTask.params)).length">
            <div style="padding:6px 10px 2px; font-size:0.68rem; font-weight:700; text-transform:uppercase; letter-spacing:.04em; color:#6c757d;">
              Parameters
            </div>
            <table style="width:100%; border-collapse:collapse; font-size:0.78rem; padding-bottom:4px;">
              <tr v-for="(v, k) in parseKV(infoTask.params)" :key="k"
                  style="border-bottom:1px solid #f0f0f0;">
                <td style="padding:3px 6px 3px 10px; color:#6c757d; white-space:nowrap; width:40%;">{{ k }}</td>
                <td style="padding:3px 10px 3px 4px; word-break:break-all; color:#343a40;">{{ v }}</td>
              </tr>
            </table>
          </template>

          <!-- Attributes -->
          <template v-if="Object.keys(parseKV(infoTask.attributes)).length">
            <div style="padding:6px 10px 2px; font-size:0.68rem; font-weight:700; text-transform:uppercase; letter-spacing:.04em; color:#6c757d; border-top:1px solid #dee2e6;">
              Attributes
            </div>
            <table style="width:100%; border-collapse:collapse; font-size:0.78rem; padding-bottom:4px;">
              <tr v-for="(v, k) in parseKV(infoTask.attributes)" :key="k"
                  style="border-bottom:1px solid #f0f0f0;">
                <td style="padding:3px 6px 3px 10px; color:#6c757d; white-space:nowrap; width:40%;">{{ k }}</td>
                <td style="padding:3px 10px 3px 4px; word-break:break-all; color:#343a40;">{{ v }}</td>
              </tr>
            </table>
          </template>

        </div>
      </div>

      <!-- Context menu -->
      <div v-if="menu.visible"
           ref="menuRef"
           @click.stop
           :style="{
             position:     'absolute',
             left:         menu.x + 'px',
             top:          menu.y + 'px',
             zIndex:       1050,
             background:   '#fff',
             border:       '1px solid rgba(0,0,0,.15)',
             borderRadius: '4px',
             boxShadow:    '0 6px 16px rgba(0,0,0,.18)',
             minWidth:     '175px',
             overflow:     'hidden',
           }">

        <div style="padding:8px 12px 6px; background:#f8f9fa; border-bottom:1px solid #dee2e6;">
          <div style="font-size:0.75rem; font-weight:700; color:#343a40; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:190px;">
            <i class="fas fa-project-diagram mr-1 text-primary"></i>{{ menu.task && menu.task.id }}
          </div>
          <div v-if="menu.task" style="margin-top:2px;">
            <span class="badge badge-sm" :style="{ background: colors[menu.task.status] || '#6c757d', color:'#fff', fontSize:'0.68rem' }">
              {{ menu.task.status }}
            </span>
          </div>
        </div>

        <div style="padding:3px 0;">
          <button class="dropdown-item d-flex align-items-center"
                  style="gap:8px; font-size:0.83rem; padding:7px 14px;"
                  @click="showInfo(menu.task)">
            <i class="fas fa-info-circle text-secondary" style="width:14px; text-align:center;"></i> Info
          </button>
          <button class="dropdown-item d-flex align-items-center"
                  style="gap:8px; font-size:0.83rem; padding:7px 14px;"
                  @click="$emit('show-logs', menu.task.id); closeMenu()">
            <i class="fas fa-terminal text-info" style="width:14px; text-align:center;"></i> View Logs
          </button>
          <button class="dropdown-item d-flex align-items-center"
                  style="gap:8px; font-size:0.83rem; padding:7px 14px;"
                  @click="$emit('reset', menu.task.id); closeMenu()">
            <i class="fas fa-undo text-warning" style="width:14px; text-align:center;"></i> Reset
          </button>
          <div class="dropdown-divider" style="margin:2px 0;"></div>
          <button class="dropdown-item text-danger d-flex align-items-center"
                  style="gap:8px; font-size:0.83rem; padding:7px 14px;"
                  @click="$emit('delete', menu.task.id); closeMenu()">
            <i class="fas fa-trash" style="width:14px; text-align:center;"></i> Delete
          </button>
        </div>

      </div>
    </div>
  `
};
