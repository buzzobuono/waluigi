export default {
  name: 'DagChart',
  emits: ['show-logs', 'reset', 'delete', 'show-info'],
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

  setup(props, { emit }) {
    const svgRef       = Vue.ref(null);
    const containerRef = Vue.ref(null);
    const menuRef      = Vue.ref(null);
    const menu         = Vue.ref({ visible: false, x: 0, y: 0, task: null });
    function openMenu(clientX, clientY, t) {
      const cRect = containerRef.value.getBoundingClientRect();
      // Use actual menu dimensions if already rendered, otherwise approximate
      const mw = menuRef.value ? menuRef.value.offsetWidth  || 185 : 185;
      const mh = menuRef.value ? menuRef.value.offsetHeight || 155 : 155;
      const cw = cRect.width;
      const ch = cRect.height;
      let x = clientX - cRect.left;
      let y = clientY - cRect.top;
      // Prefer opening to the left of the click (button is on node's right edge)
      if (x + mw > cw) x = Math.max(4, x - mw);
      if (y + mh > ch) y = Math.max(4, ch - mh - 4);
      menu.value = { visible: true, x, y, task: t };
    }

    function closeMenu() { menu.value.visible = false; }

    function showInfo(t) { emit('show-info', t); closeMenu(); }

    function fitHeight() {
      if (!containerRef.value) return;
      const top = containerRef.value.getBoundingClientRect().top + window.scrollY;
      containerRef.value.style.height = `calc(100vh - ${Math.round(top) + 12}px)`;
    }

    Vue.onMounted(() => { Vue.nextTick(fitHeight); window.addEventListener('resize', fitHeight); });
    Vue.onUnmounted(() => { window.removeEventListener('resize', fitHeight); });

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

      const btnW   = 22;
      const moreBtn = node.append('g')
        .style('cursor', 'pointer')
        .on('click',       (ev, t) => { ev.stopPropagation(); openMenu(ev.clientX, ev.clientY, t); })
        .on('contextmenu', (ev, t) => { ev.preventDefault(); ev.stopPropagation(); openMenu(ev.clientX, ev.clientY, t); })
        .on('touchstart',  (ev, t) => { ev.stopPropagation(); const touch = ev.touches[0]; openMenu(touch.clientX, touch.clientY, t); });

      moreBtn.append('rect')
        .attr('class', 'more-bg')
        .attr('x', nodeW - btnW).attr('y', 0)
        .attr('width', btnW).attr('height', nodeH).attr('rx', 4)
        .attr('fill', 'transparent')
        .on('mouseenter', function() { d3.select(this).attr('fill', 'rgba(0,0,0,0.07)'); })
        .on('mouseleave', function() { d3.select(this).attr('fill', 'transparent'); });

      moreBtn.append('text')
        .attr('x', nodeW - btnW / 2).attr('y', nodeH / 2)
        .attr('text-anchor', 'middle').attr('dominant-baseline', 'middle')
        .attr('fill', '#adb5bd')
        .attr('font-size', '15px').attr('font-weight', 'bold')
        .attr('pointer-events', 'none')
        .text('⋮');

      const zoom = d3.zoom().scaleExtent([0.05, 4]).on('zoom', e => g.attr('transform', e.transform));
      svg.call(zoom);
      // Initial scale: fit exactly 5 nodes + gaps + padding vertically; align left
      const ch = containerRef.value.offsetHeight;
      const k  = ch / (5 * nodeH + 4 * gapY + 2 * padY);
      svg.call(zoom.transform, d3.zoomIdentity.scale(k));
    };

    Vue.onMounted(renderDag);
    Vue.watch(() => props.tasks, () => Vue.nextTick(renderDag), { deep: true });

    return { svgRef, containerRef, menuRef, menu, closeMenu, showInfo };
  },

  template: `
    <div ref="containerRef"
         @click="closeMenu"
         style="background:#f4f6f9; border:1px solid #007bff; border-top:3px solid #007bff; border-radius:4px; overflow:hidden; position:relative; user-select:none; min-height:300px;">

      <svg ref="svgRef" style="width:100%; height:100%; display:block;"></svg>

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
