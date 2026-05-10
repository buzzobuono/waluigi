export default {
  name: 'DagChart',
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
    const svgRef = Vue.ref(null);

    const renderDag = () => {
      if (!svgRef.value || !props.tasks.length) return;
      const d3   = window.d3;
      const el   = svgRef.value;

      const nodeW  = 190;
      const nodeH  = 58;
      const padX   = 40;   // left/right padding
      const padY   = 40;   // top/bottom padding
      const gapX   = 70;   // horizontal gap between columns
      const gapY   = 24;   // vertical gap between nodes in the same column
      const levelW = nodeW + gapX;

      const svg = d3.select(el);
      svg.selectAll('*').remove();
      const g = svg.append('g');

      // Build adjacency maps
      const nodeMap    = {};
      const childrenOf = {};
      props.tasks.forEach(t => {
        nodeMap[t.id] = t;
        if (t.parent_id) {
          if (!childrenOf[t.parent_id]) childrenOf[t.parent_id] = [];
          childrenOf[t.parent_id].push(t.id);
        }
      });

      // BFS from roots to assign depth levels (root = 0)
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

      // Group by level
      const byLevel = {};
      props.tasks.forEach(t => {
        const l = level[t.id] ?? 0;
        if (!byLevel[l]) byLevel[l] = [];
        byLevel[l].push(t);
      });
      Object.values(byLevel).forEach(arr => arr.sort((a, b) => visitOrder[a.id] - visitOrder[b.id]));

      const levels        = Object.keys(byLevel).map(Number).sort((a, b) => a - b);
      const maxLevel      = Math.max(...levels);
      const maxColSize    = Math.max(...Object.values(byLevel).map(a => a.length));

      // SVG canvas size
      const totalW = padX * 2 + nodeW + maxLevel * levelW;
      const totalH = padY * 2 + maxColSize * nodeH + (maxColSize - 1) * gapY;

      svg.attr('viewBox', `0 0 ${totalW} ${totalH}`)
         .style('height', Math.max(200, totalH) + 'px');

      // Position: level 0 (root) rightmost, maxLevel (leaves) leftmost
      // Each level forms a vertical column; nodes in the column are centered
      const pos = {};
      levels.forEach(l => {
        const nodes  = byLevel[l];
        const cx     = padX + (maxLevel - l) * levelW + nodeW / 2;
        const colH   = nodes.length * nodeH + (nodes.length - 1) * gapY;
        const startY = (totalH - colH) / 2;
        nodes.forEach((t, i) => {
          pos[t.id] = {
            x: cx,
            y: startY + i * (nodeH + gapY) + nodeH / 2  // vertical center
          };
        });
      });

      // Arrow marker (points right →)
      g.append('defs').append('marker')
        .attr('id', 'arrowR')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 10).attr('refY', 0)
        .attr('markerWidth', 6).attr('markerHeight', 6)
        .attr('orient', 'auto')
        .append('path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#007bff');

      // Links: child (left) right-edge → parent (right) left-edge
      // Drawn left-to-right to show execution flow (leaf runs first → root runs last)
      const links = props.tasks.filter(t => t.parent_id && nodeMap[t.parent_id]);
      g.append('g').selectAll('path').data(links).enter().append('path')
        .attr('d', d => {
          const cp = pos[d.parent_id];
          const cc = pos[d.id];
          const x1 = cc.x + nodeW / 2;        // child right edge
          const y1 = cc.y;
          const x2 = cp.x - nodeW / 2;        // parent left edge
          const y2 = cp.y;
          const mx = (x1 + x2) / 2;
          return `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`;
        })
        .attr('fill', 'none')
        .attr('stroke', '#007bff')
        .attr('stroke-width', 1.5)
        .attr('marker-end', 'url(#arrowR)');

      // Nodes
      const node = g.append('g').selectAll('g').data(props.tasks).enter().append('g')
        .attr('transform', t => {
          const p = pos[t.id] || { x: 0, y: 0 };
          return `translate(${p.x - nodeW / 2},${p.y - nodeH / 2})`;
        })
        .style('cursor', 'pointer')
        .on('click', (ev, t) => window.__showLogs && window.__showLogs(t.id));

      // Node background
      node.append('rect')
        .attr('width', nodeW).attr('height', nodeH).attr('rx', 4)
        .attr('fill', '#ffffff')
        .attr('stroke', t => props.colors[t.status] || '#6c757d')
        .attr('stroke-width', 2);

      // Status accent bar (left side)
      node.append('rect')
        .attr('width', 4).attr('height', nodeH)
        .attr('rx', 2)
        .attr('fill', t => props.colors[t.status] || '#6c757d');

      // Task id label
      node.append('text')
        .attr('x', nodeW / 2 + 2).attr('y', 23)
        .attr('text-anchor', 'middle')
        .attr('fill', '#343a40')
        .attr('font-size', '11px').attr('font-weight', '600')
        .attr('font-family', '"Source Sans Pro", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif')
        .text(t => t.id.length > 24 ? t.id.slice(0, 22) + '…' : t.id);

      // Status label
      node.append('text')
        .attr('x', nodeW / 2 + 2).attr('y', 42)
        .attr('text-anchor', 'middle')
        .attr('fill', t => props.colors[t.status] || '#6c757d')
        .attr('font-size', '10px').attr('font-weight', 'bold')
        .text(t => t.status);

      // Zoom/pan
      svg.call(d3.zoom().scaleExtent([0.2, 4]).on('zoom', e => g.attr('transform', e.transform)));
    };

    Vue.onMounted(renderDag);
    Vue.watch(() => props.tasks, () => Vue.nextTick(renderDag), { deep: true });

    return { svgRef };
  },

  template: `
    <div style="background:#f4f6f9; border:1px solid #007bff; border-top:3px solid #007bff; border-radius:4px; overflow:hidden; position:relative;">
      <svg ref="svgRef" style="width:100%; display:block;"></svg>
      <div style="position:absolute; bottom:10px; left:10px; display:flex; gap:12px; pointer-events:none; background:rgba(255,255,255,0.9); padding:5px 10px; border-radius:4px; border:1px solid #dee2e6;">
        <span v-for="(color, status) in colors" :key="status" style="font-size:0.75rem; color:#495057; font-weight:600;">
          <i class="fas fa-circle" :style="'color:'+color"></i> {{ status }}
        </span>
      </div>
    </div>
  `
};
