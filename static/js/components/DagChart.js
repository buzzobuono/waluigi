export default {
  name: 'DagChart',
  props: {
    tasks: { type: Array, required: true },
    colors: { 
      type: Object, 
      default: () => ({
        success: '#28a745',
        running: '#17a2b8',
        pending: '#ffc107',
        failed: '#dc3545',
        skipped: '#6c757d'
      })
    }
  },
  setup(props) {
    const svgRef = Vue.ref(null);

    const renderDag = () => {
      if (!svgRef.value || !props.tasks.length) return;
      const d3 = window.d3;
      const el = svgRef.value;
      const W = el.clientWidth || 900;
      const H = el.clientHeight || 500;

      const svg = d3.select(el);
      svg.selectAll('*').remove();
      const mainGroup = svg.append('g');

      const nodeMap = {};
      const childrenMap = {};
      props.tasks.forEach(t => { 
        nodeMap[t.id] = t; 
        if (t.parent_id) {
          if (!childrenMap[t.parent_id]) childrenMap[t.parent_id] = [];
          childrenMap[t.parent_id].push(t.id);
        }
      });

      const roots = props.tasks.filter(t => !t.parent_id || !nodeMap[t.parent_id]);
      const levelArr = {};
      const order = {}; 
      let visitCount = 0;

      const traverse = (id, l) => {
        if (levelArr[id] !== undefined && levelArr[id] >= l) return;
        levelArr[id] = l;
        order[id] = visitCount++; 
        const children = (childrenMap[id] || []).sort();
        children.forEach(childId => traverse(childId, l + 1));
      };
      roots.forEach(r => traverse(r.id, 0));

      const byLevel = {};
      props.tasks.forEach(t => {
        const l = levelArr[t.id] || 0;
        if (!byLevel[l]) byLevel[l] = [];
        byLevel[l].push(t);
      });

      Object.values(byLevel).forEach(nodes => nodes.sort((a, b) => order[a.id] - order[b.id]));
      const levels = Object.keys(byLevel).map(Number).sort((a, b) => a - b);
      const levelH = Math.min(130, (H - 80) / Math.max(levels.length, 1));
      const nodeW = 180;
      const nodeH = 55;

      const pos = {};
      levels.forEach(l => {
        const nodes = byLevel[l];
        nodes.forEach((t, i) => {
          pos[t.id] = { x: (W / (nodes.length + 1)) * (i + 1), y: 50 + l * levelH };
        });
      });

      svg.attr('viewBox', `0 0 ${W} ${H}`);

      mainGroup.append('defs').append('marker')
        .attr('id', 'arrow').attr('viewBox', '0 -5 10 10').attr('refX', 10).attr('markerWidth', 6).attr('markerHeight', 6).attr('orient', 'auto')
        .append('path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#007bff');

      const links = props.tasks.filter(t => t.parent_id && nodeMap[t.parent_id]);
      mainGroup.append('g').selectAll('path').data(links).enter().append('path')
        .attr('d', d => {
          const s = pos[d.parent_id], t = pos[d.id], mY = (s.y + nodeH + t.y) / 2;
          return `M${s.x},${s.y + nodeH} C${s.x},${mY} ${t.x},${mY} ${t.x},${t.y}`;
        })
        .attr('fill', 'none').attr('stroke', '#007bff').attr('stroke-width', 1.5).attr('marker-end', 'url(#arrow)');

      const node = mainGroup.append('g').selectAll('g').data(props.tasks).enter().append('g')
        .attr('transform', t => `translate(${(pos[t.id]?.x || 0) - nodeW / 2},${(pos[t.id]?.y || 0)})`)
        .style('cursor', 'pointer')
        .on('click', (ev, t) => window.__showLogs && window.__showLogs(t.id));

      node.append('rect').attr('width', nodeW).attr('height', nodeH).attr('rx', 4)
        .attr('fill', '#ffffff').attr('stroke', '#007bff').attr('stroke-width', 2);

      node.append('text').attr('x', nodeW / 2).attr('y', 22).attr('text-anchor', 'middle').attr('fill', '#343a40').attr('font-size', '11px').attr('font-weight', '600').attr('font-family', '"Source Sans Pro", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif')
        .text(t => t.id.length > 24 ? t.id.slice(0, 22) + '…' : t.id);

      node.append('text').attr('x', nodeW / 2).attr('y', 40).attr('text-anchor', 'middle').attr('fill', t => props.colors[t.status]).attr('font-size', '10px').attr('font-weight', 'bold').attr('text-transform', 'uppercase')
        .text(t => t.status);

      svg.call(d3.zoom().scaleExtent([0.1, 4]).on('zoom', (e) => mainGroup.attr('transform', e.transform)));
    };

    Vue.onMounted(renderDag);
    Vue.watch(() => props.tasks, () => Vue.nextTick(renderDag), { deep: true });

    return { svgRef };
  },
  template: `
    <div style="background:#f4f6f9; border:1px solid #007bff; border-top: 3px solid #007bff; border-radius:4px; overflow:hidden; position:relative;">
      <svg ref="svgRef" style="width:100%; height:450px; display:block;"></svg>
      <div style="position:absolute; bottom:10px; left:10px; display:flex; gap:12px; pointer-events:none; background: rgba(255,255,255,0.9); padding: 5px 10px; border-radius: 4px; border: 1px solid #dee2e6;">
        <span v-for="(color, status) in colors" :key="status" style="font-size:0.75rem; color:#495057; font-weight: 600;">
          <i class="fas fa-circle" :style="'color:'+color"></i> {{ status.toUpperCase() }}
        </span>
      </div>
    </div>
  `
};
