export default {
  name: 'DagChart',
  props: {
    tasks: { type: Array, required: true },
    colors: { type: Object, required: true }
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

      const links = props.tasks.filter(t => t.parent_id && nodeMap[t.parent_id]);
      mainGroup.append('g').selectAll('path').data(links).enter().append('path')
        .attr('d', d => {
          const s = pos[d.parent_id], t = pos[d.id], mY = (s.y + nodeH + t.y) / 2;
          return `M${s.x},${s.y + nodeH} C${s.x},${mY} ${t.x},${mY} ${t.x},${t.y}`;
        })
        .attr('fill', 'none').attr('stroke', '#4b0082').attr('stroke-width', 1.5).attr('marker-end', 'url(#arrow)');

      const node = mainGroup.append('g').selectAll('g').data(props.tasks).enter().append('g')
        .attr('transform', t => `translate(${(pos[t.id]?.x || 0) - nodeW / 2},${(pos[t.id]?.y || 0)})`)
        .style('cursor', 'pointer')
        .on('click', (ev, t) => window.__showLogs && window.__showLogs(t.id));

      node.append('rect').attr('width', nodeW).attr('height', nodeH).attr('rx', 6)
        .attr('fill', '#1a002e').attr('stroke', t => props.colors[t.status] || '#4b0082').attr('stroke-width', 2);

      node.append('text').attr('x', nodeW / 2).attr('y', 22).attr('text-anchor', 'middle').attr('fill', '#00d4ff').attr('font-size', '10px').attr('font-family', 'monospace')
        .text(t => t.id.length > 22 ? t.id.slice(0, 20) + '…' : t.id);

      node.append('text').attr('x', nodeW / 2).attr('y', 38).attr('text-anchor', 'middle').attr('fill', t => props.colors[t.status]).attr('font-size', '9px').attr('font-weight', 'bold')
        .text(t => t.status);

      svg.call(d3.zoom().scaleExtent([0.2, 3]).on('zoom', (e) => mainGroup.attr('transform', e.transform)));
    };

    Vue.onMounted(renderDag);
    Vue.watch(() => props.tasks, () => Vue.nextTick(renderDag), { deep: true });

    return { svgRef };
  },
  template: `
    <div style="background:#0f001a; border:1px solid #4b0082; border-radius:8px; overflow:hidden; position:relative;">
      <svg ref="svgRef" style="width:100%; height:450px; display:block;"></svg>
      <div style="position:absolute; bottom:10px; left:10px; display:flex; gap:10px; pointer-events:none;">
        <span v-for="(color, status) in colors" :key="status" style="font-size:0.7em; color:#aaa;">
          <i class="fas fa-circle" :style="'color:'+color"></i> {{ status }}
        </span>
      </div>
    </div>
  `
};
