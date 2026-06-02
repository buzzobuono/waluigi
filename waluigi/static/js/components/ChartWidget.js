const { ref, onMounted, onBeforeUnmount, watch, nextTick } = Vue;

export const ChartWidget = {
  name: 'ChartWidget',
  props: {
    option:  { type: Object,  default: null },
    loading: { type: Boolean, default: false },
    error:   { type: String,  default: null },
    height:  { type: String,  default: '320px' },
  },
  setup(props) {
    const elRef = ref(null);
    let ec = null;

    function init() {
      if (!elRef.value || !window.echarts) return;
      ec = window.echarts.init(elRef.value, null, { renderer: 'canvas' });
      if (props.option) ec.setOption(props.option);
    }

    onMounted(() => { nextTick(init); });
    onBeforeUnmount(() => { ec?.dispose(); ec = null; });

    watch(() => props.option, (opt) => {
      if (!opt) return;
      if (!ec) { nextTick(init); return; }
      ec.setOption(opt, true);
    });

    return { elRef };
  },
  template: `
    <div style="position:relative; width:100%; height:100%;">
      <div v-if="loading" class="d-flex justify-content-center align-items-center" :style="{ height }">
        <i class="fas fa-spinner fa-spin text-muted fa-2x"></i>
      </div>
      <div v-else-if="error" class="alert alert-warning small m-2">
        <i class="fas fa-exclamation-triangle mr-1"></i>{{ error }}
      </div>
      <div v-else ref="elRef" style="width:100%; height:100%;"></div>
    </div>
  `,
};

export default ChartWidget;
