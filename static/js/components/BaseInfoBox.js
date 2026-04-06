export default {
  name: 'BaseInfoBox',
  props: {
    label: String,
    value: [String, Number],
    icon: String,
    color: { type: String, default: 'info' } // info, success, warning, danger
  },
  template: `
    <div class="info-box shadow-sm">
      <span :class="['info-box-icon', 'bg-' + color]">
        <i :class="icon"></i>
      </span>
      <div class="info-box-content">
        <span class="info-box-text text-uppercase small font-weight-bold">{{ label }}</span>
        <span class="info-box-number" style="font-size: 1.4rem;">{{ value }}</span>
      </div>
    </div>
  `
};
