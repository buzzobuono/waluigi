export default {
  name: 'BaseButton',
  props: {
    label: String,
    icon: String,
    color: { type: String, default: 'outline-primary' },
    size: { type: String, default: 'xs' },
    loading: Boolean,
    disabled: Boolean,
    title: String
  },
  template: `
    <button 
      type="button"
      :class="['btn', 'btn-' + size, 'btn-' + color, 'text-nowrap']"
      :disabled="disabled || loading"
      :title="title"
    >
      <i v-if="loading" class="fas fa-sync fa-spin" :class="{ 'mr-1': label }"></i>
      <i v-else-if="icon" :class="[icon, { 'mr-1': label }]"></i>
      <span v-if="label">{{ label }}</span>
    </button>
  `
};
