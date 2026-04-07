export default {
  name: 'BaseButton',
  props: {
    label: String,
    icon: String,
    color: { type: String, default: 'primary' },
    size: { type: String, default: 'sm' }, 
    loading: Boolean,
    disabled: Boolean,
    title: String
  },
  template: `
    <button 
      type="button"
      :class="['btn', 'btn-' + color, size ? 'btn-' + size : '']"
      :disabled="disabled || loading"
      :title="title"
    >
      <!-- Icona di caricamento -->
      <template v-if="loading">
        <i class="fas fa-spinner fa-spin" :class="{ 'mr-1': label }"></i>
      </template>
      
      <!-- Icona normale -->
      <template v-else-if="icon">
        <i :class="['fas', icon, { 'mr-1': label }]"></i>
      </template>

      <!-- Testo del pulsante -->
      <span v-if="label">{{ label }}</span>
    </button>
  `
};
