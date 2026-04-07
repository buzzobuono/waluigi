export default {
  name: 'BaseButton',
  props: {
    label: String,
    icon: String,
    // Colori standard: primary, success, info, warning, danger, default, dark
    // Prefissi standard: 'btn-primary' (pieno) o 'btn-outline-primary' (bordo)
    color: { type: String, default: 'primary' },
    // Taglie standard AdminLTE: lg, sm, xs (AdminLTE aggiunge xs a Bootstrap)
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
