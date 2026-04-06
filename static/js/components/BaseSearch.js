const { defineComponent } = Vue;

export default defineComponent({
  name: 'BaseSearch',
  props: {
    modelValue: { type: String, default: '' },
    placeholder: { type: String, default: 'Search...' },
    width: { type: String, default: '250px' }
  },
  emits: ['update:modelValue'],
  
  template: `
    <div class="input-group input-group-sm shadow-sm" :style="{ width: width }">
      <div class="input-group-prepend">
        <span class="input-group-text bg-dark border-secondary">
          <i class="fas fa-search text-muted"></i>
        </span>
      </div>
      <input 
        type="text" 
        class="form-control bg-dark border-secondary text-white" 
        :placeholder="placeholder"
        :value="modelValue"
        @input="$emit('update:modelValue', $event.target.value)"
      >
    </div>
  `
});
