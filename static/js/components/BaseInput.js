export default {
  name: 'BaseInput',
  props: {
    modelValue: String,
    placeholder: String,
    size: { type: String, default: 'sm' },
    disabled: Boolean,
    readonly: Boolean
  },
  emits: ['update:modelValue', 'keyup.enter'],
  template: `
    <input
      type="text"
      :class="['form-control', size ? 'form-control-' + size : '']"
      :value="modelValue"
      :placeholder="placeholder"
      :disabled="disabled"
      :readonly="readonly"
      @input="$emit('update:modelValue', $event.target.value)"
      @keyup.enter="$emit('keyup.enter')"
    />
  `
};