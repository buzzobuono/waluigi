const { defineComponent } = Vue;

export default defineComponent({
  name: 'BaseSearch',
  props: {
    modelValue: { type: String, default: '' },
    placeholder: { type: String, default: 'Search' }
  },
  emits: ['update:modelValue'],

  template: `
    <form class="form-inline ml-3">
      <div class="input-group input-group-sm">
        
        <input 
          class="form-control" 
          type="search"
          :placeholder="placeholder"
          :value="modelValue"
          @input="$emit('update:modelValue', $event.target.value)"
        >

        <div class="input-group-append">
          <button class="btn btn-sm btn-default" type="button">
            <i class="fas fa-search"></i>
          </button>
        </div>

      </div>
    </form>
  `
});