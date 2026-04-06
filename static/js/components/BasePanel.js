export default {
  name: 'BasePanel',
  props: {
    title: String,
    icon: String,
    type: { type: String, default: 'outline' },
    noPadding: Boolean
  },
  template: `
    <div :class="['card', 'card-' + type, 'mb-3']">
      <div class="card-header d-flex align-items-center w-100">
        
        <div class="d-flex align-items-center">
          <i v-if="icon" :class="[icon, 'mr-2']"></i>
          <h3 class="card-title mb-0">
            <span v-if="title" v-html="title"></span>
            <slot name="title"></slot>
          </h3>
        </div>

        <div class="card-tools d-flex align-items-center flex-grow-1">
          <slot name="tools"></slot>
        </div>
        
      </div>

      <div :class="['card-body', { 'p-0': noPadding }]">
        <slot></slot>
      </div>
    </div>
  `
};
