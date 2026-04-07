export default {
  name: 'BasePanel',
  props: {
    title: String,
    icon: String,
    // In AdminLTE 'type' può essere: primary, success, info, warning, danger
    // Usando 'card-outline' con questi colori si ottiene il look professionale
    type: { type: String, default: 'primary' }, 
    noPadding: Boolean
  },
  template: `
    <div :class="['card', 'card-' + type, 'card-outline']">
      
      <div v-if="title || $slots.title || $slots.tools" class="card-header">
        
        <h3 class="card-title">
          <i v-if="icon" :class="['fas mr-1', icon]"></i>
          <slot name="title">
            <span v-if="title" v-html="title"></span>
          </slot>
        </h3>

        <div class="card-tools">
          <slot name="tools"></slot>
        </div>
        
      </div>
  
      <div :class="['card-body', { 'p-0': noPadding }]">
        <slot></slot>
      </div>
    </div>
  `
};
