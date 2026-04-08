import BaseButton from './BaseButton.js';

export default {
  name: 'BaseModal',
  components: { BaseButton },
  props: {
    title: { type: String, default: '' },
    size: { type: String, default: "sm" }, // opzionale: sm, lg, xl
    variant: { type: String, default: 'default' },
    scrollable: { type: Boolean, default: false },
    icon: { type: String, default: '' },
    bodyStyle:  { type: Object,  default: () => ({}) }
  },
  data() {
    return {
      isVisible: false
    }
  },
  methods: {
    open() {
      this.isVisible = true;
      document.body.classList.add('modal-open');
    },
    close() {
      this.isVisible = false;
      document.body.classList.remove('modal-open');
    }
  },
  template: `
  <div v-show="isVisible">
    
    <!-- backdrop -->
    <div class="modal-backdrop fade show"></div>
    
    <!-- modal -->
    <div class="modal fade show d-block" tabindex="-1">
      
      <div 
        :class="[
          'modal-dialog',
          size ? 'modal-' + size : '',
          scrollable ? 'modal-dialog-scrollable' : ''
        ]"
        style="
          max-width: 80vw;
          width: fit-content;
          margin: 1.75rem auto;
        "
      >
        
        <div 
          class="modal-content"
          style="
            width: 100%;
            min-width: 280px;
            max-width: 100%;
          "
        >
          
          <!-- HEADER -->
          <div :class="['modal-header', variant !== 'default' ? 'bg-' + variant : '']">
            
            <h5 class="modal-title">
              <i v-if="icon" :class="['fas mr-2', icon]"></i>
              
              <slot name="title">
                <span v-if="title" v-html="title"></span>
              </slot>
            </h5>

            <base-button 
              icon="fas fa-times"
              color="outline-secondary"
              @click="close"
            />

          </div>

          <!-- BODY -->
          <div 
            class="modal-body"
            :style="{
              overflowX: 'auto',
              maxHeight: '70vh',
              ...bodyStyle
            }"
          >
            <slot></slot>
          </div>

          <!-- FOOTER -->
          <div class="modal-footer justify-content-between">
            <slot name="footer">
              <base-button 
                label="Close"
                icon="fas fa-times" 
                class="ml-auto"
                color="outline-secondary"
                @click="close"
              />
            </slot>
          </div>

        </div>
      </div>
    </div>
  </div>
  `
};