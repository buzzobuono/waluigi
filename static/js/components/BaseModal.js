export default {
  name: 'BaseModal',
  props: {
    title: { type: String, default: '' },
    size: { type: String, default: 'sm' },
    variant: { type: String, default: 'default' }, // primary, danger, success, ecc.
    scrollable: { type: Boolean, default: false }
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
  <div v-if="isVisible" class="modal-wrapper">
    
    <div class="modal-backdrop fade show"></div>
    
    <div class="modal fade show" style="display: block;" tabindex="-1" role="dialog">
      <div :class="['modal-dialog', 'modal-' + size]" role="document">
        <div class="modal-content">
          
          <div :class="['modal-header', variant !== 'default' ? 'bg-' + variant : '']">
            <h4 class="modal-title">{{ title }}</h4>
            <button type="button" class="close" @click="close">&times;</button>
          </div>

          <div class="modal-body">
            <slot></slot>
          </div>

          <div class="modal-footer justify-content-between">
            <slot name="footer">
              <button type="button" class="btn btn-default" @click="close">Chiudi</button>
            </slot>
          </div>

        </div>
      </div>
    </div>
  </div>
`

};
