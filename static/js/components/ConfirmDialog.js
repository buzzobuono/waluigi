import BaseModal from './BaseModal.js';
import BaseButton from './BaseButton.js';

export default {
  name: 'ConfirmDialog',
  components: { BaseModal, BaseButton },
  
  setup() {
    const title   = Vue.ref('');
    const message = Vue.ref('');
    const callback     = Vue.ref(null);
    const confirmModal = Vue.ref(null);

    const show = async (msg) => {
      message.value = msg;
      confirmModal.value.open();
    };
    
    const ask = (msg, cb) => {
      message.value = msg;
      callback.value = cb;
      confirmModal.value.open();
    };

    const confirm = () => {
      confirmModal.value.close();
      callback.value?.(true);
    };

    const cancel = () => {
      confirmModal.value.close();
      callback.value?.(false);
    };
    
    return { title, message, show, ask, confirm, cancel, confirmModal };
  },

  template: `
    <base-modal ref="confirmModal" :title="title" icon="fas fa-exclamation-triangle" variant="warning" size="sm">
  
      <p class="mb-0" v-html="message"></p>
      
      <template #footer>
        <base-button label="Cancel" color="outline-secondary" icon="fas fa-times" @click="cancel" />
        <base-button label="Confirm" color="danger" icon="fas fa-check" @click="confirm" />
      </template>
  
    </base-modal>
  `
};
