import { fmtDt } from '../utils.js';
import { TASK_STATUS } from '../config.js';
import BaseModal from './BaseModal.js';

export default {
  name: 'TaskInfoModal',
  components: { BaseModal },

  setup() {
    const modal = Vue.ref(null);
    const task  = Vue.ref(null);

    function parseKV(str) {
      if (!str) return {};
      try {
        const v = JSON.parse(str);
        if (v && typeof v === 'object' && !Array.isArray(v)) return v;
      } catch {}
      return str ? { value: str } : {};
    }

    function show(t) {
      task.value = t;
      modal.value.open();
    }

    return { modal, task, show, parseKV, fmtDt, TASK_STATUS };
  },

  template: `
    <base-modal ref="modal" size="md" :scrollable="true">

      <template #title>
        <i class="fas fa-project-diagram mr-2 text-primary"></i>{{ task && task.id }}
      </template>

      <template v-if="task">

        <!-- Status + last update -->
        <div class="d-flex align-items-center mb-3">
          <span :class="['badge', 'badge-' + (TASK_STATUS[task.status]?.color || 'secondary'), 'mr-2']">
            {{ task.status }}
          </span>
          <span class="text-muted small">
            <i class="far fa-clock mr-1"></i>{{ fmtDt(task.last_update) }}
          </span>
        </div>

        <!-- Parameters -->
        <template v-if="Object.keys(parseKV(task.params)).length">
          <p class="text-uppercase text-muted mb-1" style="font-size:0.7rem; font-weight:700; letter-spacing:.05em;">
            Parameters
          </p>
          <table class="table table-sm table-borderless mb-3">
            <tr v-for="(v, k) in parseKV(task.params)" :key="k">
              <td class="text-muted pl-0" style="width:35%; white-space:nowrap;">{{ k }}</td>
              <td class="pr-0" style="word-break:break-all;">{{ v }}</td>
            </tr>
          </table>
        </template>

        <!-- Attributes -->
        <template v-if="Object.keys(parseKV(task.attributes)).length">
          <p class="text-uppercase text-muted mb-1" style="font-size:0.7rem; font-weight:700; letter-spacing:.05em;">
            Attributes
          </p>
          <table class="table table-sm table-borderless mb-0">
            <tr v-for="(v, k) in parseKV(task.attributes)" :key="k">
              <td class="text-muted pl-0" style="width:35%; white-space:nowrap;">{{ k }}</td>
              <td class="pr-0" style="word-break:break-all;">{{ v }}</td>
            </tr>
          </table>
        </template>

      </template>

    </base-modal>
  `
};
