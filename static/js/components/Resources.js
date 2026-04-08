import BasePage from './BasePage.js';
import BasePanel from './BasePanel.js';
import BaseButton from './BaseButton.js';

export default {
  name: 'Resources',
  props: { 
    resources: Array,
    loading: Boolean 
  },
  components: { BasePage, BasePanel, BaseButton },
  emits: ['refresh'],
  
  methods: {
    pct(r) {
      return r.amount > 0 ? Math.round(r.usage / r.amount * 100) : 0;
    },
    color(r) {
      const p = this.pct(r);
      return p > 80 ? 'danger' : p > 50 ? 'warning' : 'success';
    }
  },

  template: `
    <base-page 
      title="Resources"
      subtitle="Cluster resources consumption"
      icon="fas fa-microchip">
      
      <template #actions>
          <base-button 
            icon="fas fa-sync-alt" 
            color="outline-primary" 
            label="Update"
            :loading="loading"
            class="ml-auto"
            @click="$emit('refresh')"
          />
      </template>

      <div v-if="!resources || !resources.length" class="text-muted mt-3 text-center">
        No resources configured.
      </div>

      <div v-else class="row">
        <div class="col-12 col-sm-6 col-md-4" v-for="r in resources" :key="r.name">
          <base-panel>
            <template #title>
              <h3 class="card-title">{{ r.name }}</h3>
            </template>
            
            <template #tools>
              <span :class="['badge', 'bg-'+color(r), 'ml-auto']">{{ pct(r) }}%</span>
            </template>

            <div class="card-body p-0">
              <div class="d-flex justify-content-between mb-2">
                <span>Usage: <b>{{ r.usage }}</b> / {{ r.amount }}</span>
                <span>Available: <b>{{ r.amount - r.usage }}</b></span>
              </div>
              <div class="progress shadow">
                <div 
                  class="progress-bar" 
                  :class="'bg-'+color(r)"
                  :style="'width:'+pct(r)+'%'"
                ></div>
              </div>
            </div>
          </base-panel>
        </div>
      </div>
    </base-page>
  `
};
