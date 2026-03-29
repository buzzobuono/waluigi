// components/Resources.js
export default {
  name: 'Resources',
  props: { resources: Array },
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
    <div>
      <p v-if="!resources || !resources.length" class="text-muted mt-3">
        No resources configured.
      </p>
      <div class="row">
        <div class="col-12 col-sm-6 col-md-4" v-for="r in resources" :key="r.name">
          <div class="card card-outline">
            <div class="card-header d-flex justify-content-between align-items-center">
              <h3 class="card-title">{{ r.name.toUpperCase() }}</h3>
              <span :class="['badge', 'bg-'+color(r)]">{{ pct(r) }}%</span>
            </div>
            <div class="card-body">
              <div class="d-flex justify-content-between mb-2" style="color:#ccc; font-size:0.9em;">
                <span>Usage: <b>{{ r.usage }}</b> / {{ r.amount }}</span>
                <span>Available: <b>{{ r.amount - r.usage }}</b></span>
              </div>
              <div class="progress progress-sm">
                <div class="progress-bar" :class="'bg-'+color(r)"
                     :style="'width:'+pct(r)+'%'"></div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  `
};
