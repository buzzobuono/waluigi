// components/Workers.js
export default {
  name: 'Workers',
  props: { workers: Array },
  computed: {
    totalSlots() { return (this.workers || []).reduce((s, w) => s + (w.max_slots || 0), 0); },
    freeSlots()  { return (this.workers || []).reduce((s, w) => s + (w.free_slots || 0), 0); },
    busySlots()  { return this.totalSlots - this.freeSlots; }
  },
  template: `
    <div>
      <!-- Stats -->
      <div class="row mb-3">
        <div class="col-6 col-sm-3">
          <div class="info-box">
            <span class="info-box-icon bg-success"><i class="fas fa-server"></i></span>
            <div class="info-box-content">
              <span class="info-box-text">Workers</span>
              <span class="info-box-number">{{ (workers || []).length }}</span>
            </div>
          </div>
        </div>
        <div class="col-6 col-sm-3">
          <div class="info-box">
            <span class="info-box-icon bg-info"><i class="fas fa-puzzle-piece"></i></span>
            <div class="info-box-content">
              <span class="info-box-text">Total Slots</span>
              <span class="info-box-number">{{ totalSlots }}</span>
            </div>
          </div>
        </div>
        <div class="col-6 col-sm-3">
          <div class="info-box">
            <span class="info-box-icon bg-warning"><i class="fas fa-circle"></i></span>
            <div class="info-box-content">
              <span class="info-box-text">Free Slots</span>
              <span class="info-box-number">{{ freeSlots }}</span>
            </div>
          </div>
        </div>
        <div class="col-6 col-sm-3">
          <div class="info-box">
            <span class="info-box-icon bg-danger"><i class="fas fa-minus-circle"></i></span>
            <div class="info-box-content">
              <span class="info-box-text">Busy Slots</span>
              <span class="info-box-number">{{ busySlots }}</span>
            </div>
          </div>
        </div>
      </div>

      <!-- Table -->
      <div class="card card-outline">
        <div class="card-header">
          <h3 class="card-title"><i class="fas fa-server mr-2"></i>Workers</h3>
        </div>
        <div class="card-body p-0">
          <div class="table-responsive">
            <table class="table table-sm table-hover mb-0">
              <thead>
                <tr>
                  <th>URL</th>
                  <th>Status</th>
                  <th>Max Slots</th>
                  <th>Free Slots</th>
                  <th>Last Seen</th>
                </tr>
              </thead>
              <tbody>
                <tr v-if="!workers || !workers.length">
                  <td colspan="5" class="text-center text-muted py-3">No workers registered</td>
                </tr>
                <tr v-for="w in workers" :key="w.url">
                  <td style="font-family:monospace; font-size:0.85em;">{{ w.url }}</td>
                  <td><span :class="['badge', 'badge-'+(w.status||'ALIVE')]">{{ w.status || 'ALIVE' }}</span></td>
                  <td>{{ w.max_slots }}</td>
                  <td>{{ w.free_slots }}</td>
                  <td style="font-size:0.8em;">{{ w.last_seen || '—' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  `
};
