import UserBadge from './UserBadge.js';

export default {
  name: 'NavBar',
  components: { UserBadge },
  props: {
    title:   String,
    user:    Object,
    nsStore: Object,
  },
  emits: ['logout'],
  template: `
    <nav class="main-header navbar navbar-expand navbar-white navbar-light">
      <ul class="navbar-nav">
        <li class="nav-item">
          <a class="nav-link" data-widget="pushmenu" href="#" role="button">
            <i class="fas fa-bars"></i>
          </a>
        </li>
        <li class="nav-item d-none d-sm-inline-block">
          <span class="nav-link font-weight-bold">{{ title }}</span>
        </li>
      </ul>

      <ul class="navbar-nav ml-auto align-items-center">

        <!-- Namespace selector -->
        <li v-if="nsStore && nsStore.available.length"
            class="nav-item d-flex align-items-center mr-3">
          <i class="fas fa-layer-group text-muted mr-2 small"></i>
          <select
            class="form-control form-control-sm"
            style="max-width:180px; background:transparent; border:1px solid #dee2e6; border-radius:4px;"
            v-model="nsStore.selected">
            <option v-for="ns in nsStore.available" :key="ns" :value="ns">{{ ns }}</option>
          </select>
        </li>
        <li v-else-if="nsStore && user && !nsStore.available.length"
            class="nav-item d-flex align-items-center mr-3">
          <i class="fas fa-layer-group text-muted mr-2 small"></i>
          <span class="text-muted small">no namespaces</span>
        </li>

        <UserBadge
          :username="user?.name"
          :role="user?.role"
          @logout="$emit('logout')"
        />
      </ul>
    </nav>
  `
};
