import UserBadge from './UserBadge.js';

export default {
  name: 'NavBar',
  components: { UserBadge },
  props: {
    title: String,
    loading: Boolean,
    user: Object // Es: { name: 'Admin', role: 'Dev', avatar: '' }
  },
  emits: ['refresh', 'logout'],
  template: `
    <nav class="main-header navbar navbar-expand navbar-white navbar-light">
      <ul class="navbar-nav">
        <li class="nav-item">
          <a class="nav-link" data-widget="pushmenu" href="#" role="button">
            <i class="fas fa-bars"></i>
          </a>
        </li>
        <li class="nav-item d-none d-sm-inline-block">
          <span class="nav-link font-weight-bold text-white">{{ title }}</span>
        </li>
      </ul>

      <ul class="navbar-nav ml-auto align-items-center">
        
        <li class="nav-item mr-2">
          <a class="nav-link" href="#" @click.prevent="$emit('refresh')" title="Refresh Data">
            <i class="fas fa-sync-alt wl-accent" :class="{ 'fa-spin': loading }"></i>
          </a>
        </li>

        <UserBadge 
          :username="user?.name" 
          :role="user?.role" 
          :avatarUrl="user?.avatar"
          @logout="$emit('logout')" 
        />
      </ul>
    </nav>
  `
};
