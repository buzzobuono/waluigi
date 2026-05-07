import BaseButton from './BaseButton.js';

export default {
  name: 'UserBadge',
  components: { BaseButton },
  props: {
    username: { type: String, default: '' },
    role:     { type: String, default: 'User' },
    avatarUrl: { type: String, default: null },
  },
  emits: ['logout'],
  computed: {
    isAdmin() { return this.role === 'Administrator'; },
  },
  template: `
    <li class="nav-item dropdown user-menu">

      <a href="#" class="nav-link dropdown-toggle" data-toggle="dropdown">
        <i class="fas fa-user-circle"></i>
        <span class="d-none d-md-inline ml-2">{{ username }}</span>
        <span v-if="isAdmin" class="badge badge-warning ml-1" style="font-size:0.65rem">Admin</span>
      </a>

      <ul class="dropdown-menu dropdown-menu-lg dropdown-menu-right">

        <li class="user-header text-center" :class="isAdmin ? 'bg-warning' : 'bg-primary'">
          <i class="fas fa-user-shield fa-3x mb-2" v-if="isAdmin"></i>
          <i class="fas fa-user-circle fa-3x mb-2" v-else></i>
          <p>
            {{ username }}
            <small>{{ role }}</small>
          </p>
        </li>

        <li class="user-footer text-right p-2">
          <BaseButton
            label="Logout"
            icon="fas fa-sign-out-alt"
            color="danger"
            @click="$emit('logout')"
          />
        </li>

      </ul>
    </li>
  `
};