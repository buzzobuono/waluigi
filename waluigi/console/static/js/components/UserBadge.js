import BaseButton from './BaseButton.js';

export default {
  name: 'UserBadge',
  components: { BaseButton },
  props: {
    username: { type: String, default: '' },
    role:     { type: String, default: 'User' },
  },
  emits: ['logout'],
  template: `
    <li class="nav-item dropdown user-menu">

      <a href="#" class="nav-link dropdown-toggle" data-toggle="dropdown">
        <i class="fas fa-user-circle"></i>
        <span class="d-none d-md-inline ml-2">{{ username }}</span>
      </a>

      <ul class="dropdown-menu dropdown-menu-right">
        <li class="dropdown-item-text py-2 px-3 border-bottom">
          <div class="font-weight-bold">{{ username }}</div>
          <small class="text-muted">{{ role }}</small>
        </li>
        <li class="p-2">
          <BaseButton
            label="Logout"
            icon="fas fa-sign-out-alt"
            color="outline-danger"
            class="btn-block"
            @click="$emit('logout')"
          />
        </li>
      </ul>

    </li>
  `
};
