import BaseButton from './BaseButton.js';

export default {
  name: 'UserBadge',
  components: { BaseButton },
  props: {
    username: { type: String, default: 'User' },
    role: { type: String, default: 'Administrator' }
  },
  emits: ['logout', 'profile'],
  template: `
    <li class="nav-item dropdown">
      <a class="nav-link dropdown-toggle d-flex align-items-center" 
         data-toggle="dropdown" 
         href="#" 
         role="button">
        <i class="fas fa-user-circle fa-lg mr-2 text-white"></i>
        <span class="d-none d-md-inline text-white font-weight-light">{{ username }}</span>
      </a>

      <div class="dropdown-menu dropdown-menu-lg dropdown-menu-right shadow-lg border-0 p-0" 
           style="background: var(--wl-dark); min-width: 220px;">
        
        <div class="p-3 text-center" style="background: var(--wl-mid); border-bottom: 1px solid var(--wl-accent);">
          <i class="fas fa-user-shield fa-2x text-warning mb-2"></i>
          <p class="mb-0 text-white" style="font-size: 0.9em;">{{ username }}</p>
          <small class="text-muted text-uppercase" style="font-size: 0.75em;">{{ role }}</small>
        </div>

        <div class="p-2 d-flex justify-content-between" style="background: var(--wl-dark);">
          <BaseButton 
            label="Profilo" 
            icon="fas fa-user" 
            color="outline-light" 
            @click="$emit('profile')"
          />
          <BaseButton 
            label="Logout" 
            icon="fas fa-sign-out-alt" 
            color="outline-danger" 
            @click="$emit('logout')"
          />
        </div>
      </div>
    </li>
  `
};
