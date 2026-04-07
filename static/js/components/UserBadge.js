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
    <li class="nav-item dropdown user-menu">
      
      <a href="#" 
         class="nav-link dropdown-toggle" 
         data-toggle="dropdown">
        <i class="fas fa-user-circle"></i>
        <span class="d-none d-md-inline ml-2">{{ username }}</span>
      </a>

      <ul class="dropdown-menu dropdown-menu-lg dropdown-menu-right">
        
        <!-- User header -->
        <li class="user-header bg-primary text-center">
          <i class="fas fa-user-circle fa-3x mb-2"></i>
          <p>
            {{ username }}
            <small>{{ role }}</small>
          </p>
        </li>

        <!-- User footer -->
        <li class="user-footer d-flex justify-content-between p-2">
          <BaseButton 
            label="Profilo" 
            icon="fas fa-user" 
            color="primary" 
            @click="$emit('profile')"
          />
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