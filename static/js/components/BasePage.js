export default {
  name: 'BasePage',
  props: ['title', 'subtitle', 'icon', 'loading', 'error'],
  template: `
    <div class="base-page-container pt-3">
      <div class="content-header px-0 pt-0 mb-2">
        <h1 class="m-0 wl-accent" style="font-size: 1.8rem; letter-spacing: -0.5px;">
          <i v-if="icon" :class="[icon, 'mr-2 text-muted']" style="font-size: 0.8em;"></i>
          {{ title }}
          <small v-if="subtitle" class="text-muted ml-2" style="font-size: 0.55em; font-weight: 300;">
            {{ subtitle }}
          </small>
        </h1>
      </div>

      <div v-if="$slots.actions" class="page-toolbar d-flex flex-wrap align-items-center mb-3 pb-3 border-bottom border-secondary-light">
        <slot name="actions"></slot>
      </div>

      <div v-if="error" class="alert alert-danger shadow-sm mb-4 border-0" style="background: rgba(220, 53, 69, 0.1); color: #ff6b6b;">
        <i class="fas fa-exclamation-triangle mr-2"></i> {{ error }}
      </div>

      <div v-if="loading" class="text-center py-5">
        <div class="spinner-border text-primary" role="status">
          <span class="sr-only">Loading...</span>
        </div>
      </div>
      
      <div v-else class="page-content animate__animated animate__fadeIn">
        <slot></slot>
      </div>
    </div>
  `
};
