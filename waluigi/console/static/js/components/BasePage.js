export default {
  name: 'BasePage',
  props: ['title', 'subtitle', 'icon', 'loading', 'error'],
  template: `
    <div class="pt-3">
      
      <div class="content-header px-0 pt-0 mb-2">
        <h1 class="m-0">
          <i v-if="icon" :class="[icon, 'mr-2']"></i>
          {{ title }}
          <small v-if="subtitle" class="text-muted ml-2">
            {{ subtitle }}
          </small>
        </h1>
      </div>

      <div v-if="$slots.actions" class="d-flex flex-wrap align-items-center mb-3 pb-3 border-bottom">
        <slot name="actions"></slot>
      </div>

      <div v-if="error" class="alert alert-danger mb-4">
        <i class="fas fa-exclamation-triangle mr-2"></i> 
        {{ error }}
      </div>

      <div v-if="loading" class="text-center py-5">
        <div class="spinner-border" role="status">
          <span class="sr-only">Loading...</span>
        </div>
      </div>
      
      <div v-else>
        <slot></slot>
      </div>

    </div>
  `
};