export default {
  name: 'SideBar',
  props: {
    navItems: Array,
    counts: Object,
    isGroupActive: Function
  },
  template: `
    <aside class="main-sidebar elevation-4 wl-sidebar">
      <a href="/" class="brand-link text-center wl-brand">
        <span class="brand-text font-weight-bold wl-accent" style="font-size:1.1em;">🟣 Waluigi</span>
      </a>
      
      <div class="sidebar">
        <nav class="mt-2">
          <ul class="nav nav-pills nav-sidebar flex-column" data-widget="treeview" role="menu" data-accordion="false">
            
            <li v-for="item in navItems" :key="item.label" 
                :class="['nav-item', { 'menu-open': isGroupActive(item) }]">
              
              <router-link v-if="!item.children" :to="item.path" 
                           class="nav-link d-flex align-items-center text-nowrap" 
                           active-class="active">
                <i :class="['nav-icon fas', item.icon]"></i>
                <p class="ml-2 mb-0">
                  {{ item.label }}
                  <span v-if="item.key" class="badge badge-secondary right">
                    {{ counts[item.key] }}
                  </span>
                </p>
              </router-link>

              <template v-else>
                <a href="#" class="nav-link d-flex align-items-center text-nowrap" 
                   :class="{ 'active': isGroupActive(item) }">
                  <i :class="['nav-icon fas', item.icon]"></i>
                  <p class="ml-2 mb-0">
                    {{ item.label }}
                    <i class="right fas fa-angle-left"></i>
                  </p>
                </a>
                
                <ul class="nav nav-treeview">
                  <li v-for="sub in item.children" :key="sub.path" class="nav-item">
                    <router-link :to="sub.path" 
                                 class="nav-link d-flex align-items-center text-nowrap" 
                                 active-class="active" 
                                 style="padding-left: 2rem;">
                      <i :class="['nav-icon fas fa-xs', sub.icon]"></i>
                      <p class="ml-2 mb-0">
                        {{ sub.label }}
                        <span v-if="sub.key" class="badge badge-secondary right">
                          {{ counts[sub.key] }}
                        </span>
                      </p>
                    </router-link>
                  </li>
                </ul>
              </template>

            </li>
          </ul>
        </nav>
      </div>
    </aside>
  `
};
