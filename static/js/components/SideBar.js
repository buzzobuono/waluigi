export default {
  name: 'SideBar',
  props: {
    navItems: Array,
    counts: Object,
    isGroupActive: Function
  },
  template: `
    <aside class="main-sidebar sidebar-light-primary elevation-4">
      
      <a href="/" class="brand-link text-center">
        <span class="brand-text font-weight-light" style="font-size:1.1em;">
          <i class="fas fa-table mr-1 text-primary"></i> <strong>Waluigi</strong>
        </span>
      </a>
      
      <div class="sidebar">
        <nav class="mt-2">
          <ul class="nav nav-pills nav-sidebar flex-column" data-widget="treeview" role="menu" data-accordion="false">
            
            <li v-for="item in navItems" :key="item.label" 
                :class="['nav-item', { 'menu-open': isGroupActive(item) }]">
  
              <router-link v-if="!item.children" :to="item.path" 
                           class="nav-link" 
                           active-class="active">
                <i :class="['nav-icon fas', item.icon]"></i>
                <p>
                  {{ item.label }}
                  <span v-if="item.key && counts[item.key]" class="badge badge-info right">
                    {{ counts[item.key] }}
                  </span>
                </p>
              </router-link>

              <template v-else>
                <a href="#" class="nav-link" 
                   :class="{ 'active': isGroupActive(item) }">
                  <i :class="['nav-icon fas', item.icon]"></i>
                  <p>
                    {{ item.label }}
                    <i class="right fas fa-angle-left"></i>
                  </p>
                </a>
                
                <ul class="nav nav-treeview">
                  <li v-for="sub in item.children" :key="sub.path" class="nav-item">
                    <router-link :to="sub.path" 
                                 class="nav-link" 
                                 active-class="active">
                      <i :class="['nav-icon fas fa-circle fa-xs', sub.icon ? sub.icon : '']"></i>
                      <p>
                        {{ sub.label }}
                        <span v-if="sub.key && counts[sub.key]" class="badge badge-info right">
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
