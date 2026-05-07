import { router }  from './router.js';
import { clearToken, getToken } from './api.js';
import SideBar     from './components/SideBar.js';
import NavBar      from './components/NavBar.js';

const { createApp, ref, computed, onMounted } = Vue;

function decodeToken(token) {
  try { return JSON.parse(atob(token.split('.')[1])); } catch { return null; }
}

const App = {
  name: 'App',
  components: { SideBar, NavBar },

  setup() {
    const user = ref(null);

    function loadUser() {
      const token = getToken();
      if (!token) { user.value = null; return; }
      const payload = decodeToken(token);
      user.value = payload
        ? { name: payload.sub, role: payload.is_admin ? 'Administrator' : 'User', isAdmin: !!payload.is_admin }
        : null;
    }

    function logout() {
      clearToken();
      user.value = null;
      router.push('/login');
    }

    const navItems = [
      { path: '/dashboard',  label: 'Dashboard',  icon: 'fa-th-large' },
      {
        label: 'Operations', icon: 'fa-cogs',
        children: [
          { path: '/namespaces', label: 'Namespaces', icon: 'fa-layer-group' },
          { path: '/jobs',  label: 'Jobs',  icon: 'fa-briefcase' },
          { path: '/tasks', label: 'Tasks', icon: 'fa-tasks' }
        ]
      },
      {
        label: 'Cluster', icon: 'fa-microchip',
        children: [
          { path: '/workers',   label: 'Workers',   icon: 'fa-server' },
          { path: '/resources', label: 'Resources', icon: 'fa-chart-bar' }
        ]
      },
      {
        label: 'Data Management', icon: 'fa-database',
        children: [
          { path: '/catalog',    label: 'Catalog',      icon: 'fa-table' },
          { path: '/sources',    label: 'Sources',      icon: 'fa-plug' },
          { path: '/lineage',    label: 'Lineage',      icon: 'fa-project-diagram' },
          { path: '/dq/rules',   label: 'Expectations', icon: 'fa-shield-alt' },
        ]
      },
      {
        label: 'Administration', icon: 'fa-user-shield', adminOnly: true,
        children: [
          { path: '/admin/users', label: 'Users', icon: 'fa-users' },
        ]
      }
    ];

    const isGroupActive = (item) => {
      if (!item.children) return false;
      return item.children.some(sub => router.currentRoute.value.path.startsWith(sub.path));
    };

    const isLogin = computed(() => router.currentRoute.value.path === '/login');

    onMounted(() => loadUser());
    router.afterEach(() => loadUser());

    return {
      navItems, isGroupActive,
      currentTitle: computed(() => router.currentRoute.value.meta?.title || 'Console'),
      user, logout, isLogin,
    };
  },

  template: `
    <template v-if="isLogin">
      <router-view />
    </template>
    <template v-else>
      <div class="wrapper">
        <NavBar
          :title="currentTitle"
          :user="user"
          @logout="logout"
        />
        <SideBar
          :navItems="navItems.filter(i => !i.adminOnly || user?.isAdmin)"
          :isGroupActive="isGroupActive"
        />
        <div class="content-wrapper wl-content">
          <section class="content pt-3">
            <div class="container-fluid">
              <router-view />
            </div>
          </section>
        </div>
      </div>
    </template>
  `
};

const vueApp = createApp(App);
vueApp.use(router);
vueApp.mount('#app');
