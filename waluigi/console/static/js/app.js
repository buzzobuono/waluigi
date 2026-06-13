import { router }  from './router.js';
import { clearToken, getToken, api } from './api.js';
import { nsStore } from './store.js';
import SideBar     from './components/SideBar.js';
import NavBar      from './components/NavBar.js';

const { createApp, ref, computed, watch, onMounted } = Vue;

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
      const isAdmin = payload?.namespaces === "*";
      user.value = payload
        ? { name: payload.sub, role: isAdmin ? 'Administrator' : 'User', isAdmin, namespaces: payload.namespaces }
        : null;
    }

    async function loadNamespaces() {
      if (!getToken()) {
        nsStore.available = [];
        nsStore.selected  = '';
        return;
      }
      try {
        const data = await api.namespaces();
        const list = (Array.isArray(data) ? data : []).map(r => r.namespace);
        nsStore.available = list;
        // keep current selection if still valid, else pick first
        if (!nsStore.selected || !list.includes(nsStore.selected)) {
          nsStore.selected = list[0] || '';
        }
      } catch { /* not logged in yet or boss unreachable */ }
    }

    function logout() {
      clearToken();
      user.value = null;
      nsStore.available = [];
      nsStore.selected  = '';
      router.push('/login');
    }

    // reload namespaces when user logs in/out
    watch(user, (u) => {
      if (u) loadNamespaces();
      else { nsStore.available = []; nsStore.selected = ''; }
    });

    const navItems = [
      { path: '/dashboard',  label: 'Dashboard',  icon: 'fa-th-large' },
      {
        label: 'Operations', icon: 'fa-cogs',
        children: [
          { path: '/namespaces',       label: 'Namespace',        icon: 'fa-layer-group' },
          { path: '/jobs',             label: 'Jobs',             icon: 'fa-briefcase' },
          { path: '/tasks',            label: 'Tasks',            icon: 'fa-tasks' },
          { path: '/task-definitions', label: 'Task Definitions', icon: 'fa-cubes' },
          { path: '/job-definitions',  label: 'Job Definitions',  icon: 'fa-list-alt' },
          { path: '/cron-jobs',        label: 'Cron Jobs',        icon: 'fa-clock' },
          { path: '/resources',        label: 'Resources',        icon: 'fa-chart-bar' },
          { path: '/secrets',          label: 'Secrets',          icon: 'fa-key' },
        ]
      },
      {
        label: 'Cluster', icon: 'fa-microchip', adminOnly: true,
        children: [
          { path: '/workers', label: 'Workers', icon: 'fa-server' },
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

    onMounted(() => { loadUser(); loadNamespaces(); });
    router.afterEach(() => loadUser());

    return {
      navItems, isGroupActive,
      currentTitle: computed(() => router.currentRoute.value.meta?.title || 'Console'),
      user, logout, isLogin, nsStore,
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
          :ns-store="nsStore"
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
