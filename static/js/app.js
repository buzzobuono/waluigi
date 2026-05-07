import { router }  from './router.js';
import { api, clearToken, getToken } from './api.js';
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
    const jobs = ref([]), tasks = ref([]), workers = ref([]), resources = ref([]);
    const loading = ref(false);
    const user    = ref(null);

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

    const counts = computed(() => ({
      jobs: jobs.value.length,
      tasks: tasks.value.length,
      workers: workers.value.length,
      namespaces: new Set(tasks.value.map(t => t.namespace).filter(ns => ns)).size
    }));

    const navItems = [
      { path: '/dashboard',  label: 'Dashboard',  icon: 'fa-th-large', key: null },
      {
        label: 'Operations', icon: 'fa-cogs',
        children: [
          { path: '/namespaces', label: 'Namespaces', icon: 'fa-layer-group', key: 'namespaces' },
          { path: '/jobs',  label: 'Jobs',  icon: 'fa-briefcase', key: 'jobs' },
          { path: '/tasks', label: 'Tasks', icon: 'fa-tasks',     key: 'tasks' }
        ]
      },
      {
        label: 'Cluster', icon: 'fa-microchip',
        children: [
          { path: '/workers',   label: 'Workers',   icon: 'fa-server',    key: 'workers' },
          { path: '/resources', label: 'Resources', icon: 'fa-chart-bar', key: null }
        ]
      },
      {
        label: 'Data Management', icon: 'fa-database',
        children: [
          { path: '/catalog',    label: 'Catalog',      icon: 'fa-table',           key: null },
          { path: '/sources',    label: 'Sources',      icon: 'fa-plug',            key: null },
          { path: '/lineage',    label: 'Lineage',      icon: 'fa-project-diagram', key: null },
          { path: '/dq/rules',   label: 'Expectations', icon: 'fa-shield-alt',      key: null },
        ]
      },
      {
        label: 'Administration', icon: 'fa-user-shield', adminOnly: true,
        children: [
          { path: '/admin/users', label: 'Users', icon: 'fa-users', key: null },
        ]
      }
    ];

    const isGroupActive = (item) => {
      if (!item.children) return false;
      return item.children.some(sub => router.currentRoute.value.path.startsWith(sub.path));
    };

    async function refreshAll() {
      loading.value = true;
      try {
        [jobs.value, tasks.value, workers.value, resources.value] = await Promise.all([
          api.jobs().catch(() => []), api.tasks().catch(() => []),
          api.workers().catch(() => []), api.resources().catch(() => [])
        ]);
      } finally { loading.value = false; }
    }

    const isLogin = computed(() => router.currentRoute.value.path === '/login');

    const LIVE_PATHS = new Set(['/jobs', '/tasks', '/workers', '/resources', '/namespaces', '/dashboard']);

    onMounted(() => loadUser());
    router.afterEach((to) => {
      loadUser();
      if (LIVE_PATHS.has(to.path) || to.path.startsWith('/tasks/')) refreshAll();
    });

    return {
      loading, counts, navItems, refreshAll, isGroupActive,
      currentTitle: computed(() => router.currentRoute.value.meta?.title || 'Console'),
      jobs, tasks, workers, resources,
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
          :loading="loading"
          :user="user"
          @refresh="refreshAll"
          @logout="logout"
        />
        <SideBar
          :navItems="navItems.filter(i => !i.adminOnly || user?.isAdmin)"
          :counts="counts"
          :isGroupActive="isGroupActive"
        />
        <div class="content-wrapper wl-content">
          <section class="content pt-3">
            <div class="container-fluid">
              <router-view
                :jobs="jobs" :tasks="tasks" :workers="workers" :resources="resources"
                @refresh="refreshAll"
              />
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
