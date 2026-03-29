import { router }   from './router.js';
import { api }      from './api.js';
import LogModal     from './components/LogModal.js';

const { createApp, ref, computed, provide, onMounted } = Vue;

const App = {
  name: 'App',
  components: { LogModal },

  setup() {
    const jobs      = ref([]);
    const tasks     = ref([]);
    const workers   = ref([]);
    const resources = ref([]);
    const loading   = ref(false);
    const clock     = ref('');
    const logModalRef = ref(null);

    provide('showLogs', (taskId) => {
      logModalRef.value?.show(taskId);
    });

    const counts = computed(() => ({
      jobs:    jobs.value.length,
      tasks:   tasks.value.length,
      workers: workers.value.length,
    }));

    const currentTitle = computed(() => {
      const route = router.currentRoute.value;
      return route.meta?.title || 'Console';
    });

    const navItems = [
      { path: '/jobs',      label: 'Jobs',      icon: 'fa-briefcase', key: 'jobs'    },
      { path: '/tasks',     label: 'Tasks',      icon: 'fa-tasks',     key: 'tasks'   },
      { path: '/workers',   label: 'Workers',    icon: 'fa-server',    key: 'workers' },
      { path: '/resources', label: 'Resources', icon: 'fa-chart-bar',       key: null      },
      { path: '/catalog',   label: 'Catalog',   icon: 'fa-database',       key: null      },
      { path: '/lineage',   label: 'Lineage',   icon: 'fa-project-diagram',key: null      },
    ];

    async function refreshAll() {
      loading.value = true;
      try {
        [jobs.value, tasks.value, workers.value, resources.value] = await Promise.all([
          api.jobs().catch(() => []),
          api.tasks().catch(() => []),
          api.workers().catch(() => []),
          api.resources().catch(() => []),
        ]);
      } finally {
        loading.value = false;
      }
    }
    
    setInterval(() => {
      clock.value = new Date().toLocaleTimeString();
    }, 1000);
    
    onMounted(() => {
      refreshAll();
      setInterval(refreshAll, 10000);
    });

    return {
      jobs, tasks, workers, resources,
      loading, clock, counts, currentTitle,
      navItems, refreshAll, logModalRef,
    };
  },

  template: `
    <div class="wrapper">

      <!-- Navbar -->
      <nav class="main-header navbar navbar-expand navbar-dark wl-navbar">
        <ul class="navbar-nav">
          <li class="nav-item">
            <a class="nav-link" data-widget="pushmenu" href="#" role="button">
              <i class="fas fa-bars"></i>
            </a>
          </li>
        </ul>
        <ul class="navbar-nav ml-auto align-items-center">
          <li class="nav-item mr-3">
            <span class="nav-link wl-clock">{{ clock }}</span>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="#" @click.prevent="refreshAll" title="Refresh">
              <i class="fas fa-sync-alt wl-accent" :class="{ 'fa-spin': loading }"></i>
            </a>
          </li>
        </ul>
      </nav>

      <!-- Sidebar -->
      <aside class="main-sidebar elevation-4 wl-sidebar">
        <a href="/" class="brand-link text-center wl-brand">
          <span class="brand-text font-weight-bold wl-accent" style="font-size:1.1em;">🟣 Waluigi</span>
        </a>
        <div class="sidebar">
          <nav class="mt-2">
            <ul class="nav nav-pills nav-sidebar flex-column" role="menu">
              <li class="nav-item" v-for="item in navItems" :key="item.path">
                <router-link :to="item.path" class="nav-link" active-class="active">
                  <i :class="['nav-icon fas', item.icon]"></i>
                  <p>
                    {{ item.label }}
                    <span v-if="item.key" class="badge badge-secondary right">
                      {{ counts[item.key] }}
                    </span>
                  </p>
                </router-link>
              </li>
            </ul>
          </nav>
        </div>
      </aside>

      <!-- Content -->
      <div class="content-wrapper wl-content">
        <div class="content-header">
          <div class="container-fluid">
            <h1 class="m-0 wl-accent">{{ currentTitle }}</h1>
          </div>
        </div>
        <section class="content">
          <div class="container-fluid">
            <router-view
              :jobs="jobs"
              :tasks="tasks"
              :workers="workers"
              :resources="resources"
              @refresh="refreshAll"
            ></router-view>
          </div>
        </section>
      </div>

      <!-- Log Modal -->
      <log-modal ref="logModalRef"></log-modal>

      <footer class="main-footer wl-footer text-sm">
        <strong>Waluigi Console</strong> — auto-refresh 10s
      </footer>

    </div>
  `
};

const vueApp = createApp(App);
vueApp.use(router);
vueApp.mount('#app');
