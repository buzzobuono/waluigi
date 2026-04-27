import { router }  from './router.js';
import { api }     from './api.js';
import SideBar     from './components/SideBar.js';
import NavBar      from './components/NavBar.js'; // Nuovo import

const { createApp, ref, computed, onMounted } = Vue;

const App = {
  name: 'App',
  components: { 
    SideBar, 
    NavBar 
  },
  
  setup() {
    const jobs = ref([]), tasks = ref([]), workers = ref([]), resources = ref([]);
    const loading = ref(false);

    const counts = computed(() => ({
      jobs: jobs.value.length,
      tasks: tasks.value.length,
      workers: workers.value.length,
      namespaces: new Set(tasks.value.map(t => t.namespace).filter(ns => ns)).size
    }));

    const navItems = [
      { path: '/namespaces', label: 'Namespaces', icon: 'fa-layer-group', key: 'namespaces' },
      { 
        label: 'Operations', icon: 'fa-cogs', 
        children: [
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
          { path: '/catalog', label: 'Catalog', icon: 'fa-table',           key: null },
          { path: '/lineage', label: 'Lineage', icon: 'fa-project-diagram', key: null }
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
    
    /*onMounted(() => {
      refreshAll();
      setInterval(refreshAll, 10000);
    });*/

    return { 
      loading, counts, navItems, refreshAll, isGroupActive, 
      currentTitle: computed(() => router.currentRoute.value.meta?.title || 'Console'),
      jobs, tasks, workers, resources 
    };
  },
  
  template: `
    <div class="wrapper">
      
      <NavBar 
        :title="currentTitle"
        :loading="loading" 
        @refresh="refreshAll" 
      />

      <SideBar 
        :navItems="navItems" 
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
  `
};

const vueApp = createApp(App);
vueApp.use(router);
vueApp.mount('#app');
