import Login         from './components/Login.js';
import AdminUsers    from './components/AdminUsers.js';
import Jobs          from './components/Jobs.js';
import Tasks         from './components/Tasks.js';
import Namespaces    from './components/Namespaces.js';
import Workers       from './components/Workers.js';
import Resources     from './components/Resources.js';
import Catalog       from './components/Catalog.js';
import Sources       from './components/Sources.js';
import Lineage       from './components/Lineage.js';
import JobDag        from './components/JobDag.js';
import DatasetPreview  from './components/DatasetPreview.js';
import DatasetSchema   from './components/DatasetSchema.js';
import DatasetDQ       from './components/DatasetDQ.js';
import Expectations    from './components/Expectations.js';
import DatasetCharts   from './components/DatasetCharts.js';
import Dashboard       from './components/Dashboard.js';

const AUTH_KEY = 'waluigi_auth_token';

const routes = [
  { path: '/login', component: Login, meta: { title: 'Login', public: true } },
  { path: '/',          redirect: '/dashboard' },
  { path: '/jobs',      component: Jobs,      meta: { title: 'Jobs',       requiresAuth: true } },
  { path: '/namespaces', component: Namespaces, meta: { title: 'Namespaces', requiresAuth: true } },
  { path: '/tasks',     component: Tasks,     meta: { title: 'Tasks',      requiresAuth: true } },
  { path: '/tasks/:namespace+', component: Tasks, meta: { title: 'Tasks', requiresAuth: true } },
  { path: '/workers',   component: Workers,   meta: { title: 'Workers',    requiresAuth: true } },
  { path: '/resources', component: Resources, meta: { title: 'Resources',  requiresAuth: true } },
  { path: '/catalog',   component: Catalog,   meta: { title: 'Catalog',    requiresAuth: true } },
  { path: '/sources',   component: Sources,   meta: { title: 'Sources',    requiresAuth: true } },
  { path: '/datasets/:id+/:version', component: DatasetPreview, meta: { title: 'Dataset Preview', requiresAuth: true } },
  { path: '/schema/:id+',            component: DatasetSchema,  meta: { title: 'Schema',          requiresAuth: true } },
  { path: '/dq/:id+/:version',       component: DatasetDQ,      meta: { title: 'Data Quality',    requiresAuth: true } },
  { path: '/chart/:id+/:cid(\\d+)',  component: DatasetCharts,  meta: { title: 'Chart',           requiresAuth: true } },
  { path: '/dashboard',              component: Dashboard,      meta: { title: 'Dashboard',       requiresAuth: true } },
  { path: '/dq/rules',  component: Expectations, meta: { title: 'DQ Rules', requiresAuth: true } },
  { path: '/lineage',   component: Lineage,   meta: { title: 'Lineage',    requiresAuth: true } },
  { path: '/jobs/:jobId',   component: JobDag,    meta: { title: 'Job DAG',    requiresAuth: true } },
  { path: '/admin/users',   component: AdminUsers, meta: { title: 'Users',      requiresAuth: true } },
];

export const router = VueRouter.createRouter({
  history: VueRouter.createWebHistory(),
  routes,
});

router.beforeEach((to) => {
  const token = localStorage.getItem(AUTH_KEY);
  if (to.meta.requiresAuth && !token) return '/login';
  if (to.path === '/login' && token)  return '/dashboard';
});
