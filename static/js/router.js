import Jobs      from './components/Jobs.js';
import Tasks     from './components/Tasks.js';
import Workers   from './components/Workers.js';
import Resources from './components/Resources.js';
import Catalog   from './components/Catalog.js';
import Lineage   from './components/Lineage.js';

const routes = [
  { path: '/',          redirect: '/jobs' },
  { path: '/jobs',      component: Jobs,      meta: { title: 'Jobs' } },
  { path: '/tasks',     component: Tasks,     meta: { title: 'Tasks' } },
  { path: '/workers',   component: Workers,   meta: { title: 'Workers' } },
  { path: '/resources', component: Resources, meta: { title: 'Resources' } },
  { path: '/catalog',   component: Catalog,   meta: { title: 'Catalog' } },
  { path: '/lineage',   component: Lineage,   meta: { title: 'Lineage' } },
];

export const router = VueRouter.createRouter({
  history: VueRouter.createWebHistory(),
  routes,
});
