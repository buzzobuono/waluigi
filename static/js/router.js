// router.js
import Jobs      from './components/Jobs.js';
import Tasks     from './components/Tasks.js';
import Workers   from './components/Workers.js';
import Resources from './components/Resources.js';

const routes = [
  { path: '/',          redirect: '/jobs' },
  { path: '/jobs',      component: Jobs,      meta: { title: 'Jobs' } },
  { path: '/tasks',     component: Tasks,     meta: { title: 'Tasks' } },
  { path: '/workers',   component: Workers,   meta: { title: 'Workers' } },
  { path: '/resources', component: Resources, meta: { title: 'Resources' } },
];

export const router = VueRouter.createRouter({
  history: VueRouter.createWebHistory(),
  routes,
});
