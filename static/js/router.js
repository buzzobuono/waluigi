import Jobs      from './components/Jobs.js';
import Tasks     from './components/Tasks.js';
import Namespaces    from './components/Namespaces.js';
import Workers   from './components/Workers.js';
import Resources from './components/Resources.js';
import Catalog   from './components/Catalog.js';
import Lineage   from './components/Lineage.js';
import JobDag    from './components/JobDag.js';
import DatasetPreview    from './components/DatasetPreview.js';

const routes = [
  { path: '/',          redirect: '/jobs' },
  { path: '/jobs',      component: Jobs,      meta: { title: 'Jobs' } },
  { path: '/namespaces', component: Namespaces, meta: { title: 'Namespaces' } },
  { path: '/tasks',     component: Tasks,     meta: { title: 'Tasks' } },
  { path: '/tasks/:namespace+', component: Tasks, meta: { title: 'Tasks' } },
  { path: '/workers',   component: Workers,   meta: { title: 'Workers' } },
  { path: '/resources', component: Resources, meta: { title: 'Resources' } },
  { path: '/catalog',   component: Catalog,   meta: { title: 'Catalog' } },
  { path: '/datasets/:id+/:version', component: DatasetPreview, meta: { title: 'Dataset Preview'} },
  { path: '/lineage',   component: Lineage,   meta: { title: 'Lineage' } },
  { path: '/jobs/:jobId', component: JobDag,  meta: { title: 'Job DAG' } }
];

export const router = VueRouter.createRouter({
  history: VueRouter.createWebHistory(),
  routes,
});
