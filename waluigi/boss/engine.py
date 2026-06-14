from __future__ import annotations
import json
import logging
import httpx

from waluigi.boss.repositories.task_repo import TaskRepository
from waluigi.boss.repositories.task_deps_repo import TaskDepsRepository
from waluigi.boss.repositories.worker_repo import WorkerRepository
from waluigi.boss.repositories.resource_repo import ResourceRepository
from waluigi.boss.repositories.task_definition_repo import TaskDefinitionRepository
from waluigi.commons.dag import DAGSpec

logger = logging.getLogger("waluigi")

_DEFAULT_RESOURCES = {}


class BossEngine:

    def __init__(self, task_repo: TaskRepository,
                 worker_repo: WorkerRepository,
                 resource_repo: ResourceRepository,
                 task_definition_repo: TaskDefinitionRepository | None = None,
                 task_deps_repo: TaskDepsRepository | None = None,
                 secret_repo=None):
        self.tasks            = task_repo
        self.task_deps        = task_deps_repo
        self.workers          = worker_repo
        self.resources        = resource_repo
        self.task_definitions = task_definition_repo
        self.secrets          = secret_repo

    # ── Registration ─────────────────────────────────────────────────────────

    def register_job(self, namespace: str, job_id: str, spec: DAGSpec) -> None:
        """Register all tasks from a flat DAGSpec. Each task is registered once."""
        for task in spec.all_tasks():
            self.tasks.register(
                namespace=namespace,
                task_id=task.id,
                parent_id=None,
                params=task.hash(task.params),
                attributes=task.hash(task.attributes),
                job_id=job_id,
            )
            if self.task_deps:
                for req_id in task.requires:
                    self.task_deps.add(namespace, task.id, req_id)

    def register_worker(self, url: str, max_slots: int, free_slots: int,
                        affinity: list[str] | None = None) -> None:
        self.workers.register(url, max_slots, free_slots, affinity)

    # ── Planner ───────────────────────────────────────────────────────────────

    def build(self, namespace: str, job_metadata: dict,
              spec: DAGSpec, task_id: str | None = None,
              _memo: dict | None = None) -> bool | None | str:
        """
        Recursively plan and dispatch tasks starting from the terminal task.

        Uses a memo dict to deduplicate shared dependencies (diamond patterns):
        each task is evaluated at most once per planning cycle.

        Returns:
          True    — task (and all deps) are SUCCESS
          False   — task is blocked; retry on next tick
          None    — task or dep FAILED
          "PAUSE" — all workers saturated
        """
        if task_id is None:
            task_id = spec.terminal().id
        if _memo is None:
            _memo = {}
        if task_id in _memo:
            return _memo[task_id]

        task = spec.task(task_id)
        params_hash = task.hash(task.params)
        status = self.tasks.get_status(namespace, task_id, params_hash)

        if status == "FAILED":
            logger.info(f"🛑 {task_id} failed — propagating.")
            _memo[task_id] = None
            return None
        if status == "RUNNING":
            _memo[task_id] = False
            return False
        if status == "SUCCESS":
            _memo[task_id] = True
            return True

        all_deps_ready = True
        for dep in spec.deps_of(task_id):
            res = self.build(namespace, job_metadata, spec, dep.id, _memo)
            if res == "PAUSE":
                return "PAUSE"
            if res is None:
                _memo[task_id] = None
                return None
            if res is False:
                all_deps_ready = False

        if not all_deps_ready:
            self._set_status(namespace, task, "PENDING")
            _memo[task_id] = False
            return False

        # Re-check: another boss may have moved the task forward
        status = self.tasks.get_status(namespace, task_id, params_hash)
        if status in ("RUNNING", "READY"):
            _memo[task_id] = False
            return False

        # Resolve taskRef against DB-defined TaskDefinitions
        if task.type and self.task_definitions is not None:
            defn = self.task_definitions.get(namespace, task.type)
            if defn is None:
                logger.error(f"❌ Unknown task type '{task.type}' — no TaskDefinition found in namespace '{namespace}'")
                self._set_status(namespace, task, "FAILED")
                _memo[task_id] = None
                return None
            spec_def = defn["spec"]
            task.command  = spec_def.get("command", "")
            task.script   = spec_def.get("script")
            task.affinity = spec_def.get("affinity", [])
            task.type = None

        task_resources = getattr(task, "resources", _DEFAULT_RESOURCES)

        try:
            if not self.resources.acquire(namespace, task_resources):
                logger.info(f"⏳ {task_id} — not enough resources, will retry")
                _memo[task_id] = False
                return False

            self._set_status(namespace, task, "READY")
            dispatch_result = self._dispatch(namespace, job_metadata, task)

            if dispatch_result == "WORKERS_SATURATED":
                self.resources.release(namespace, task_resources)
                self._set_status(namespace, task, "PENDING")
                return "PAUSE"

            if dispatch_result == "FATAL_ERROR":
                self.resources.release(namespace, task_resources)
                self._set_status(namespace, task, "FAILED")
                _memo[task_id] = None
                return None

            if dispatch_result == "RETRY":
                self.resources.release(namespace, task_resources)
                self._set_status(namespace, task, "PENDING")
                _memo[task_id] = False
                return False

            logger.info(f"🚀 {task_id} dispatched")

        except Exception as e:
            self.resources.release(namespace, task_resources)
            logger.error(f"❌ {task_id} error: {e}")
            self._set_status(namespace, task, "PENDING")

        _memo[task_id] = False
        return False

    # ── Internals ─────────────────────────────────────────────────────────────

    def _set_status(self, namespace: str, task, status: str) -> None:
        self.tasks.update(
            namespace=namespace,
            task_id=task.id,
            params=task.hash(task.params),
            attributes=task.hash(task.attributes),
            status=status,
        )

    def _dispatch(self, namespace: str, job_metadata: dict, task) -> str:
        secrets = {}
        if self.secrets is not None:
            try:
                secrets = self.secrets.get_all_for_namespace(namespace)
            except Exception:
                pass
        payload = {
            "command":     task.command,
            "script":      task.script,
            "id":          task.id,
            "job_id":      job_metadata.get("name", ""),
            "params":      vars(task.params),
            "params_hash": task.hash(task.params),
            "attributes":  vars(task.attributes),
            "config":      task.config,
            "resources":   task.resources,
            "secrets":     secrets,
        }

        available = self.workers.get_available()
        if not available:
            return "WORKERS_SATURATED"

        task_affinity = set(getattr(task, 'affinity', None) or [])
        logger.info(f"🎯 {task.id} — affinity={task_affinity or '(any)'}")
        if task_affinity:
            available = [
                w for w in available
                if task_affinity.issubset(set(json.loads(w.get('affinity') or '[]')))
            ]
            if not available:
                logger.info(f"⏳ {task.id} — no worker with affinity {task_affinity}")
                return "RETRY"

        all_busy = True
        for worker in available:
            url = worker["url"]
            if not self.workers.acquire_slot(url):
                continue
            try:
                r = httpx.post(f"{url}/namespaces/{namespace}/dispatch", json=payload, timeout=10)
                if r.status_code == 202:
                    logger.info(f"🚀 Dispatched {task.id} → {url}")
                    return "SUCCESS"
                self.workers.release_slot(url)
                if r.status_code == 400:
                    logger.error(f"💥 Fatal 400 from {url} for {task.id}")
                    return "FATAL_ERROR"
                if r.status_code != 429:
                    all_busy = False
            except Exception:
                logger.warning(f"Worker {url} unreachable — removing.")
                self.workers.delete(url)
                all_busy = False

        return "WORKERS_SATURATED" if all_busy else "RETRY"
