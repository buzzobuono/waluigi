from __future__ import annotations
import logging
import httpx

from waluigi.boss2.repositories.task_repo import TaskRepository
from waluigi.boss2.repositories.worker_repo import WorkerRepository
from waluigi.boss2.repositories.resource_repo import ResourceRepository

logger = logging.getLogger("waluigi")

_DEFAULT_RESOURCES = {"coin": 1.0}


class BossEngine:

    def __init__(self, task_repo: TaskRepository, worker_repo: WorkerRepository,
                 resource_repo: ResourceRepository):
        self.tasks     = task_repo
        self.workers   = worker_repo
        self.resources = resource_repo

    # ── Registration (called at submit time) ──────────────────────────────────

    def register_job(self, job_id: str, task, parent_id: str | None) -> None:
        self.tasks.register(
            task_id=task.id,
            namespace=task.namespace,
            parent_id=parent_id,
            params=task.hash(task.params),
            attributes=task.hash(task.attributes),
            job_id=job_id,
        )
        for dep in task.requires():
            self.register_job(job_id, dep, task.id)

    def register_worker(self, url: str, max_slots: int, free_slots: int) -> None:
        self.workers.register(url, max_slots, free_slots)

    # ── Planner ───────────────────────────────────────────────────────────────

    def build(self, job_metadata: dict, task, parent_id) -> bool | None | str:
        """
        Recursively plan and dispatch a task and its dependencies.

        Returns:
          True        — task (and all deps) are SUCCESS
          False       — task is blocked; retry on next tick
          None        — task or dep FAILED; propagate failure upward
          "PAUSE"     — all workers saturated; stop this planning cycle
        """
        params_hash = task.hash(task.params)
        status = self.tasks.get_status(task.id, params_hash)

        if status == "FAILED":
            logger.info(f"🛑 {task.id} failed — propagating.")
            return None
        if status == "RUNNING":
            return False
        if status == "SUCCESS":
            return True

        all_deps_ready = True
        for dep in task.requires():
            res = self.build(job_metadata=job_metadata, task=dep, parent_id=task.id)
            if res == "PAUSE":
                return "PAUSE"
            if res is None:
                return None
            if res is False:
                all_deps_ready = False

        if not all_deps_ready:
            self._set_status(task, "PENDING")
            return False

        # Re-check: another boss may have moved the task forward in the meantime
        status = self.tasks.get_status(task.id, params_hash)
        if status in ("RUNNING", "READY"):
            return False

        task_resources = getattr(task, "resources", _DEFAULT_RESOURCES)

        try:
            if not self.resources.acquire(task_resources):
                logger.info(f"⏳ {task.id} — not enough resources, will retry")
                return False

            # Mark as READY before dispatching
            self._set_status(task, "READY")

            dispatch_result = self._dispatch(job_metadata, task)

            if dispatch_result == "WORKERS_SATURATED":
                self.resources.release(task_resources)
                self._set_status(task, "PENDING")
                return "PAUSE"

            if dispatch_result == "FATAL_ERROR":
                self.resources.release(task_resources)
                self._set_status(task, "FAILED")
                return None

            if dispatch_result == "RETRY":
                self.resources.release(task_resources)
                self._set_status(task, "PENDING")
                return False

            logger.info(f"🚀 {task.id} dispatched")

        except Exception as e:
            self.resources.release(task_resources)
            logger.error(f"❌ {task.id} error: {e}")
            self._set_status(task, "PENDING")

        return False

    # ── Internals ─────────────────────────────────────────────────────────────

    def _set_status(self, task, status: str) -> None:
        self.tasks.update(
            task_id=task.id,
            namespace=task.namespace,
            params=task.hash(task.params),
            attributes=task.hash(task.attributes),
            status=status,
        )

    def _dispatch(self, job_metadata: dict, task) -> str:
        payload = {
            "workdir":     job_metadata.get("workdir", "/work"),
            "type":        task.type,
            "command":     task.command,
            "script":      task.script,
            "id":          task.id,
            "job_id":      job_metadata.get("name", ""),
            "params":      vars(task.params),
            "params_hash": task.hash(task.params),
            "attributes":  vars(task.attributes),
            "config":      task.config,
            "resources":   task.resources,
            "namespace":   task.namespace,
        }

        available = self.workers.get_available()
        if not available:
            return "WORKERS_SATURATED"

        all_busy = True
        for worker in available:
            url = worker["url"]
            if not self.workers.acquire_slot(url):
                continue
            try:
                r = httpx.post(f"{url}/execute", json=payload, timeout=10)
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
