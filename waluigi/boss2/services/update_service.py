from __future__ import annotations
from waluigi.boss2.repositories.task_repo import TaskRepository
from waluigi.boss2.repositories.resource_repo import ResourceRepository
from waluigi.boss2.repositories.worker_repo import WorkerRepository


class UpdateService:
    """Handles the worker-callback lifecycle: RUNNING lock, resource/slot release, status update."""

    def __init__(self, task_repo: TaskRepository, resource_repo: ResourceRepository,
                 worker_repo: WorkerRepository):
        self.tasks     = task_repo
        self.resources = resource_repo
        self.workers   = worker_repo

    def handle(self, task_id: str, status: str, namespace: str | None,
               params: str | None, attributes: str | None,
               resources: dict, worker_url: str | None) -> bool:
        """
        Returns False (409) if the RUNNING lock is already held.
        Returns True otherwise.
        """
        if status == "RUNNING":
            if not self.tasks.try_lock(task_id):
                return False

        if status in ("SUCCESS", "FAILED"):
            self.resources.release(resources)
            if worker_url:
                self.workers.release_slot(worker_url)

        self.tasks.update(
            task_id=task_id,
            namespace=namespace or "",
            params=params or "",
            attributes=attributes or "",
            status=status,
        )
        return True
