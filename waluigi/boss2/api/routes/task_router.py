from fastapi import APIRouter, Depends

from waluigi.commons.responses import ok, ko
from waluigi.boss2.api.schemas import TaskUpdateRequest, LogAppendRequest
from waluigi.boss2.config.dependencies import task_service, update_service, log_service

router = APIRouter(
    prefix="/namespaces/{namespace}/tasks",
    tags=["Tasks"]
)


@router.get("")
async def list_tasks(namespace: str, job_id: str | None = None, svc=Depends(task_service)):
    return ok(svc.list_tasks(namespace=namespace, job_id=job_id))


@router.patch("/{task_id}")
async def update(namespace: str, task_id: str, body: TaskUpdateRequest, svc=Depends(update_service)):
    success = svc.handle(
        namespace=namespace,
        task_id=task_id,
        status=body.status,
        params=body.params,
        attributes=body.attributes,
        resources=body.resources,
        worker_url=body.worker_url,
    )
    if not success:
        return ko("Task already RUNNING — duplicate lock attempt", status=409)
    return ok({"id": task_id, "status": body.status})


@router.post("/{task_id}/_reset")
async def reset_task(namespace: str, task_id: str, svc=Depends(task_service)):
    svc.reset(namespace, task_id)
    return ok({"task_id": task_id, "status": "PENDING"})


@router.delete("/{task_id}")
async def delete_task(namespace: str, task_id: str, svc=Depends(task_service)):
    svc.delete(namespace, task_id)
    return ok({"task_id": task_id})


@router.post("/{task_id}/logs", status_code=201)
async def append_logs(namespace: str, task_id: str, body: LogAppendRequest, svc=Depends(log_service)):
    if not body.logs:
        return ok({"task_id": task_id, "appended": 0})
    svc.append(namespace, task_id, body.logs, body.worker_id)
    return ok({"task_id": task_id, "appended": len(body.logs)}, status=201)


@router.get("/{task_id}/logs")
async def get_logs(namespace: str, task_id: str, limit: int = 20, svc=Depends(log_service)):
    return ok(svc.get(namespace, task_id, limit))
