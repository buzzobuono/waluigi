from fastapi import APIRouter, Depends

from waluigi.commons.responses import ok, ko
from waluigi.boss2.api.schemas import TaskUpdateRequest
from waluigi.boss2.config.dependencies import task_service, update_service

router = APIRouter(
    prefix="",
    tags=["Tasks"]
)

@router.get("/tasks")
async def list_tasks(job_id: str | None = None, namespace: str | None = None, svc=Depends(task_service)):
    return ok(svc.list_tasks(job_id=job_id, namespace=namespace))
    

@router.patch("/tasks/{task_id}")
async def update(task_id: str, body: TaskUpdateRequest, svc=Depends(update_service)):
    success = svc.handle(
        task_id=task_id,
        status=body.status,
        namespace=body.namespace,
        params=body.params,
        attributes=body.attributes,
        resources=body.resources,
        worker_url=body.worker_url,
    )
    if not success:
        return ko("Task already RUNNING — duplicate lock attempt", status=409)
    return ok({"id": task_id, "status": body.status})

@router.post("/tasks/{task_id}/_reset")
async def reset_task(task_id: str, svc=Depends(task_service)):
    svc.reset(task_id)
    return ok({"task_id": task_id, "status": "PENDING"})

@router.delete("/tasks/{task_id}")
async def delete_task(task_id: str, svc=Depends(task_service)):
    svc.delete(task_id)
    return ok({"task_id": task_id})

