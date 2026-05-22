from fastapi import APIRouter, Depends

from waluigi.commons.responses import ok
from waluigi.boss2.api.schemas import TaskUpdateRequest
from waluigi.boss2.config.dependencies import task_service, update_service

router = APIRouter(
    prefix="",
    tags=["Running Tasks"]
)

@router.get("/tasks")
async def list_tasks(svc=Depends(task_service)):
    return ok(svc.list())

@router.get("/namespaces")
async def list_namespaces(svc=Depends(task_service)):
    return ok(svc.list_namespaces())

@router.post("/namespaces/{namespace}/_reset")
async def reset_namespace(namespace: str, svc=Depends(task_service)):
    target = None if namespace == "None" else namespace
    svc.reset_namespace(target)
    return ok({"namespace": target})

@router.delete("/namespaces/{namespace}")
async def delete_namespace(namespace: str, svc=Depends(task_service)):
    target = None if namespace == "None" else namespace
    svc.delete_namespace(target)
    return ok({"namespace": target})

@router.get("/jobs/{job_id}/tasks")
async def list_tasks_by_job(job_id: str, svc=Depends(task_service)):
    return ok(svc.list_by_job(job_id))
    
@router.post("/tasks/{task_id}/_reset")
async def reset_task(task_id: str, svc=Depends(task_service)):
    svc.reset(task_id)
    return ok({"task_id": task_id, "status": "PENDING"})

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
    return ok({"id": body.id, "status": body.status})

@router.delete("/tasks/{task_id}")
async def delete_task(task_id: str, svc=Depends(task_service)):
    svc.delete(task_id)
    return ok({"task_id": task_id})

