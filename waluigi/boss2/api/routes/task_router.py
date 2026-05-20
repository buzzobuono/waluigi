from fastapi import APIRouter, Depends

from waluigi.commons.responses import ok
from waluigi.boss2.config.dependencies import task_service

router = APIRouter(prefix="/api")


@router.get("/tasks")
async def list_tasks(svc=Depends(task_service)):
    return ok(svc.list())


@router.get("/tasks/{job_id}/by-job")
async def list_tasks_by_job(job_id: str, svc=Depends(task_service)):
    return ok(svc.list_by_job(job_id))


@router.get("/namespaces")
async def list_namespaces(svc=Depends(task_service)):
    return ok(svc.list_namespaces())


@router.post("/reset/task/{task_id}")
async def reset_task(task_id: str, svc=Depends(task_service)):
    svc.reset(task_id)
    return ok({"task_id": task_id, "status": "PENDING"})


@router.post("/reset/namespace/{namespace}")
async def reset_namespace(namespace: str, svc=Depends(task_service)):
    target = None if namespace == "None" else namespace
    svc.reset_namespace(target)
    return ok({"namespace": target})


@router.post("/delete/task/{task_id}")
async def delete_task(task_id: str, svc=Depends(task_service)):
    svc.delete(task_id)
    return ok({"task_id": task_id})


@router.post("/delete/namespace/{namespace}")
async def delete_namespace(namespace: str, svc=Depends(task_service)):
    target = None if namespace == "None" else namespace
    svc.delete_namespace(target)
    return ok({"namespace": target})
