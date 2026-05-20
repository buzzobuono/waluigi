from fastapi import APIRouter, Depends

from waluigi.commons.responses import ok, ko
from waluigi.boss2.config.dependencies import job_service, task_service

router = APIRouter(prefix="/api")


@router.get("/jobs")
async def list_jobs(svc=Depends(job_service)):
    return ok(svc.list())


@router.get("/jobs/{job_id}/tasks")
async def list_job_tasks(job_id: str, svc=Depends(task_service)):
    return ok(svc.list_by_job(job_id))


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str, svc=Depends(job_service)):
    if not svc.cancel(job_id):
        return ko(f"Job '{job_id}' not found or already terminal", status=409)
    return ok({"job_id": job_id, "status": "CANCELLED"})


@router.delete("/jobs/{job_id}")
async def delete_job(job_id: str, svc=Depends(job_service)):
    if not svc.delete(job_id):
        return ko(f"Job '{job_id}' not found or not in terminal state", status=409)
    return ok({"job_id": job_id})
