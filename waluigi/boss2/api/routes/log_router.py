from fastapi import APIRouter, Depends

from waluigi.commons.responses import ok, ko
from waluigi.boss2.api.schemas import LogAppendRequest
from waluigi.boss2.config.dependencies import log_service

router = APIRouter(prefix="/api")


@router.post("/logs/{task_id}", status_code=201)
async def append_logs(task_id: str, body: LogAppendRequest, svc=Depends(log_service)):
    if not body.logs:
        return ok({"task_id": task_id, "appended": 0})
    svc.append(task_id, body.logs, body.worker_id)
    return ok({"task_id": task_id, "appended": len(body.logs)}, status=201)


@router.get("/logs/{task_id}")
async def get_logs(task_id: str, limit: int = 20, svc=Depends(log_service)):
    return ok(svc.get(task_id, limit))
