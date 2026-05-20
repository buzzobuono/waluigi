from fastapi import APIRouter, Depends

from waluigi.commons.responses import ok, ko
from waluigi.boss2.api.schemas import TaskUpdateRequest
from waluigi.boss2.config.dependencies import update_service

router = APIRouter()


@router.post("/update")
async def update(body: TaskUpdateRequest, svc=Depends(update_service)):
    success = svc.handle(
        task_id=body.id,
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
