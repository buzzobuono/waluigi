from fastapi import APIRouter, Depends

from waluigi.commons.responses import ok
from waluigi.boss.api.schemas import WorkerRegisterRequest
from waluigi.boss.config.dependencies import worker_service, boss_engine

router = APIRouter(
    prefix="/workers",
    tags=["Workers"]
)

@router.get("")
async def list_workers(svc=Depends(worker_service)):
    return ok(svc.list())

@router.post("")
async def register_worker(body: WorkerRegisterRequest, engine=Depends(boss_engine)):
    engine.register_worker(body.url, body.max_slots, body.free_slots, body.affinity)
    return ok({"url": body.url})


