from fastapi import APIRouter, Depends

from waluigi.commons.responses import ok
from waluigi.boss2.api.schemas import WorkerRegisterRequest
from waluigi.boss2.config.dependencies import worker_service, boss_engine

router = APIRouter()


@router.post("/worker/register")
async def register_worker(body: WorkerRegisterRequest, engine=Depends(boss_engine)):
    engine.register_worker(body.url, body.max_slots, body.free_slots)
    return ok({"url": body.url})


@router.get("/api/workers")
async def list_workers(svc=Depends(worker_service)):
    return ok(svc.list())
