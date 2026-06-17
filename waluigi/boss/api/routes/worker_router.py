from typing import Optional

import httpx
from fastapi import APIRouter, Depends, Query

from waluigi.commons.responses import ok, ko
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


@router.post("/_prune",
    summary="Prune ghost workers",
    description=(
        "Pings every registered worker. Workers that do not respond are removed from the DB. "
        "Returns the list of removed URLs."
    )
)
async def prune_workers(svc=Depends(worker_service)):
    workers = svc.list()
    removed = []
    async with httpx.AsyncClient(timeout=3.0) as client:
        for w in workers:
            url = w["url"]
            try:
                r = await client.get(f"{url}/slots")
                if r.status_code < 500:
                    continue
            except Exception:
                pass
            svc.remove(url)
            removed.append(url)
    return ok({"removed": removed, "count": len(removed)})


@router.delete("/prepare",
    summary="Clear prepare directory on workers",
    description=(
        "Sends DELETE /prepare to all registered workers (or a single target). "
        "Workers that are busy return a 'busy' status; unreachable workers are reported separately."
    )
)
async def prune_prepare(
    target: Optional[str] = Query(None, description="Worker URL to target (default: all)"),
    svc=Depends(worker_service),
):
    workers = svc.list()
    if target:
        workers = [w for w in workers if w["url"] == target]
        if not workers:
            return ko(f"Worker '{target}' not found", 404)

    results = []
    async with httpx.AsyncClient(timeout=5.0) as client:
        for w in workers:
            url = w["url"]
            try:
                r = await client.delete(f"{url}/prepare")
                body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                if r.status_code == 422:
                    msgs = (body.get("diagnostic") or {}).get("messages", ["busy"])
                    results.append({"url": url, "status": "busy", "message": msgs[0] if msgs else "busy"})
                elif r.status_code == 200:
                    data = body.get("data") or {}
                    results.append({"url": url, "status": "cleared",
                                    "cleared_bytes": data.get("cleared_bytes", 0)})
                else:
                    results.append({"url": url, "status": "error", "code": r.status_code})
            except Exception as e:
                results.append({"url": url, "status": "unreachable", "message": str(e)})

    return ok(results)


