from fastapi import APIRouter, Depends, Request

from waluigi.commons.responses import ok, ko
from waluigi.boss.config.dependencies import cron_job_service, namespaces_repository

router = APIRouter(
    prefix="/namespaces/{namespace}/cron-jobs",
    tags=["Cron Jobs"],
)


@router.get("")
async def list_cron_jobs(namespace: str, svc=Depends(cron_job_service)):
    return ok(svc.list(namespace))


@router.get("/{id}")
async def get_cron_job(namespace: str, id: str, svc=Depends(cron_job_service)):
    cj = svc.get(namespace, id)
    if cj is None:
        return ko(f"CronJob '{id}' not found", status=404)
    return ok(cj)


@router.post("")
async def upsert_cron_job(
    namespace: str,
    request: Request,
    svc=Depends(cron_job_service),
    ns_repo=Depends(namespaces_repository),
):
    if not ns_repo.exists(namespace):
        return ko(f"Namespace '{namespace}' not found", status=404)
    data = await request.json()
    meta = dict(data.get("metadata", {}))
    name = meta.get("name", "").strip()
    if not name:
        return ko("metadata.name is required", status=400)
    spec = data.get("spec", {})
    if not spec.get("schedule"):
        return ko("spec.schedule is required", status=400)
    if not (spec.get("jobRef") or {}).get("name"):
        return ko("spec.jobRef.name is required", status=400)
    enabled = spec.get("enabled", True)
    svc.upsert(namespace, name, spec, enabled=bool(enabled))
    return ok({"id": name, "namespace": namespace})


@router.delete("/{id}")
async def delete_cron_job(namespace: str, id: str, svc=Depends(cron_job_service)):
    if not svc.delete(namespace, id):
        return ko(f"CronJob '{id}' not found", status=404)
    return ok({"id": id})


@router.post("/{id}/_enable")
async def enable_cron_job(namespace: str, id: str, svc=Depends(cron_job_service)):
    if not svc.set_enabled(namespace, id, True):
        return ko(f"CronJob '{id}' not found", status=404)
    return ok({"id": id, "enabled": True})


@router.post("/{id}/_disable")
async def disable_cron_job(namespace: str, id: str, svc=Depends(cron_job_service)):
    if not svc.set_enabled(namespace, id, False):
        return ko(f"CronJob '{id}' not found", status=404)
    return ok({"id": id, "enabled": False})
