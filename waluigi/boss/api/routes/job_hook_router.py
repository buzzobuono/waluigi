from fastapi import APIRouter, Depends, Request

from waluigi.commons.responses import ok, ko
from waluigi.boss.config.dependencies import job_hook_service, namespaces_repository

router = APIRouter(
    prefix="/namespaces/{namespace}/job-hooks",
    tags=["Job Hooks"],
)


@router.get("")
async def list_job_hooks(namespace: str, svc=Depends(job_hook_service)):
    return ok(svc.list(namespace))


@router.get("/{id}")
async def get_job_hook(namespace: str, id: str, svc=Depends(job_hook_service)):
    h = svc.get(namespace, id)
    if h is None:
        return ko(f"JobHook '{id}' not found", status=404)
    return ok(h)


@router.post("")
async def upsert_job_hook(
    namespace: str,
    request: Request,
    svc=Depends(job_hook_service),
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
    watch = spec.get("watch") or {}
    if not watch.get("job"):
        return ko("spec.watch.job is required", status=400)
    if not watch.get("on"):
        return ko("spec.watch.on is required (list of events, e.g. [success, failure])", status=400)
    trigger = spec.get("trigger") or {}
    if not (trigger.get("jobRef") or {}).get("name"):
        return ko("spec.trigger.jobRef.name is required", status=400)
    enabled = spec.get("enabled", True)
    svc.upsert(namespace, name, spec, enabled=bool(enabled))
    return ok({"id": name, "namespace": namespace})


@router.delete("/{id}")
async def delete_job_hook(namespace: str, id: str, svc=Depends(job_hook_service)):
    if not svc.delete(namespace, id):
        return ko(f"JobHook '{id}' not found", status=404)
    return ok({"id": id})


@router.post("/{id}/_enable")
async def enable_job_hook(namespace: str, id: str, svc=Depends(job_hook_service)):
    if not svc.set_enabled(namespace, id, True):
        return ko(f"JobHook '{id}' not found", status=404)
    return ok({"id": id, "enabled": True})


@router.post("/{id}/_disable")
async def disable_job_hook(namespace: str, id: str, svc=Depends(job_hook_service)):
    if not svc.set_enabled(namespace, id, False):
        return ko(f"JobHook '{id}' not found", status=404)
    return ok({"id": id, "enabled": False})
