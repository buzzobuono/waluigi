from fastapi import APIRouter, Depends, Request

from waluigi.commons.responses import ok, ko
from waluigi.boss.config.dependencies import (
    namespace_service, task_service, job_service,
    cron_job_service, job_definition_service, task_definition_service,
)

router = APIRouter(
    prefix="/namespaces",
    tags=["Namespaces"]
)


@router.get("")
async def list_namespaces(svc=Depends(namespace_service)):
    return ok(svc.list_namespaces())


@router.get("/{namespace}/overview")
async def namespace_overview(
    namespace: str,
    ns_svc=Depends(namespace_service),
    task_svc=Depends(task_service),
    job_svc=Depends(job_service),
    cron_svc=Depends(cron_job_service),
    jd_svc=Depends(job_definition_service),
    td_svc=Depends(task_definition_service),
):
    if not ns_svc.exists(namespace):
        return ko(f"Namespace '{namespace}' not found", status=404)
    return ok({
        "namespace":        namespace,
        "tasks":            task_svc.list_tasks(namespace=namespace),
        "jobs":             job_svc.list(namespace=namespace),
        "cron_jobs":        cron_svc.list(namespace=namespace),
        "job_definitions":  jd_svc.list(namespace=namespace),
        "task_definitions": td_svc.list(namespace=namespace),
    })


@router.post("")
async def create_namespace(request: Request, svc=Depends(namespace_service)):
    doc = await request.json()
    kind = doc.get("kind")
    if kind != "Namespace":
        return ko("Expected kind: Namespace", status=400)
    meta = doc.get("metadata", {})
    name = meta.get("name", "").strip()
    if not name:
        return ko("metadata.name is required", status=400)
    description = meta.get("description", "")
    return ok(svc.create_namespace(name, description))


@router.post("/{namespace}/_reset")
async def reset_namespace(namespace: str, svc=Depends(namespace_service)):
    if not svc.exists(namespace):
        return ko(f"Namespace '{namespace}' not found", status=404)
    svc.reset_namespace(namespace)
    return ok({"namespace": namespace})


@router.delete("/{namespace}")
async def delete_namespace(namespace: str, svc=Depends(namespace_service)):
    if not svc.exists(namespace):
        return ko(f"Namespace '{namespace}' not found", status=404)
    svc.delete_namespace(namespace)
    return ok({"namespace": namespace})
