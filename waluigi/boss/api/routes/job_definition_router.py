from fastapi import APIRouter, Depends, Request

from waluigi.commons.responses import ok, ko
from waluigi.boss.config.dependencies import job_definition_service, namespaces_repository

router = APIRouter(
    prefix="/namespaces/{namespace}/job-definitions",
    tags=["Job Definitions"],
)


@router.get("")
async def list_definitions(namespace: str, svc=Depends(job_definition_service)):
    return ok(svc.list(namespace))


@router.get("/{id}")
async def get_definition(namespace: str, id: str, svc=Depends(job_definition_service)):
    d = svc.get(namespace, id)
    if d is None:
        return ko(f"JobDefinition '{id}' not found", status=404)
    return ok(d)


@router.post("")
async def upsert_definition(
    namespace: str,
    request: Request,
    svc=Depends(job_definition_service),
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
    if not spec.get("tasks"):
        return ko("spec.tasks must be a non-empty list", status=400)
    meta["namespace"] = namespace
    svc.upsert(namespace, name, meta, spec)
    return ok({"id": name, "namespace": namespace})


@router.delete("/{id}")
async def delete_definition(namespace: str, id: str, svc=Depends(job_definition_service)):
    if not svc.delete(namespace, id):
        return ko(f"JobDefinition '{id}' not found", status=404)
    return ok({"id": id})
