from fastapi import APIRouter, Depends, Request

from waluigi.commons.responses import ok, ko
from waluigi.boss.config.dependencies import namespace_service

router = APIRouter(
    prefix="/namespaces",
    tags=["Namespaces"]
)


@router.get("")
async def list_namespaces(svc=Depends(namespace_service)):
    return ok(svc.list_namespaces())


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
