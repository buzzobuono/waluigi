from fastapi import APIRouter, Depends, Request

from waluigi.commons.responses import ok, ko
from waluigi.boss2.config.dependencies import resource_service, namespaces_repository

router = APIRouter(
    prefix="/namespaces/{namespace}/resources",
    tags=["Resources"],
)


@router.get("")
async def list_resources(namespace: str, svc=Depends(resource_service)):
    return ok(svc.list(namespace))


@router.post("")
async def apply_resources(namespace: str, request: Request,
                          svc=Depends(resource_service),
                          ns_repo=Depends(namespaces_repository)):
    if not ns_repo.exists(namespace):
        return ko(f"Namespace '{namespace}' not found", status=404)
    doc = await request.json()
    if not doc or doc.get("kind") not in ("NamespaceResources", "ClusterResources"):
        return ko("Expected kind: NamespaceResources", status=400)
    spec = doc.get("spec", {})
    if not isinstance(spec, dict):
        return ko("spec must be a dict of resource_name: amount", status=400)
    success, msg = svc.apply(namespace, spec)
    if not success:
        return ko(msg, status=409)
    return ok(None, msg)
