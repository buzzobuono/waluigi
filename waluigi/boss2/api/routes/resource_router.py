from fastapi import APIRouter, Depends, Request

from waluigi.commons.responses import ok, ko
from waluigi.boss2.config.dependencies import resource_service

router = APIRouter(
    prefix="/resources",
    tags=["Resources"]
)

@router.get("")
async def list_resources(svc=Depends(resource_service)):
    return ok(svc.list())


@router.post("")
async def apply_resources(request: Request, svc=Depends(resource_service)):
    doc = await request.json()
    if not doc or doc.get("kind") != "ClusterResources":
        return ko("Expected kind: ClusterResources", status=400)
    spec = doc.get("spec", {})
    if not spec:
        return ko("spec is empty", status=400)
    success, msg = svc.apply(spec)
    if not success:
        return ko(msg, status=409)
    return ok(None, msg)
