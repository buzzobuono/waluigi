from fastapi import APIRouter, Depends

from waluigi.commons.responses import ok
from waluigi.boss2.config.dependencies import namespace_service

router = APIRouter(
    prefix="",
    tags=["Namespaces"]
)

@router.get("/namespaces")
async def list_namespaces(svc=Depends(namespace_service)):
    return ok(svc.list_namespaces())

@router.post("/namespaces/{namespace}/_reset")
async def reset_namespace(namespace: str, svc=Depends(namespace_service)):
    target = None if namespace == "None" else namespace
    svc.reset_namespace(target)
    return ok({"namespace": target})

@router.delete("/namespaces/{namespace}")
async def delete_namespace(namespace: str, svc=Depends(namespace_service)):
    target = None if namespace == "None" else namespace
    svc.delete_namespace(target)
    return ok({"namespace": target})
        