from fastapi import APIRouter, Depends

from waluigi.commons.responses import ok
from waluigi.boss2.config.dependencies import namespace_service

router = APIRouter(
    prefix="/namespaces",
    tags=["Namespaces"]
)


@router.get("")
async def list_namespaces(svc=Depends(namespace_service)):
    return ok(svc.list_namespaces())


@router.post("/{namespace}/_reset")
async def reset_namespace(namespace: str, svc=Depends(namespace_service)):
    svc.reset_namespace(namespace)
    return ok({"namespace": namespace})


@router.delete("/{namespace}")
async def delete_namespace(namespace: str, svc=Depends(namespace_service)):
    svc.delete_namespace(namespace)
    return ok({"namespace": namespace})
