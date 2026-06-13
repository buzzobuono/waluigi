from fastapi import APIRouter, Depends, Request

from waluigi.commons.responses import ok, ko
from waluigi.boss.config.dependencies import secret_service, namespaces_repository

router = APIRouter(
    prefix="/namespaces/{namespace}/secrets",
    tags=["Secrets"],
)


@router.get("")
async def list_secrets(namespace: str, svc=Depends(secret_service)):
    return ok(svc.list_names(namespace))


@router.post("/{name}")
async def upsert_secret(namespace: str, name: str, request: Request,
                        svc=Depends(secret_service),
                        ns_repo=Depends(namespaces_repository)):
    if not ns_repo.exists(namespace):
        return ko(f"Namespace '{namespace}' not found", status=404)
    doc = await request.json()
    if not isinstance(doc, dict):
        return ko("Body must be a JSON object of key: value pairs", status=400)
    svc.upsert(namespace, name, doc)
    return ok({"namespace": namespace, "name": name})


@router.delete("/{name}")
async def delete_secret(namespace: str, name: str, svc=Depends(secret_service)):
    if not svc.delete(namespace, name):
        return ko(f"Secret '{name}' not found in namespace '{namespace}'", status=404)
    return ok({"namespace": namespace, "name": name})
