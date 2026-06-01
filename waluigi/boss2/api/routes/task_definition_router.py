from fastapi import APIRouter, Depends

from waluigi.commons.responses import ok, ko
from waluigi.boss2.config.dependencies import task_definition_service

router = APIRouter(
    prefix="/namespaces/{namespace}/task-definitions",
    tags=["Task Definitions"],
)


@router.get("")
async def list_task_definitions(namespace: str, svc=Depends(task_definition_service)):
    return ok(svc.list(namespace))


@router.get("/{id}")
async def get_task_definition(namespace: str, id: str, svc=Depends(task_definition_service)):
    defn = svc.get(namespace, id)
    if defn is None:
        return ko(f"TaskDefinition '{id}' not found in namespace '{namespace}'", status=404)
    return ok(defn)


@router.post("", status_code=201)
async def upsert_task_definition(namespace: str, body: dict, svc=Depends(task_definition_service)):
    kind     = body.get("kind", "TaskDefinition")
    metadata = body.get("metadata", {})
    spec     = body.get("spec", {})
    id       = metadata.get("name")
    if not id:
        return ko("metadata.name is required", status=400)
    svc.upsert(namespace, id, kind, metadata, spec)
    return ok({"namespace": namespace, "id": id}, status=201)


@router.delete("/{id}")
async def delete_task_definition(namespace: str, id: str, svc=Depends(task_definition_service)):
    if not svc.delete(namespace, id):
        return ko(f"TaskDefinition '{id}' not found", status=404)
    return ok({"namespace": namespace, "id": id})
