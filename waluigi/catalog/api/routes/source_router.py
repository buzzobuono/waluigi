from fastapi import APIRouter, Depends
from waluigi.commons.responses import ok, ko
from waluigi.commons.utils import _model_dump
from waluigi.catalog.api.schemas import SourceCreateRequest, SourceUpdateRequest
from waluigi.catalog.services.source_service import SourceService
from waluigi.catalog.config.dependencies import source_service

source_router = APIRouter(
    prefix="/namespaces/{namespace}/sources",
    tags=["Sources"]
)


@source_router.get("", summary="List sources")
async def list_sources(namespace: str,
                       svc: SourceService = Depends(source_service)):
    return ok(svc.list(namespace))


@source_router.post("", summary="Register or update a source (upsert)", status_code=200)
async def create_source(namespace: str, body: SourceCreateRequest,
                        svc: SourceService = Depends(source_service)):
    try:
        return ok(svc.upsert(namespace, body.id, body.type.value,
                             body.config, body.description))
    except ValueError as e:
        return ko(str(e), 409)


@source_router.get("/{source_id}", summary="Get a source details")
async def get_source(namespace: str, source_id: str,
                     svc: SourceService = Depends(source_service)):
    source = svc.get(namespace, source_id)
    if not source:
        return ko("Source not found", 404)
    return ok(source)


@source_router.patch("/{source_id}", summary="Update a source")
async def update_source(namespace: str, source_id: str,
                        body: SourceUpdateRequest,
                        svc: SourceService = Depends(source_service)):
    source = svc.update(namespace, source_id, **_model_dump(body))
    if not source:
        return ko("Source not found", 404)
    return ok(source)


@source_router.delete("/{source_id}", summary="Delete a source")
async def delete_source(namespace: str, source_id: str,
                        svc: SourceService = Depends(source_service)):
    try:
        deleted = svc.delete(namespace, source_id)
    except ValueError as e:
        return ko(str(e), 409)
    if not deleted:
        return ko("Source not found", 404)
    return ok({"id": source_id})
