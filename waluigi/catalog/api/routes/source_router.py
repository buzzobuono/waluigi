from fastapi import APIRouter, Depends
from waluigi.core.responses import ok, ko
from waluigi.core.utils import _model_dump
from waluigi.catalog.api.schemas import SourceCreateRequest, SourceUpdateRequest
from waluigi.catalog.services.source_service import SourceService
from waluigi.catalog.api.dependencies import source_service

source_router = APIRouter(
    prefix="/sources",
    tags=["Sources"]
)


@source_router.get("", summary="List sources")
async def list_sources(source_service: SourceService = Depends(source_service)):
    return ok(source_service.list())


@source_router.post("", summary="Register or update a source (upsert)", status_code=200)
async def create_source(body: SourceCreateRequest, source_service: SourceService = Depends(source_service)):
    try:
        return ok(source_service.upsert(body.id, body.type.value, body.config, body.description))
    except ValueError as e:
        return ko(str(e), 409)


@source_router.get("/{source_id}", summary="Get a source details")
async def get_source(source_id: str, source_service: SourceService = Depends(source_service)):
    source = source_service.get(source_id)
    if not source:
        return ko("Source not found", 404)
    return ok(source)


@source_router.patch("/{source_id}", summary="Update a source")
async def update_source(source_id: str, body: SourceUpdateRequest, source_service: SourceService = Depends(source_service)):
    source = source_service.update(source_id, **_model_dump(body))
    if not source:
        return ko("Source not found", 404)
    return ok(source)


@source_router.delete("/{source_id}", summary="Delete a source")
async def delete_source(source_id: str, source_service: SourceService = Depends(source_service)):
    try:
        deleted = source_service.delete(source_id)
    except ValueError as e:
        return ko(str(e), 409)
    if not deleted:
        return ko("Source not found", 404)
    return ok({"id": source_id})