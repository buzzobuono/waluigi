from fastapi import APIRouter, Depends
from waluigi.core.responses import ok, ko
from waluigi.catalog.api.schemas import MetadataSetRequest
from waluigi.catalog.services.metadata_service import MetadataService
from waluigi.catalog.config.dependencies import metadata_service

metadata_router = APIRouter(
    prefix="/datasets",
    tags=["Metadata"]
)

@metadata_router.get("/{dataset_id:path}/versions/{version}/metadata",
         summary="Get all metadata for a version")
async def get_metadata(dataset_id: str, version: str, metadata_service: MetadataService = Depends(metadata_service)):
    try:
        return ok(metadata_service.get_version_metadata(dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)


@metadata_router.post("/{dataset_id:path}/versions/{version}/metadata",
          summary="Set a metadata key on a version")
async def set_metadata(dataset_id: str, version: str, body: MetadataSetRequest, metadata_service: MetadataService = Depends(metadata_service)):
    try:
        return ok(metadata_service.set_version_metadata(
            dataset_id, version, body.key, body.value))
    except ValueError as e:
        status = 422 if "reserved" in str(e) else 404
        return ko(str(e), status)


@metadata_router.delete("/{dataset_id:path}/versions/{version}/metadata/{key}",
            summary="Delete a metadata key from a version")
async def delete_metadata(dataset_id: str, version: str, key: str, metadata_service: MetadataService = Depends(metadata_service)):
    try:
        return ok(metadata_service.delete_version_metadata(dataset_id, version, key))
    except ValueError as e:
        return ko(str(e), 404)