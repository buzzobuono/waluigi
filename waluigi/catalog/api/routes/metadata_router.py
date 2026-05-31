from fastapi import APIRouter, Depends
from waluigi.commons.responses import ok, ko
from waluigi.catalog.api.schemas import MetadataSetRequest
from waluigi.catalog.services.metadata_service import MetadataService
from waluigi.catalog.config.dependencies import metadata_service

metadata_router = APIRouter(
    prefix="/namespaces/{namespace}/datasets",
    tags=["Metadata"],
)


@metadata_router.get("/{dataset_id:path}/versions/{version}/metadata",
    summary="Get all metadata for a version")
async def get_metadata(namespace: str, dataset_id: str, version: str,
                       svc: MetadataService = Depends(metadata_service)):
    try:
        return ok(svc.get_version_metadata(namespace, dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)


@metadata_router.post("/{dataset_id:path}/versions/{version}/metadata",
    summary="Set a metadata key on a version")
async def set_metadata(namespace: str, dataset_id: str, version: str,
                       body: MetadataSetRequest,
                       svc: MetadataService = Depends(metadata_service)):
    try:
        return ok(svc.set_version_metadata(namespace, dataset_id, version,
                                           body.key, body.value))
    except ValueError as e:
        status = 422 if "reserved" in str(e) else 404
        return ko(str(e), status)


@metadata_router.delete("/{dataset_id:path}/versions/{version}/metadata/{key}",
    summary="Delete a metadata key from a version")
async def delete_metadata(namespace: str, dataset_id: str, version: str, key: str,
                          svc: MetadataService = Depends(metadata_service)):
    try:
        return ok(svc.delete_version_metadata(namespace, dataset_id, version, key))
    except ValueError as e:
        return ko(str(e), 404)
