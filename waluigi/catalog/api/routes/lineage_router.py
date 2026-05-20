from fastapi import APIRouter, Depends
from waluigi.core.responses import ok, ko
from waluigi.catalog.api.schemas import MetadataSetRequest
from waluigi.catalog.services.lineage_service import LineageService
from waluigi.catalog.config.dependencies import lineage_service

lineage_router = APIRouter(
    prefix="/datasets",
    tags=["Lineage"]
)


@lineage_router.get("/{dataset_id:path}/lineage/{version}", tags=["Lineage"],
         summary="Get upstream and downstream lineage")
async def get_lineage(dataset_id: str, version: str, lineage_service: LineageService = Depends(lineage_service)):
    try:
        return ok(lineage_service.get_lineage(dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)

