from fastapi import APIRouter, Depends
from waluigi.commons.responses import ok, ko
from waluigi.catalog.services.lineage_service import LineageService
from waluigi.catalog.config.dependencies import lineage_service

lineage_router = APIRouter(
    prefix="/namespaces/{namespace}/datasets",
    tags=["Lineage"],
)


@lineage_router.get("/{dataset_id:path}/lineage/{version}",
    summary="Get upstream and downstream lineage")
async def get_lineage(namespace: str, dataset_id: str, version: str,
                      svc: LineageService = Depends(lineage_service)):
    try:
        return ok(svc.get_lineage(namespace, dataset_id, version))
    except ValueError as e:
        return ko(str(e), 404)
