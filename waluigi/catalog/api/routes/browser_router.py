from fastapi import APIRouter, Depends
from waluigi.core.responses import ok, ko
from waluigi.catalog.services.browser_service import CatalogBrowserService
from waluigi.catalog.api.dependencies import catalog_browser_service

browser_router = APIRouter(
    prefix="/folders",
    tags=["Folders"]
)

@browser_router.get("/{prefix:path}/",
         summary="List datasets and virtual sub-prefixes under a prefix",
         description=(
             "Trailing slash distinguishes browse from dataset access. "
             "Returns direct child datasets and deeper virtual prefixes, "
             "exactly like S3 ListObjects with a delimiter."
         ))
async def list_folders(prefix: str, catalog_browser_service: CatalogBrowserService = Depends(catalog_browser_service)):
    return ok(catalog_browser_service.list_folders(prefix))
