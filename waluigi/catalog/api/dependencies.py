import logging
from fastapi import Depends

from waluigi.catalog.config import args
from waluigi.catalog.db import CatalogDB
from waluigi.catalog.services.browser_service import CatalogBrowserService
from waluigi.catalog.services.source_service import SourceService
from waluigi.catalog.services.metadata_service import MetadataService
from waluigi.sdk.dataquality import DQManager
from waluigi.catalog.services.dq_service import DQService
from waluigi.catalog.services.schema_service import SchemaService
from waluigi.catalog.services.chart_service import ChartService
from waluigi.catalog.services.lineage_service import LineageService
from waluigi.catalog.services.dataset_service import DatasetService
from waluigi.catalog.services.version_service import VersionService
from waluigi.catalog.services.materialize_service import MaterializeService

logger = logging.getLogger("waluigi")

def get_db():
    try:
        db = CatalogDB(args.db_url)
        logger.info(f"Database ready: {args.db_url}")
    except Exception as e:
        print("QUI")
        logger.error(f"Critical DB error: {e}")
    return db

def catalog_browser_service(db=Depends(get_db)) -> CatalogBrowserService:
    return CatalogBrowserService(db)

def source_service(db=Depends(get_db)) -> SourceService:
    return SourceService(db)

def metadata_service(db=Depends(get_db)) -> MetadataService:
    return MetadataService(db)

def dq_manager() -> DQManager:
    return DQManager(args.rules_path)

def dq_service(db=Depends(get_db), dq_manager=Depends(dq_manager)) -> DQService:
    return DQService(db, dq_manager)

def version_service(db=Depends(get_db), dq_service=Depends(dq_service)) -> VersionService:
    return VersionService(db, args.data_path, dq_service)

def schema_service(db=Depends(get_db)) -> SchemaService:
    return SchemaService(db)

def chart_service(db=Depends(get_db)) -> ChartService:
    return ChartService(db)

def lineage_service(db=Depends(get_db)) -> LineageService:
    return LineageService(db)

def dataset_service(db=Depends(get_db)) -> DatasetService:
    return DatasetService(db)

def materialize_service(db=Depends(get_db)) -> MaterializeService:
    return MaterializeService(db, args.data_path)