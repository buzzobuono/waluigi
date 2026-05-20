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

_db: CatalogDB | None = None


def init_db(url: str):
    global _db
    _db = CatalogDB(url)
    logger.info(f"Database ready: {url}")


def get_db() -> CatalogDB:
    return _db


def catalog_browser_service(db=Depends(get_db)) -> CatalogBrowserService:
    return CatalogBrowserService(db.folders)


def source_service(db=Depends(get_db)) -> SourceService:
    return SourceService(db.sources)


def metadata_service(db=Depends(get_db)) -> MetadataService:
    return MetadataService(db.versions, db.metadata)


def dq_manager() -> DQManager:
    return DQManager(args.rules_path)


def dq_service(db=Depends(get_db),
               mgr=Depends(dq_manager)) -> DQService:
    return DQService(db.datasets, db.dq_results, db.expectations, mgr)


def schema_service(db=Depends(get_db)) -> SchemaService:
    return SchemaService(db.datasets, db.schema)


def chart_service(db=Depends(get_db)) -> ChartService:
    return ChartService(db.datasets, db.versions, db.sources, db.charts)


def lineage_service(db=Depends(get_db)) -> LineageService:
    return LineageService(db.versions, db.lineage)


def dataset_service(db=Depends(get_db)) -> DatasetService:
    return DatasetService(db.datasets, db.sources, db.schema)


def version_service(db=Depends(get_db),
                    dq_svc=Depends(dq_service)) -> VersionService:
    return VersionService(
        versions=db.versions,
        datasets=db.datasets,
        sources=db.sources,
        metadata=db.metadata,
        schema=db.schema,
        lineage=db.lineage,
        expectations=db.expectations,
        data_path=args.data_path,
        dq_service=dq_svc,
    )


def materialize_service(db=Depends(get_db)) -> MaterializeService:
    return MaterializeService(
        datasets=db.datasets,
        versions=db.versions,
        schema=db.schema,
        lineage=db.lineage,
        metadata=db.metadata,
        data_path=args.data_path,
    )
