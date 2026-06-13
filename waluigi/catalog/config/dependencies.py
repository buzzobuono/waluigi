import logging
from fastapi import Depends

from waluigi.catalog.config.args import args
from waluigi.catalog.db import CatalogDB
from waluigi.catalog.repositories.source_repo import SourceRepository
from waluigi.catalog.repositories.dataset_repo import DatasetRepository
from waluigi.catalog.repositories.version_repo import VersionRepository
from waluigi.catalog.repositories.schema_repo import SchemaRepository
from waluigi.catalog.repositories.expectation_repo import ExpectationRepository
from waluigi.catalog.repositories.chart_repo import ChartRepository
from waluigi.catalog.repositories.dq_result_repo import DQResultRepository
from waluigi.catalog.repositories.lineage_repo import LineageRepository
from waluigi.catalog.repositories.metadata_repo import MetadataRepository
from waluigi.catalog.repositories.folder_repo import FolderRepository
from waluigi.catalog.services.browser_service import CatalogBrowserService
from waluigi.catalog.services.source_service import SourceService
from waluigi.catalog.services.metadata_service import MetadataService
from waluigi.catalog.services.dq_service import DQService
from waluigi.catalog.services.schema_service import SchemaService
from waluigi.catalog.services.chart_service import ChartService
from waluigi.catalog.services.lineage_service import LineageService
from waluigi.catalog.services.dataset_service import DatasetService
from waluigi.catalog.services.version_service import VersionService
from waluigi.catalog.services.materialize_service import MaterializeService
from waluigi.sdk.dataquality import DQManager

logger = logging.getLogger("waluigi")

_db: CatalogDB | None = None


def init_db(url: str):
    global _db
    _db = CatalogDB(url)
    logger.info(f"Database ready: {url}")


def get_db() -> CatalogDB:
    return _db


# ---------------------------------------------------------------------------
# Repository dependencies
# ---------------------------------------------------------------------------

def sources_repository(db=Depends(get_db)) -> SourceRepository:
    return db.sources

def datasets_repository(db=Depends(get_db)) -> DatasetRepository:
    return db.datasets

def versions_repository(db=Depends(get_db)) -> VersionRepository:
    return db.versions

def schema_repository(db=Depends(get_db)) -> SchemaRepository:
    return db.schema

def expectations_repository(db=Depends(get_db)) -> ExpectationRepository:
    return db.expectations

def charts_repository(db=Depends(get_db)) -> ChartRepository:
    return db.charts

def dq_results_repository(db=Depends(get_db)) -> DQResultRepository:
    return db.dq_results

def lineage_repository(db=Depends(get_db)) -> LineageRepository:
    return db.lineage

def metadata_repository(db=Depends(get_db)) -> MetadataRepository:
    return db.metadata

def folders_repository(db=Depends(get_db)) -> FolderRepository:
    return db.folders


# ---------------------------------------------------------------------------
# Service dependencies
# ---------------------------------------------------------------------------

def catalog_browser_service(
    folders_repo=Depends(folders_repository),
) -> CatalogBrowserService:
    return CatalogBrowserService(folders_repository=folders_repo)


def source_service(
    sources_repo=Depends(sources_repository),
) -> SourceService:
    return SourceService(source_repository=sources_repo)


def metadata_service(
    versions_repo=Depends(versions_repository),
    metadata_repo=Depends(metadata_repository),
) -> MetadataService:
    return MetadataService(
        versions_repository=versions_repo,
        metadata_repository=metadata_repo,
    )


def dq_manager() -> DQManager:
    return DQManager(args.rules_path)


def dq_service(
    datasets_repo=Depends(datasets_repository),
    dq_results_repo=Depends(dq_results_repository),
    expectations_repo=Depends(expectations_repository),
    mgr=Depends(dq_manager),
) -> DQService:
    return DQService(
        datasets_repository=datasets_repo,
        dq_results_repository=dq_results_repo,
        expectations_repository=expectations_repo,
        dq_manager=mgr,
    )


def schema_service(
    datasets_repo=Depends(datasets_repository),
    schema_repo=Depends(schema_repository),
) -> SchemaService:
    return SchemaService(
        datasets_repository=datasets_repo,
        schema_repository=schema_repo,
    )


def chart_service(
    datasets_repo=Depends(datasets_repository),
    versions_repo=Depends(versions_repository),
    sources_repo=Depends(sources_repository),
    charts_repo=Depends(charts_repository),
) -> ChartService:
    return ChartService(
        datasets_repository=datasets_repo,
        versions_repository=versions_repo,
        sources_repository=sources_repo,
        charts_repository=charts_repo,
    )


def lineage_service(
    versions_repo=Depends(versions_repository),
    lineage_repo=Depends(lineage_repository),
) -> LineageService:
    return LineageService(
        versions_repository=versions_repo,
        lineage_repository=lineage_repo,
    )


def dataset_service(
    datasets_repo=Depends(datasets_repository),
    sources_repo=Depends(sources_repository),
    versions_repo=Depends(versions_repository),
    schema_repo=Depends(schema_repository),
) -> DatasetService:
    return DatasetService(
        datasets_repository=datasets_repo,
        sources_repository=sources_repo,
        versions_repository=versions_repo,
        schema_repository=schema_repo,
    )


def version_service(
    versions_repo=Depends(versions_repository),
    datasets_repo=Depends(datasets_repository),
    sources_repo=Depends(sources_repository),
    metadata_repo=Depends(metadata_repository),
    schema_repo=Depends(schema_repository),
    lineage_repo=Depends(lineage_repository),
    expectations_repo=Depends(expectations_repository),
    dq_svc=Depends(dq_service),
) -> VersionService:
    return VersionService(
        versions_repository=versions_repo,
        datasets_repository=datasets_repo,
        sources_repository=sources_repo,
        metadata_repository=metadata_repo,
        schema_repository=schema_repo,
        lineage_repository=lineage_repo,
        expectations_repository=expectations_repo,
        data_path=args.data_path,
        dq_service=dq_svc,
    )


def materialize_service(
    datasets_repo=Depends(datasets_repository),
    versions_repo=Depends(versions_repository),
    schema_repo=Depends(schema_repository),
    lineage_repo=Depends(lineage_repository),
    metadata_repo=Depends(metadata_repository),
) -> MaterializeService:
    return MaterializeService(
        datasets_repository=datasets_repo,
        versions_repository=versions_repo,
        schema_repository=schema_repo,
        lineage_repository=lineage_repo,
        metadata_repository=metadata_repo,
        data_path=args.data_path,
    )
