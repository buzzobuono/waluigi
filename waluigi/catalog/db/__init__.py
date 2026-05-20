from waluigi.catalog.db.base import _set_engine
from waluigi.catalog.db.engine import create_catalog_engine


class CatalogDB:
    """Registry: initialises one shared engine and exposes typed repositories."""

    def __init__(self, url: str):
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

        engine = create_catalog_engine(url)
        _set_engine(engine)

        self.sources      = SourceRepository(engine)
        self.datasets     = DatasetRepository(engine)
        self.versions     = VersionRepository(engine)
        self.schema       = SchemaRepository(engine)
        self.expectations = ExpectationRepository(engine)
        self.charts       = ChartRepository(engine)
        self.dq_results   = DQResultRepository(engine)
        self.lineage      = LineageRepository(engine)
        self.metadata     = MetadataRepository(engine)
        self.folders      = FolderRepository(engine)
