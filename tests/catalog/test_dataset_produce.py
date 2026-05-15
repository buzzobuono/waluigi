import pytest
import shutil
import tempfile
from pathlib import Path

from waluigi.catalog.api.schemas import SourceCreateRequest, SourceType

# ── Shared rows fixture ───────────────────────────────────────────────────────

@pytest.fixture
def sample_rows():
    return [
        {"date": "2026", "product": "A", "quantity": 11, "revenue": 100.0},
        {"date": "2026", "product": "B", "quantity": 25, "revenue": 250.0},
        {"date": "2026", "product": "C", "quantity":  7, "revenue":  70.0},
        {"date": "2026", "product": "D", "quantity": 42, "revenue": 420.0},
        {"date": "2026", "product": "E", "quantity":  3, "revenue":  30.0},
        {"date": "2026", "product": "F", "quantity":  9, "revenue": 350.0},
    ]


# ── Local (parquet) ───────────────────────────────────────────────────────────

SOURCE_LOCAL   = "produce_local"
DATASET_LOCAL  = "test/produce/sales_local"

@pytest.fixture(autouse=True)
def cleanup_local(catalog):
    def _clean():
        try: catalog._delete(f"/datasets/{DATASET_LOCAL}")
        except Exception: pass
        try: catalog.delete_source(SOURCE_LOCAL)
        except Exception: pass
    _clean()
    yield
    _clean()


def test_produce_local_row_count(catalog, sample_rows):
    catalog.create_source(SourceCreateRequest(
        id=SOURCE_LOCAL, type=SourceType.LOCAL, config={}, description="Local"))
    handle = catalog.create_dataset(DATASET_LOCAL, format="parquet", source_id=SOURCE_LOCAL)

    with handle.create_version(metadata={"source": "SAP", "date_ref": "2026"}) as ctx:
        count = ctx.write(sample_rows)

    assert count == len(sample_rows)
    assert not ctx.skipped


def test_produce_local_version_committed(catalog, sample_rows):
    catalog.create_source(SourceCreateRequest(
        id=SOURCE_LOCAL, type=SourceType.LOCAL, config={}, description="Local"))
    handle = catalog.create_dataset(DATASET_LOCAL, format="parquet", source_id=SOURCE_LOCAL)

    with handle.create_version(metadata={"source": "SAP", "date_ref": "2026"}) as ctx:
        ctx.write(sample_rows)

    versions = catalog.list_versions(DATASET_LOCAL)
    assert len(versions) == 1
    ver = versions[0]
    assert ver["status"] == "committed"
    assert ver["version"] is not None
    assert ver["location"] is not None


def test_produce_local_readable_after_commit(catalog, sample_rows):
    catalog.create_source(SourceCreateRequest(
        id=SOURCE_LOCAL, type=SourceType.LOCAL, config={}, description="Local"))
    handle = catalog.create_dataset(DATASET_LOCAL, format="parquet", source_id=SOURCE_LOCAL)

    with handle.create_version(metadata={"source": "SAP", "date_ref": "2026"}) as ctx:
        ctx.write(sample_rows)

    reader = catalog.read_dataset(DATASET_LOCAL)
    df = reader.read()
    assert len(df) == len(sample_rows)
    assert set(df.columns) >= {"date", "product", "quantity", "revenue"}


def test_produce_local_dedup_skips_on_same_metadata(catalog, sample_rows):
    catalog.create_source(SourceCreateRequest(
        id=SOURCE_LOCAL, type=SourceType.LOCAL, config={}, description="Local"))
    handle = catalog.create_dataset(DATASET_LOCAL, format="parquet", source_id=SOURCE_LOCAL)
    meta = {"source": "SAP", "date_ref": "2026"}

    with handle.create_version(metadata=meta) as ctx:
        ctx.write(sample_rows)

    with handle.create_version(metadata=meta, force=False) as ctx2:
        count2 = ctx2.write(sample_rows)

    assert ctx2.skipped
    assert count2 == 0
    assert len(catalog.list_versions(DATASET_LOCAL)) == 1


def test_produce_local_force_creates_new_version(catalog, sample_rows):
    catalog.create_source(SourceCreateRequest(
        id=SOURCE_LOCAL, type=SourceType.LOCAL, config={}, description="Local"))
    handle = catalog.create_dataset(DATASET_LOCAL, format="parquet", source_id=SOURCE_LOCAL)
    meta = {"source": "SAP", "date_ref": "2026"}

    with handle.create_version(metadata=meta) as ctx:
        ctx.write(sample_rows)

    with handle.create_version(metadata=meta, force=True) as ctx2:
        count2 = ctx2.write(sample_rows)

    assert not ctx2.skipped
    assert count2 == len(sample_rows)
    assert len(catalog.list_versions(DATASET_LOCAL)) == 2


def test_produce_local_version_metadata_stored(catalog, sample_rows):
    catalog.create_source(SourceCreateRequest(
        id=SOURCE_LOCAL, type=SourceType.LOCAL, config={}, description="Local"))
    handle = catalog.create_dataset(DATASET_LOCAL, format="parquet", source_id=SOURCE_LOCAL)
    meta = {"source": "SAP", "date_ref": "2026", "rows": str(len(sample_rows))}

    with handle.create_version(metadata=meta) as ctx:
        ctx.write(sample_rows)
        version = ctx.version

    stored = catalog.get_version_metadata(DATASET_LOCAL, version)
    assert stored.get("source") == "SAP"
    assert stored.get("date_ref") == "2026"


# ── SQLite (sql format) ───────────────────────────────────────────────────────

SOURCE_SQL    = "produce_sqlite"
DATASET_SQL   = "test/produce/sales_sqlite"

@pytest.fixture
def sqlite_url():
    tmp = tempfile.mkdtemp()
    yield f"sqlite:///{tmp}/test.db"
    shutil.rmtree(tmp)


@pytest.fixture(autouse=True)
def cleanup_sql(catalog):
    def _clean():
        try: catalog._delete(f"/datasets/{DATASET_SQL}")
        except Exception: pass
        try: catalog.delete_source(SOURCE_SQL)
        except Exception: pass
    _clean()
    yield
    _clean()


def test_produce_sql_row_count(catalog, sqlite_url, sample_rows):
    catalog.create_source(SourceCreateRequest(
        id=SOURCE_SQL, type=SourceType.SQL,
        config={"url": sqlite_url}, description="SQLite"))
    handle = catalog.create_dataset(DATASET_SQL, format="sql", source_id=SOURCE_SQL)

    with handle.create_version(metadata={"source": "SAP", "date_ref": "2026"}) as ctx:
        count = ctx.write(sample_rows)

    assert count == len(sample_rows)
    assert not ctx.skipped


def test_produce_sql_version_committed(catalog, sqlite_url, sample_rows):
    catalog.create_source(SourceCreateRequest(
        id=SOURCE_SQL, type=SourceType.SQL,
        config={"url": sqlite_url}, description="SQLite"))
    handle = catalog.create_dataset(DATASET_SQL, format="sql", source_id=SOURCE_SQL)

    with handle.create_version(metadata={"source": "SAP", "date_ref": "2026"}) as ctx:
        ctx.write(sample_rows)

    versions = catalog.list_versions(DATASET_SQL)
    assert len(versions) == 1
    assert versions[0]["status"] == "committed"
    assert versions[0]["dataset_id"] == DATASET_SQL
