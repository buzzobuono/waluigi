import pytest
import shutil
import tempfile

from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.api.schemas import SourceCreateRequest, SourceType

# ── Fixtures ──────────────────────────────────────────────────────────────────

SOURCE_ID  = "versions_local"
DATASET_ID = "test/versions/main"

ROWS = [
    {"id": 1, "name": "Alice", "score": 9.5},
    {"id": 2, "name": "Bob",   "score": 7.2},
    {"id": 3, "name": "Carol", "score": 8.8},
]


@pytest.fixture(scope="module", autouse=True)
def ensure_source():
    try:
        catalog.create_source(SourceCreateRequest(
            id=SOURCE_ID, type=SourceType.LOCAL,
            config={}, description="source for version tests"))
    except Exception:
        pass
    yield
    try: catalog.delete_source(SOURCE_ID)
    except Exception: pass


@pytest.fixture(autouse=True)
def cleanup():
    def _clean():
        try: catalog._delete(f"/datasets/{DATASET_ID}")
        except Exception: pass
    _clean()
    yield
    _clean()


@pytest.fixture
def committed_version():
    """Write one version and return (handle, version_str)."""
    handle = catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID)
    with handle.create_version(metadata={"ref": "v1"}, force=True) as ctx:
        ctx.write(ROWS)
    versions = catalog.list_versions(DATASET_ID)
    return handle, versions[0]["version"]


# ── List versions ─────────────────────────────────────────────────────────────

def test_list_versions_empty_after_create():
    catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID)
    versions = catalog.list_versions(DATASET_ID)
    assert versions == []


def test_list_versions_nonexistent_dataset():
    with pytest.raises(CatalogError):
        catalog.list_versions("does/not/exist")


def test_list_versions_after_commit():
    handle = catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID)
    with handle.create_version(metadata={"ref": "v1"}, force=True) as ctx:
        ctx.write(ROWS)
    versions = catalog.list_versions(DATASET_ID)
    assert len(versions) == 1
    v = versions[0]
    assert v["dataset_id"] == DATASET_ID
    assert v["status"]     == "committed"
    assert v["version"]    is not None
    assert v["location"]   is not None


def test_list_versions_newest_first():
    handle = catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID)
    for i in range(3):
        with handle.create_version(metadata={"seq": str(i)}, force=True) as ctx:
            ctx.write(ROWS)
    versions = catalog.list_versions(DATASET_ID)
    assert len(versions) == 3
    dates = [v["version"] for v in versions]
    assert dates == sorted(dates, reverse=True)


# ── Reserve / Commit (2-phase write) ──────────────────────────────────────────

def test_reserve_returns_location_and_version():
    catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID)
    result = catalog._post(f"/datasets/{DATASET_ID}/_reserve",
                           json={"metadata": {}, "force": True})
    assert result["dataset_id"] == DATASET_ID
    assert result["version"]    is not None
    assert result["location"]   is not None
    assert result["skipped"]    is False


def test_reserve_nonexistent_dataset():
    with pytest.raises(CatalogError):
        catalog._post("/datasets/does/not/exist/_reserve",
                      json={"metadata": {}, "force": True})


def test_reserve_dedup_same_metadata_returns_skipped():
    handle = catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID)
    meta = {"run": "daily", "date": "2026-01-01"}
    with handle.create_version(metadata=meta) as ctx:
        ctx.write(ROWS)

    result = catalog._post(f"/datasets/{DATASET_ID}/_reserve",
                           json={"metadata": meta, "force": False})
    assert result["skipped"] is True
    assert result["version"] is not None


def test_reserve_force_bypasses_dedup():
    handle = catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID)
    meta = {"run": "daily", "date": "2026-01-01"}
    with handle.create_version(metadata=meta) as ctx:
        ctx.write(ROWS)
    first_version = catalog.list_versions(DATASET_ID)[0]["version"]

    result = catalog._post(f"/datasets/{DATASET_ID}/_reserve",
                           json={"metadata": meta, "force": True})
    assert result["skipped"] is False
    assert result["version"] != first_version


def test_commit_wrong_status_returns_409(committed_version):
    _, version = committed_version
    with pytest.raises(CatalogError) as exc_info:
        catalog._post(f"/datasets/{DATASET_ID}/_commit/{version}",
                      json={"metadata": {}})
    assert "409" in str(exc_info.value)


def test_commit_nonexistent_version():
    catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID)
    with pytest.raises(CatalogError):
        catalog._post(f"/datasets/{DATASET_ID}/_commit/9999-fake-version",
                      json={"metadata": {}})


# ── Fail ──────────────────────────────────────────────────────────────────────

def test_fail_reserved_version_removes_it():
    catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID)
    reserve = catalog._post(f"/datasets/{DATASET_ID}/_reserve",
                            json={"metadata": {}, "force": True})
    version = reserve["version"]

    result = catalog._post(f"/datasets/{DATASET_ID}/_fail/{version}", json={})
    assert result["status"] == "failed"

    versions = catalog.list_versions(DATASET_ID)
    assert not any(v["version"] == version for v in versions)


def test_fail_nonexistent_version():
    catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID)
    with pytest.raises(CatalogError):
        catalog._post(f"/datasets/{DATASET_ID}/_fail/9999-fake-version", json={})


# ── Deprecate ─────────────────────────────────────────────────────────────────

def test_deprecate_version(committed_version):
    _, version = committed_version
    result = catalog._delete(f"/datasets/{DATASET_ID}/_deprecate/{version}")
    assert result["status"] == "deprecated"


def test_deprecated_version_not_in_list(committed_version):
    handle, version = committed_version
    catalog._delete(f"/datasets/{DATASET_ID}/_deprecate/{version}")
    versions = catalog.list_versions(DATASET_ID)
    assert not any(v["version"] == version for v in versions)


def test_deprecate_then_force_create_new(committed_version):
    handle, version = committed_version
    catalog._delete(f"/datasets/{DATASET_ID}/_deprecate/{version}")

    with handle.create_version(metadata={"ref": "v2"}, force=True) as ctx:
        ctx.write(ROWS)

    versions = catalog.list_versions(DATASET_ID)
    assert len(versions) == 1
    assert versions[0]["version"] != version


def test_deprecate_nonexistent_version():
    catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID)
    with pytest.raises(CatalogError):
        catalog._delete(f"/datasets/{DATASET_ID}/_deprecate/9999-fake-version")


# ── Preview ───────────────────────────────────────────────────────────────────

def test_preview_returns_rows_and_columns(committed_version):
    _, version = committed_version
    result = catalog._get(f"/datasets/{DATASET_ID}/_preview/{version}")
    assert result["dataset_id"] == DATASET_ID
    assert result["version"]    == version
    assert set(result["columns"]) >= {"id", "name", "score"}
    assert len(result["rows"])  == len(ROWS)


def test_preview_limit(committed_version):
    _, version = committed_version
    result = catalog._get(f"/datasets/{DATASET_ID}/_preview/{version}",
                          params={"limit": 2})
    assert result["pagination"]["limit"] == 2
    assert len(result["rows"]) == 2


def test_preview_offset(committed_version):
    _, version = committed_version
    result_all    = catalog._get(f"/datasets/{DATASET_ID}/_preview/{version}")
    result_offset = catalog._get(f"/datasets/{DATASET_ID}/_preview/{version}",
                                 params={"offset": 1})
    assert result_offset["rows"][0] == result_all["rows"][1]


def test_preview_nonexistent_version():
    catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID)
    with pytest.raises(CatalogError):
        catalog._get(f"/datasets/{DATASET_ID}/_preview/9999-fake-version")


def test_preview_nonexistent_dataset():
    with pytest.raises(CatalogError):
        catalog._get("/datasets/does/not/exist/_preview/2026-01-01T00:00:00+00:00")


# ── Virtual dataset ───────────────────────────────────────────────────────────

SOURCE_SQL_VIRTUAL = "versions_sql_virtual"
DATASET_VIRTUAL    = "test/versions/virtual"


@pytest.fixture(scope="module", autouse=True)
def ensure_sql_source():
    tmp = tempfile.mkdtemp()
    try:
        catalog.create_source(SourceCreateRequest(
            id=SOURCE_SQL_VIRTUAL, type=SourceType.SQL,
            config={"url": f"sqlite:///{tmp}/v.db"}, description="virtual sql"))
    except Exception:
        pass
    yield
    try: catalog.delete_source(SOURCE_SQL_VIRTUAL)
    except Exception: pass
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(autouse=True)
def cleanup_virtual():
    try: catalog._delete(f"/datasets/{DATASET_VIRTUAL}")
    except Exception: pass
    yield
    try: catalog._delete(f"/datasets/{DATASET_VIRTUAL}")
    except Exception: pass


def test_register_virtual_returns_version():
    result = catalog._post(f"/datasets/{DATASET_VIRTUAL}/_register-virtual", json={
        "source_id": SOURCE_SQL_VIRTUAL,
        "location":  "SELECT * FROM orders",
        "format":    "sql",
    })
    assert result["dataset_id"]  == DATASET_VIRTUAL
    assert result["source_id"]   == SOURCE_SQL_VIRTUAL
    assert result["location"]    == "SELECT * FROM orders"
    assert result["version"]     is not None


def test_register_virtual_appears_in_versions():
    catalog._post(f"/datasets/{DATASET_VIRTUAL}/_register-virtual", json={
        "source_id": SOURCE_SQL_VIRTUAL,
        "location":  "SELECT 1",
        "format":    "sql",
    })
    versions = catalog.list_versions(DATASET_VIRTUAL)
    assert len(versions) >= 1


def test_register_virtual_nonexistent_source():
    with pytest.raises(CatalogError):
        catalog._post(f"/datasets/{DATASET_VIRTUAL}/_register-virtual", json={
            "source_id": "no_such_source",
            "location":  "SELECT 1",
            "format":    "sql",
        })
