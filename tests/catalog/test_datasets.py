import pytest
from waluigi.sdk.catalog import CatalogError
from waluigi.catalog.api.schemas import DatasetStatus, SourceCreateRequest, SourceType

NS         = "test"
DATASET_ID = "unit/dataset_test"
SOURCE_ID  = "test_datasets_local"

@pytest.fixture(scope="module", autouse=True)
def ensure_source(catalog):
    try:
        catalog.create_source(SourceCreateRequest(
            id=SOURCE_ID, type=SourceType.LOCAL,
            config={}, description="source for dataset tests"))
    except Exception:
        pass
    yield
    try: catalog.delete_source(SOURCE_ID)
    except Exception: pass


@pytest.fixture(autouse=True)
def cleanup(catalog):
    def _delete():
        try: catalog._delete(catalog._ns_url(f"/datasets/{DATASET_ID}"))
        except Exception: pass
    _delete()
    yield
    _delete()


# ── Basic CRUD ────────────────────────────────────────────────────────────────

def test_list_datasets_returns_list(catalog):
    result = catalog.list_datasets()
    assert isinstance(result, list)


def test_create_and_get_dataset(catalog):
    handle = catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID,
                                    description="Dataset di test unitario")
    assert handle is not None

    dataset = catalog.get_dataset(DATASET_ID)
    assert dataset["id"]          == DATASET_ID
    assert dataset["format"]      == "parquet"
    assert dataset["status"]      == DatasetStatus.DRAFT
    assert dataset["description"] == "Dataset di test unitario"
    assert "createdate"           in dataset
    assert "updatedate"           in dataset


def test_create_dataset_with_source(catalog):
    handle = catalog.create_dataset(DATASET_ID, format="csv", source_id=SOURCE_ID)
    dataset = catalog.get_dataset(DATASET_ID)
    assert dataset["source_id"] == SOURCE_ID


def test_create_dataset_idempotent(catalog):
    catalog.create_dataset(DATASET_ID, format="csv", source_id=SOURCE_ID, description="first")
    catalog.create_dataset(DATASET_ID, format="csv", source_id=SOURCE_ID, description="second")
    dataset = catalog.get_dataset(DATASET_ID)
    assert dataset["id"] == DATASET_ID


def test_get_nonexistent_dataset(catalog):
    with pytest.raises(CatalogError):
        catalog.get_dataset("does/not/exist")


def test_update_description(catalog):
    catalog.create_dataset(DATASET_ID, format="csv", source_id=SOURCE_ID, description="original")
    catalog._patch(catalog._ns_url(f"/datasets/{DATASET_ID}"), json={"description": "updated"})
    assert catalog.get_dataset(DATASET_ID)["description"] == "updated"


def test_update_status(catalog):
    catalog.create_dataset(DATASET_ID, format="csv", source_id=SOURCE_ID)
    catalog._patch(catalog._ns_url(f"/datasets/{DATASET_ID}"), json={"status": "in_review"})
    assert catalog.get_dataset(DATASET_ID)["status"] == DatasetStatus.IN_REVIEW


def test_delete_dataset(catalog):
    catalog.create_dataset(DATASET_ID, format="csv", source_id=SOURCE_ID, description="Delete me")
    catalog._delete(catalog._ns_url(f"/datasets/{DATASET_ID}"))
    with pytest.raises(CatalogError):
        catalog.get_dataset(DATASET_ID)


def test_delete_nonexistent_dataset(catalog):
    with pytest.raises(CatalogError):
        catalog._delete(catalog._ns_url("/datasets/does/not/exist"))


# ── Filter ────────────────────────────────────────────────────────────────────

def test_filter_by_status(catalog):
    catalog.create_dataset(DATASET_ID, format="json", source_id=SOURCE_ID, description="filter test")
    results = catalog.list_datasets(status=DatasetStatus.DRAFT)
    assert any(d["id"] == DATASET_ID for d in results)


def test_filter_by_description(catalog):
    catalog.create_dataset(DATASET_ID, format="json", source_id=SOURCE_ID, description="unique-filter-marker")
    results = catalog.list_datasets(description="unique-filter-marker")
    assert any(d["id"] == DATASET_ID for d in results)


def test_filter_excludes_other_status(catalog):
    catalog.create_dataset(DATASET_ID, format="json", source_id=SOURCE_ID)
    results = catalog.list_datasets(status=DatasetStatus.APPROVED)
    assert not any(d["id"] == DATASET_ID for d in results)


# ── Status lifecycle ──────────────────────────────────────────────────────────

def test_status_lifecycle_draft_to_in_review(catalog):
    catalog.create_dataset(DATASET_ID, format="csv", source_id=SOURCE_ID)
    catalog._patch(catalog._ns_url(f"/datasets/{DATASET_ID}"), json={"status": "in_review"})
    assert catalog.get_dataset(DATASET_ID)["status"] == DatasetStatus.IN_REVIEW


def test_status_lifecycle_in_review_to_approved(catalog):
    catalog.create_dataset(DATASET_ID, format="csv", source_id=SOURCE_ID)
    catalog._patch(catalog._ns_url(f"/datasets/{DATASET_ID}"), json={"status": "in_review"})
    catalog._patch(catalog._ns_url(f"/datasets/{DATASET_ID}"), json={"status": "approved"})
    assert catalog.get_dataset(DATASET_ID)["status"] == DatasetStatus.APPROVED


def test_status_lifecycle_to_deprecated(catalog):
    catalog.create_dataset(DATASET_ID, format="csv", source_id=SOURCE_ID)
    catalog._patch(catalog._ns_url(f"/datasets/{DATASET_ID}"), json={"status": "deprecated"})
    assert catalog.get_dataset(DATASET_ID)["status"] == DatasetStatus.DEPRECATED


# ── Approve endpoint ──────────────────────────────────────────────────────────

def test_approve_changes_status(catalog):
    catalog.create_dataset(DATASET_ID, format="parquet", source_id=SOURCE_ID)
    result = catalog._post(catalog._ns_url(f"/datasets/{DATASET_ID}/_approve"),
                           json={"approved_by": "tester", "notes": "looks good"})
    assert result["status"] == "approved"
    assert result["approved_by"] == "tester"


def test_approve_after_commit(catalog, sample_rows_for_approval):
    dataset_id, sample_rows = sample_rows_for_approval
    handle = catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    with handle.create_version(metadata={"ref": "approval-test"}, force=True) as ctx:
        ctx.write(sample_rows)

    result = catalog._post(catalog._ns_url(f"/datasets/{dataset_id}/_approve"),
                           json={"approved_by": "tester", "notes": "all good"})
    assert result["status"] == "approved"
    assert "schema_published_at" in result


@pytest.fixture
def sample_rows_for_approval(catalog):
    dataset_id = "test/unit/dataset_approve"
    rows = [{"col": i} for i in range(5)]
    yield dataset_id, rows
    try: catalog._delete(catalog._ns_url(f"/datasets/{dataset_id}"))
    except Exception: pass
