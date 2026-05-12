import pytest
import warnings
from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.api.schemas import DatasetStatus

@pytest.fixture(scope="module")
def dataset_id():
    return "test/unit/dataset_test"

@pytest.fixture(autouse=True)
def cleanup(dataset_id):
    def _delete():
        try:
            catalog._delete(f"/datasets/{dataset_id}")
        except Exception:
            pass
    _delete()
    yield
    _delete()

def test_find_datasets_initial():
    datasets = catalog.list_datasets(status=DatasetStatus.DRAFT)
    assert isinstance(datasets, list)

def test_create_and_get_dataset(dataset_id):
    handle = catalog.create_dataset(dataset_id, format="parquet", description="Dataset di test unitario")
    assert handle is not None

    dataset = catalog.get_dataset(dataset_id)
    assert dataset["id"] == dataset_id
    assert dataset["format"] == "parquet"
    assert dataset["status"] == DatasetStatus.DRAFT

def test_update_dataset(dataset_id):
    catalog.create_dataset(dataset_id, format="csv", description="Initial description")
    catalog._patch(f"/datasets/{dataset_id}", json={"description": "Updated description", "status": "approved"})

    updated = catalog.get_dataset(dataset_id)
    assert updated["description"] == "Updated description"
    assert updated["status"] == DatasetStatus.APPROVED

def test_delete_dataset(dataset_id):
    catalog.create_dataset(dataset_id, format="csv", description="Delete me")
    catalog._delete(f"/datasets/{dataset_id}")

    with pytest.raises(CatalogError):
        catalog.get_dataset(dataset_id)

def test_find_datasets_with_filters(dataset_id):
    catalog.create_dataset(dataset_id, format="json", description="Filter me")
    results = catalog.list_datasets(status=DatasetStatus.DRAFT, description="Filter me")
    assert any(d["id"] == dataset_id for d in results)
