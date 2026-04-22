import pytest
import warnings
from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.models import (
    DatasetCreateRequest, 
    DatasetUpdateRequest, 
    DatasetStatus, 
    DatasetFormat
)

@pytest.fixture(scope="module")
def dataset_id():
    return "test/unit/dataset_test"

@pytest.fixture(autouse=True)
def cleanup(dataset_id):
    def _delete():
        try:
            catalog.delete_dataset(dataset_id)
        except:
            pass
    _delete()
    yield
    _delete()

def test_find_datasets_initial():
    datasets = catalog.find_datasets(status=DatasetStatus.DRAFT, description="")
    assert isinstance(datasets, list)

def test_create_and_get_dataset(dataset_id):
    new_dataset = DatasetCreateRequest(
        id=dataset_id,
        format=DatasetFormat.PARQUET,
        description="Dataset di test unitario",
        status=DatasetStatus.DRAFT
    )
    
    result = catalog.create_dataset(new_dataset)
    assert result is not None
    
    dataset = catalog.get_dataset(dataset_id)
    assert dataset["id"] == dataset_id
    assert dataset["format"] == DatasetFormat.PARQUET
    assert dataset["status"] == DatasetStatus.DRAFT

def test_update_dataset(dataset_id):
    catalog.create_dataset(DatasetCreateRequest(
        id=dataset_id, 
        description="Initial description", 
        format=DatasetFormat.CSV
    ))
    
    update_data = DatasetUpdateRequest(
        description="Updated description",
        status=DatasetStatus.APPROVED
    )
    
    catalog.update_dataset(dataset_id, update_data)
    
    updated = catalog.get_dataset(dataset_id)
    assert updated["description"] == "Updated description"
    assert updated["status"] == DatasetStatus.APPROVED

def test_delete_dataset(dataset_id):
    catalog.create_dataset(DatasetCreateRequest(
        id=dataset_id, 
        description="Delete me", 
        format=DatasetFormat.CSV
    ))
    
    catalog.delete_dataset(dataset_id)
    
    with pytest.raises(CatalogError):
        catalog.get_dataset(dataset_id)

def test_create_existing_dataset_fails(dataset_id):
    data = DatasetCreateRequest(
        id=dataset_id, 
        description="Twice", 
        format=DatasetFormat.CSV
    )
    catalog.create_dataset(data)
    
    with pytest.raises(CatalogError) as excinfo:
        catalog.create_dataset(data)
    assert "already exists" in str(excinfo.value).lower()

def test_find_datasets_with_filters(dataset_id):
    catalog.create_dataset(DatasetCreateRequest(
        id=dataset_id, 
        format=DatasetFormat.JSON, 
        description="Filter me", 
        status=DatasetStatus.DRAFT
    ))
    
    results = catalog.find_datasets(status=DatasetStatus.DRAFT, description="Filter me")
    
    assert any(d["id"] == dataset_id for d in results)
