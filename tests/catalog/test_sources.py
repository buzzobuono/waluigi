import pytest
from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.models import SourceCreateRequest, SourceUpdateRequest, SourceType

@pytest.fixture(scope="module")
def source_id():
    return "pg-dwh-test"

@pytest.fixture(autouse=True)
def cleanup(source_id):
    def _delete():
        try:
            catalog.delete_source(source_id)
        except:
            pass
    _delete()
    yield
    _delete()

def test_list_sources_initial():
    sources = catalog.list_sources()
    assert isinstance(sources, list)

def test_create_and_get_source(source_id):
    new_source = SourceCreateRequest(
        id=source_id,
        type=SourceType.SQL,
        config={"host": "10.0.0.1", "port": 5432},
        description="DWH Database"
    )
    
    result = catalog.create_source(new_source)
    assert result is not None # O controlla i campi specifici
    
    source = catalog.get_source(source_id)
    assert source["id"] == source_id
    assert source["config"]["host"] == "10.0.0.1"

def test_update_source(source_id):
    catalog.create_source(SourceCreateRequest(id=source_id, type=SourceType.SQL, description="Test description", config={}))
    
    update_data = SourceUpdateRequest(
        description="Updated Description",
        config={"host": "10.0.0.2"}
    )
    
    catalog.update_source(source_id, update_data)
    
    updated = catalog.get_source(source_id)
    assert updated["description"] == "Updated Description"
    assert updated["config"]["host"] == "10.0.0.2"

def test_delete_source(source_id):
    catalog.create_source(SourceCreateRequest(id=source_id, type=SourceType.SQL, description="Test description", config={}))
    
    catalog.delete_source(source_id)
    
    with pytest.raises(CatalogError):
        catalog.get_source(source_id)

def test_create_existing_source_fails(source_id):
    data = SourceCreateRequest(id=source_id, type=SourceType.SQL, description="Test description", config={})
    catalog.create_source(data)
    
    with pytest.raises(CatalogError) as excinfo:
        catalog.create_source(data)
    assert "already exists" in str(excinfo.value)
