import pytest
import shutil
import tempfile
from pathlib import Path
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import *

@pytest.fixture
def temp_db_dir():
    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "test_catalog.db"
    yield str(db_path)
    shutil.rmtree(temp_dir)

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

def test_produce_dataset_from_local_source(sample_rows):
    source_id = "local"
    dataset_id = "sales/raw/sales_raw"
    
    source = SourceCreateRequest(
        id=source_id,
        type=SourceType.LOCAL,
        config={},
        description="Local Source"
    )
    catalog.create_source(source)
    
    dataset = DatasetCreateRequest(
        id=dataset_id,
        format=DatasetFormat.PARQUET,
        description="Sales raw",
        source_id=source_id
    )
    
    metadata = {"rows": len(sample_rows), "source": "SAP_EXTRACT", "date_ref": "2026yyyyyyyy"}
    with catalog.produce(dataset, metadata) as ctx:
        count = ctx.write(sample_rows)
        
    assert count == len(sample_rows)
    info = catalog.get_dataset(dataset_id)
    assert info["id"] == dataset_id

def test_produce_dataset_from_sql_source_sqlite(temp_db_dir, sample_rows):
    source_id = "sqlite_local"
    dataset_id = "sales/raw/sales_raw_sqlite"
    
    source = SourceCreateRequest(
        id=source_id,
        type=SourceType.SQL,
        config={
            "url": f"sqlite:///{temp_db_dir}"
        },
        description="SQLite locale temporaneo"
    )
    catalog.create_source(source)
    
    dataset = DatasetCreateRequest(
        id=dataset_id,
        format=DatasetFormat.SQL,
        description="Sales raw Sqlite",
        source_id=source_id
    )

    metadata = {"rows": len(sample_rows), "source": "SAP_EXTRACT", "date_ref": "2026"}
    with catalog.produce(dataset, metadata) as ctx:
        count = ctx.write(sample_rows)
        
    assert count == len(sample_rows)
    assert not ctx.skipped

def _test_produce_dataset_from_postgresql(sample_rows):
    source_id = "pg_local"
    dataset_id = "sales/raw/sales_pg"
    
    source = SourceCreateRequest(
        id=source_id,
        type=SourceType.SQL,
        config={
            "url": "postgresql://test:test@localhost:5432/test"
        },
        description="PostgreSQL local"
    )
    catalog.create_source(source)
    
    dataset = DatasetCreateRequest(
        id=dataset_id,
        format=DatasetFormat.SQL,
        description="Sales raw PostgreSQL",
        source_id=source_id
    )

    metadata = {"rows": len(sample_rows), "source": "SAP_EXTRACT", "date_ref": "2026"}
    with catalog.produce(dataset, metadata) as writer:
        writer.write(sample_rows)
        
    if writer.skipped:
        assert writer.version is not None
    else:
        assert writer.dataset_id == dataset_id
        assert writer.version is not None
