from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.api.schemas import *

source_id  = "sqlite_local"
dataset_id = "sales/raw/sales_raw_sqlite"

# Assicura che source e dataset esistano (idempotente)
source = SourceCreateRequest(
    id=source_id,
    type=SourceType.SQL,
    config={"url": "sqlite:///test.db"},
    description="SQLite locale temporaneo"
)
catalog.create_source(source)

rows = [
    {"date": "2026", "product": "A", "quantity": 11, "revenue": 100.0},
    {"date": "2026", "product": "B", "quantity": 25, "revenue": 250.0},
]

handle = catalog.create_dataset(dataset_id, format="sql", source_id=source_id, description="Sales raw")
with handle.create_version(metadata={"source": "SAP_EXTRACT", "date_ref": "2026"}) as writer:
    writer.write(rows)

reader = catalog.read_dataset(dataset_id)
print(f"dataset_id : {reader.dataset_id}")
print(f"version    : {reader.version}")
print(f"location   : {reader.location}")
print(f"format     : {reader.format}")
data = reader.read()
print(data)


source_id  = "local"
dataset_id = "sales/raw/sales_raw_local"

# Assicura che source e dataset esistano (idempotente)
source = SourceCreateRequest(
    id=source_id,
    type=SourceType.LOCAL,
    description="Local source"
)
catalog.create_source(source)

rows = [
    {"date": "2026", "product": "A", "quantity": 11, "revenue": 100.0},
    {"date": "2026", "product": "B", "quantity": 25, "revenue": 250.0},
]

handle = catalog.create_dataset(dataset_id, format="csv", source_id=source_id, description="Sales raw")
with handle.create_version(metadata={"source": "SAP_EXTRACT", "date_ref": "2026"}) as writer:
    writer.write(rows)
