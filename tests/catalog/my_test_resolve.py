from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.models import *

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

dataset = DatasetCreateRequest(
    id=dataset_id,
    format=DatasetFormat.SQL,
    description="Sales raw",
    source_id=source_id
)

rows = [
    {"date": "2026", "product": "A", "quantity": 11, "revenue": 100.0},
    {"date": "2026", "product": "B", "quantity": 25, "revenue": 250.0},
]
metadata = {"source": "SAP_EXTRACT", "date_ref": "2026"}

with catalog.produce(dataset, metadata) as writer:
    writer.write(rows)

# Risolve la versione più recente e legge i dati
reader = catalog.resolve(dataset_id)
print(f"dataset_id : {reader.dataset_id}")
print(f"version    : {reader.version}")
print(f"location   : {reader.location}")
print(f"format     : {reader.format}")
data = reader.read()
print(data)
