from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.models import *

# Source SQLite
source_id = "sqlite_local"
dataset_id = "sales/raw/sales_raw_sqlite"
source = SourceCreateRequest(
        id=source_id,
        type=SourceType.SQL,
        config={
            "url": f"sqlite:///test.db"
        },
        description="SQLite locale temporaneo"
        )
    
catalog.create_source(source)

rows = [
    {"date": "2026", "product": "A", "quantity": 11, "revenue": 100.0},
    {"date": "2026", "product": "B", "quantity": 25, "revenue": 250.0},
    {"date": "2026", "product": "C", "quantity":  7, "revenue":  70.0},
    {"date": "2026", "product": "D", "quantity": 43, "revenue": 420.0},
    {"date": "2026", "product": "E", "quantity":  3, "revenue":  30.0},
    {"date": "2026", "product": "F", "quantity":  9, "revenue": 350.0},
]
#catalog.delete_dataset("sales/raw/sales")
dataset = DatasetCreateRequest(
    id=dataset_id,
    format=DatasetFormat.SQL,
    description="Sales raw",
    source_id=source_id
)


metadata = { "source": "SAP_EXTRACT", "date_ref": "2026" }

with catalog.produce(dataset, metadata) as writer:
    writer.write(rows)
    