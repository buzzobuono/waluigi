from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.models import *

# Source SQLite
source_id = "local2"
dataset_id = "sales/raw/sales_schema"
source = SourceCreateRequest(
        id=source_id,
        type=SourceType.LOCAL,
        description="Locale temporaneo"
        )
    
catalog.create_source(source)

rows = [
    {"date": "2026", "product": "A", "quantity": 11, "revenue1": 100.0},
    {"date": "2026", "product": "B", "quantity": 25, "revenue1": 250.0},
    {"date": "2026", "product": "C", "quantity":  7, "revenue1":  70.0},
    {"date": "2026", "product": "D", "quantity": 43, "revenue1": 420.0},
    {"date": "2026", "product": "E", "quantity":  3, "revenue1":  30.0},
    {"date": "2026", "product": "F", "quantity":  9, "revenue1": 350.0},
]
#catalog.delete_dataset("sales/raw/sales")
dataset = DatasetCreateRequest(
    id=dataset_id,
    format=DatasetFormat.CSV,
    description="Salse schema variable",
    source_id=source_id
)


metadata = { "source": "SAP_EXTRACT", "date_ref": "2026.4" }

with catalog.produce(dataset, metadata) as writer:
    writer.write(rows)
    