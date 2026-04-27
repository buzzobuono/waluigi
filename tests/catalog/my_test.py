from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.models import *

# Source SQLite
source = SourceCreateRequest(
    id="local",
    type=SourceType.LOCAL,
    description="Catalog local"
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
    id="sales/raw/sales",
    format=DatasetFormat.CSV,
    description="Sales raw",
    source_id="local"
)


metadata = { "source": "SAP_EXTRACT", "date_ref": "2026" }

with catalog.produce(dataset, metadata) as writer:
    writer.write(rows)

if writer.skipped:
    print(f"⏭️  Già prodotto — versione: {writer.version}")
else:
    print(f"✅ {writer.dataset_id}@{writer.version} scritto.")