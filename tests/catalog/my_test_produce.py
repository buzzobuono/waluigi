from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.api.schemas import SourceCreateRequest, SourceType

source_id  = "local2"
dataset_id = "sales/raw/sales_schema"

catalog.create_source(SourceCreateRequest(
    id=source_id,
    type=SourceType.LOCAL,
    description="Locale temporaneo",
))

rows = [
    {"date": "2026", "product": "A", "quantity": 11, "revenue": 100.0},
    {"date": "2026", "product": "B", "quantity": 25, "revenue": 250.0},
    {"date": "2026", "product": "C", "quantity":  7, "revenue":  70.0},
    {"date": "2026", "product": "D", "quantity": 43, "revenue": 420.0},
    {"date": "2026", "product": "E", "quantity":  3, "revenue":  30.0},
    {"date": "2026", "product": "F", "quantity":  9, "revenue": 350.0},
]

handle = catalog.create_dataset(
    dataset_id,
    format="csv",
    source_id=source_id,
    description="Sales schema variable",
)

with handle.create_version(metadata={"source": "SAP_EXTRACT", "date_ref": "2026.1"}) as writer:
    writer.write(rows)
    