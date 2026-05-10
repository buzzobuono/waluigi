from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import SourceCreateRequest, SourceType


date = context.params.date

catalog.create_source(SourceCreateRequest(
    id="local",
    type=SourceType.LOCAL,
    config={},
    description="Local Source",
))

print(f"Creating sales dataset for date: {date}")

rows = [
    {"date": date, "product": "A", "quantity": 10, "revenue": 100.0},
    {"date": date, "product": "B", "quantity": 25, "revenue": 250.0},
    {"date": date, "product": "C", "quantity":  7, "revenue":  70.0},
    {"date": date, "product": "D", "quantity": 42, "revenue": 420.0},
    {"date": date, "product": "E", "quantity":  3, "revenue":  30.0},
    {"date": date, "product": "F", "quantity":  9, "revenue": 350.0},
]

handle = catalog.create_dataset(
    "sales/raw/sales_raw2",
    format="csv",
    source_id="local",
    description="Sales raw data",
)

with handle.create_version(metadata={"date": date, "source": "SAP_EXTRACT"}) as writer:
    writer.write(rows)

if writer.skipped:
    print(f"Skipped — same metadata, existing version: {writer.version}")
else:
    print(f"Done: {writer.dataset_id} @ {writer.version} ({len(rows)} rows)")

