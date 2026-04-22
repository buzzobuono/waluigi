import csv
from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.models import (
    DatasetCreateRequest,
    DatasetStatus, 
    DatasetFormat
)

rows = [
            {"date": "2026", "product": "A", "quantity": 10, "revenue": 100.0},
            {"date": "2026", "product": "B", "quantity": 25, "revenue": 250.0},
            {"date": "2026", "product": "C", "quantity":  7, "revenue":  70.0},
            {"date": "2026", "product": "D", "quantity": 42, "revenue": 420.0},
            {"date": "2026", "product": "E", "quantity":  3, "revenue":  30.0},
            {"date": "2026", "product": "F", "quantity":  9, "revenue": 350.0},
        ]
dataset = DatasetCreateRequest(
        id="sales/raw/sales_raw",
        format=DatasetFormat.CSV,
        description="Sales raw",
        status=DatasetStatus.DRAFT
    )

with catalog.produce(dataset) as ctx:
    with open(ctx.path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    ctx.meta["rows"]   = len(rows)
    ctx.meta["source"]  = "SAP_EXTRACT"
    ctx.meta["date_ref"] = "2026"
