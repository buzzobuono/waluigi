import random
import pandas as pd
from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog
from waluigi.catalog.api.schemas import SourceCreateRequest, SourceType

METRICS = {
    "erp":    [("revenue",     "finance"),     ("costs",       "finance"),
   ("orders",      "operations"),  ("refunds",     "finance")],
    "web":    [("sessions",    "traffic"),     ("pageviews",   "traffic"),
   ("conversions", "acquisition"), ("bounce_rate", "engagement")],
    "social": [("followers",   "audience"),    ("impressions", "reach"),
   ("engagements", "interaction"), ("shares",      "viral")],
}

source = context.params.source.lower()
date   = context.params.date

catalog.create_source(SourceCreateRequest(
    id="analytics-local",
    type=SourceType.LOCAL,
    config={},
    description="Local storage for analytics pipeline",
))

print(f"Extracting raw data for source: {source}, date: {date}")

rows = [
    {"date": date, "source": source, "metric": metric,
     "value": round(random.uniform(100, 10_000), 2), "category": category}
    for metric, category in METRICS[source]
]
df = pd.DataFrame(rows)

handle = catalog.create_dataset(
    f"analytics/{source}/raw/raw_{source}",
    format="parquet",
    source_id="analytics-local",
    description=f"Raw extracted data for {source}",
)

with handle.create_version(metadata={"date": date, "source": source}) as writer:
    writer.write(df)

if writer.skipped:
    print(f"Skipped — same metadata, existing version: {writer.version}")
else:
    print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")
    