import pandas as pd
from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import SourceCreateRequest, SourceType

date = context.params.date
print(f"Building global report for {date} ...")

catalog.create_source(SourceCreateRequest(
    id="analytics-local",
    type=SourceType.LOCAL,
    config={},
    description="Local storage for analytics pipeline",
))

sources = ["erp", "web", "social"]
frames  = []
lineage = []

for source in sources:
    reader = catalog.read_dataset(f"analytics/{source}/clean/clean_{source}")
    df     = reader.read()
    df["pipeline_source"] = source
    frames.append(df)
    lineage.append({"dataset_id": reader.dataset_id, "version": reader.version})
    print(f"  {source}: {len(df)} rows @ {reader.version}")

report_df = pd.concat(frames, ignore_index=True)
print(f"Total rows in report: {len(report_df)}")

report_id = "analytics/reports/global_report"

handle = catalog.create_dataset(
    report_id,
    format="parquet",
    source_id="analytics-local",
    description="Global consolidated report across all sources",
)

handle.set_chart("value_by_source", "Total value by source", spec={
    "type": "bar",
    "x":   {"field": "pipeline_source", "label": "Source"},
    "y":   {"field": "value", "agg": "sum", "label": "Total Value"},
})
handle.set_chart("value_by_metric", "Value by metric", spec={
    "type": "bar",
    "x":   {"field": "metric", "label": "Metric"},
    "y":   {"field": "value", "agg": "sum", "label": "Total Value"},
})
handle.set_chart("value_share_by_source", "Value share by source", spec={
    "type": "pie",
    "x":   {"field": "pipeline_source"},
    "y":   {"field": "value", "agg": "sum"},
})
handle.set_chart("value_distribution", "Value distribution", spec={
    "type": "histogram",
    "x":   {"field": "value", "label": "Value"},
    "bins": 10,
})
handle.set_chart("value_by_category", "Value by category", spec={
    "type": "bar",
    "x":   {"field": "category", "label": "Category"},
    "y":   {"field": "value", "agg": "sum", "label": "Total Value"},
})

with handle.create_version(metadata={"date": date}, inputs=lineage) as writer:
    writer.write(report_df)

if writer.skipped:
    print(f"Skipped — same metadata, existing version: {writer.version}")
else:
    print(f"Done: {writer.dataset_id} @ {writer.version} ({len(report_df)} rows)")
