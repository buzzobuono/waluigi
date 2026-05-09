import random
import pandas as pd
from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import DatasetCreateRequest, DatasetFormat, SourceCreateRequest, SourceType

source    = context.params.source.lower()
date      = context.params.date
fail_prob = float(context.attributes.fail_prob)
    
if random.random() < fail_prob:
    raise RuntimeError(f"Simulated failure while cleaning {source}")
    
catalog.create_source(SourceCreateRequest(
    id="analytics-local",
    type=SourceType.LOCAL,
    config={},
    description="Local storage for analytics pipeline",
))
    
raw_id = f"analytics/{source}/raw/raw_{source}"
reader = catalog.resolve(raw_id)
df     = reader.read()
print(f"Read {len(df)} rows from {raw_id} @ {reader.version}")
    
df = df.dropna()
df["metric"] = df["metric"].str.strip().str.lower()
df["value"]  = df["value"].astype(float)
print(f"After cleaning: {len(df)} rows")
    
clean_id = f"analytics/{source}/clean/clean_{source}"
    
dataset = DatasetCreateRequest(
    id=clean_id,
    format=DatasetFormat.PARQUET,
    description=f"Cleaned data for {source}",
    source_id="analytics-local",
)
lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]
    
with catalog.produce(dataset, metadata={"date": date, "source": source},
         inputs=lineage) as writer:
    writer.write(df)
    
if writer.skipped:
    print(f"Skipped — same metadata, existing version: {writer.version}")
else:
    print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")
    
catalog.set_expectations(clean_id, [
    {
        "rule_id":   "expect_column_values_to_not_be_null",
        "inputs":    {"x": "this.metric"},
        "tolerance": 1.0,
    },
    {
        "rule_id":   "expect_column_values_to_not_be_null",
        "inputs":    {"x": "this.value"},
        "tolerance": 1.0,
    },
    {
        "rule_id":   "expect_column_values_to_be_unique",
        "inputs":    {"x": "this.metric"},
        "tolerance": 1.0,
    },
    {
        "rule_id":   "expect_column_values_to_be_between",
        "inputs":    {"x": "this.value"},
        "params":    {"min_val": 0, "max_val": 1_000_000},
        "tolerance": 1.0,
    },
    {
        "rule_id":   "expect_column_values_to_be_of_type",
        "inputs":    {"x": "this.value"},
        "params":    {"target_type": "float"},
        "tolerance": 1.0,
    },
])
print(f"DQ expectations set on {clean_id}")
