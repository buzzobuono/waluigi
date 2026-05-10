"""
MergeDatasets — concatenates multiple datasets vertically (pd.concat).

Expected context.config:
    catalog_source: str
    inputs:
        - dataset: str         # dataset ID to read
          label: str           # optional column added as 'source_label'
    output:
        dataset: str
        format:  str           # parquet | csv  (default: parquet)
        description: str
"""
import pandas as pd
from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import DatasetCreateRequest, DatasetFormat, SourceCreateRequest, SourceType


def run():
    catalog.create_source(SourceCreateRequest(
        id=context.config.catalog_source,
        type=SourceType.LOCAL,
        config={},
    ))

    frames  = []
    lineage = []

    for inp in context.config.inputs:
        reader = catalog.resolve(inp["dataset"])
        df = reader.read()
        if "label" in inp:
            df["source_label"] = inp["label"]
        frames.append(df)
        lineage.append({"dataset_id": reader.dataset_id, "version": reader.version})
        print(f"  {inp['dataset']}: {len(df)} rows @ {reader.version}")

    merged = pd.concat(frames, ignore_index=True)
    print(f"Total after merge: {len(merged)} rows")

    out = context.config.output
    fmt = getattr(out, "format", "parquet").upper()

    dataset = DatasetCreateRequest(
        id=out.dataset,
        format=DatasetFormat[fmt],
        description=getattr(out, "description", ""),
        source_id=context.config.catalog_source,
    )

    with catalog.produce(dataset, metadata=vars(context.params), inputs=lineage) as writer:
        writer.write(merged)

    if writer.skipped:
        print(f"Skipped — same metadata, existing version: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(merged)} rows)")


if __name__ == "__main__":
    run()
