"""
MergeDatasets — concatenates multiple datasets vertically (pd.concat).

config:
    inputs:
        - dataset: str
          label:   str     # optional — added as column 'source_label'
    output:
        dataset:     str
        source_id:   str   # must already exist in catalog
        format:      str   (default: parquet)
        description: str
"""
import pandas as pd
from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context


def run():
    frames  = []
    lineage = []

    for inp in context.config.inputs:
        reader = catalog.read_dataset(inp["dataset"])
        df = reader.read()
        if "label" in inp:
            df["source_label"] = inp["label"]
        frames.append(df)
        lineage.append({"dataset_id": reader.dataset_id, "version": reader.version})
        print(f"  {inp['dataset']}: {len(df)} rows @ {reader.version}")

    merged = pd.concat(frames, ignore_index=True)
    print(f"Total after merge: {len(merged)} rows")

    out = context.config.output
    source_id = out.get("source_id")
    if not source_id:
        raise ValueError(f"output.source_id is required (dataset: {out.get('dataset')})")
    handle = catalog.create_dataset(
        out["dataset"],
        format=out.get("format", "parquet"),
        source_id=source_id,
        description=out.get("description", ""),
    )
    with handle.create_version(metadata=vars(context.params), inputs=lineage) as writer:
        writer.write(merged)
    if writer.skipped:
        print(f"Skipped — same metadata: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(merged)} rows)")


if __name__ == "__main__":
    run()
