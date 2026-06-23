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
from waluigi.tasks._io import write_output


def run():
    frames  = []
    lineage = []

    for inp in context.config.inputs:     # list of plain dicts
        reader = catalog.read_dataset(inp["dataset"])
        df = reader.read()
        if "label" in inp:
            df["source_label"] = inp["label"]
        frames.append(df)
        lineage.append({"dataset_id": reader.dataset_id, "version": reader.version})
        print(f"  {inp['dataset']}: {len(df)} rows @ {reader.version}")

    merged = pd.concat(frames, ignore_index=True)
    print(f"Total after merge: {len(merged)} rows")

    write_output(merged, lineage)


if __name__ == "__main__":
    run()
