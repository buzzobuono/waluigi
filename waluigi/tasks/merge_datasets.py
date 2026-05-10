"""
MergeDatasets — concatenates multiple datasets vertically (pd.concat).

config:
    # source — see _io.py for both forms (simple catalog_source or full source block)
    inputs:
        - dataset: str         # dataset ID to read
          label:   str         # optional — added as column 'source_label'
    output:
        dataset:     str
        format:      str       # parquet | csv  (default: parquet)
        description: str
"""
import pandas as pd
from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context
from waluigi.tasks._io import create_source, write_output


def run():
    create_source()

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

    write_output(merged, lineage)


if __name__ == "__main__":
    run()
