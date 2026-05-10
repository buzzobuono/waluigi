"""
MergeDatasets — concatenates multiple datasets vertically (pd.concat).

config:
    inputs:
        - dataset: str
          label:   str     # optional — added as column 'source_label'
          source:  {id, type, description, config}
    output:
        dataset:     str
        format:      str   (default: parquet)
        description: str
        source:      {id, type, description, config}   # required
"""
import pandas as pd
from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context
from waluigi.tasks._io import _ensure_source, write_output


def run():
    frames  = []
    lineage = []

    for inp in context.config.inputs:     # list of plain dicts
        _ensure_source(inp)
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
