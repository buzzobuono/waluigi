"""
JoinDatasets — joins two datasets horizontally (pd.merge).

config:
    left:
        dataset: str
    right:
        dataset: str
    join:
        columns:  str | list
        how:      str          # inner | left | right | outer  (default: inner)
        suffixes: list         # (default: ["_x", "_y"])
    output:
        dataset:     str
        source_id:   str       # must already exist in catalog
        format:      str       (default: parquet)
        description: str
"""
import pandas as pd
from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context


def run():
    left  = context.config.left
    right = context.config.right

    left_reader  = catalog.read_dataset(left["dataset"])
    right_reader = catalog.read_dataset(right["dataset"])

    df_left  = left_reader.read()
    df_right = right_reader.read()
    print(f"  left  {left['dataset']}: {len(df_left)} rows @ {left_reader.version}")
    print(f"  right {right['dataset']}: {len(df_right)} rows @ {right_reader.version}")

    j = context.config.join
    joined = pd.merge(
        df_left, df_right,
        on=j["columns"],
        how=j.get("how", "inner"),
        suffixes=j.get("suffixes", ["_x", "_y"]),
    )
    print(f"After join ({j.get('how', 'inner')}): {len(joined)} rows")

    lineage = [
        {"dataset_id": left_reader.dataset_id,  "version": left_reader.version},
        {"dataset_id": right_reader.dataset_id, "version": right_reader.version},
    ]

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
        writer.write(joined)
    if writer.skipped:
        print(f"Skipped — same metadata: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(joined)} rows)")


if __name__ == "__main__":
    run()
