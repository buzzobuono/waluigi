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
from waluigi.tasks._io import write_output


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

    write_output(joined, lineage)


if __name__ == "__main__":
    run()
