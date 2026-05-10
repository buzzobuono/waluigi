"""
JoinDatasets — joins two datasets horizontally (pd.merge).

config:
    left:
        dataset: str
        source:  {id, type, description, config}
    right:
        dataset: str
        source:  {id, type, description, config}
    join:
        columns:  str | list
        how:      str          # inner | left | right | outer  (default: inner)
        suffixes: list         # (default: ["_x", "_y"])
    output:
        dataset:     str
        format:      str       (default: parquet)
        description: str
        source:      {id, type, description, config}   # required
"""
import pandas as pd
from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context
from waluigi.tasks._io import _ensure_source, _to_dict, write_output


def run():
    left  = _to_dict(context.config.left)
    right = _to_dict(context.config.right)

    _ensure_source(left)
    _ensure_source(right)

    left_reader  = catalog.resolve(left["dataset"])
    right_reader = catalog.resolve(right["dataset"])

    df_left  = left_reader.read()
    df_right = right_reader.read()
    print(f"  left  {left['dataset']}: {len(df_left)} rows @ {left_reader.version}")
    print(f"  right {right['dataset']}: {len(df_right)} rows @ {right_reader.version}")

    j = context.config.join
    joined = pd.merge(
        df_left, df_right,
        on=j.columns,
        how=getattr(j, "how", "inner"),
        suffixes=getattr(j, "suffixes", ["_x", "_y"]),
    )
    print(f"After join ({getattr(j, 'how', 'inner')}): {len(joined)} rows")

    lineage = [
        {"dataset_id": left_reader.dataset_id,  "version": left_reader.version},
        {"dataset_id": right_reader.dataset_id, "version": right_reader.version},
    ]

    write_output(joined, lineage)


if __name__ == "__main__":
    run()
