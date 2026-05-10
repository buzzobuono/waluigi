"""
JoinDatasets — joins two datasets horizontally (pd.merge).

config:
    # source — see _io.py for both forms (simple catalog_source or full source block)
    left:
        dataset: str
    right:
        dataset: str
    join:
        columns:  str | list   # column(s) to join on
        how:      str          # inner | left | right | outer  (default: inner)
        suffixes: list         # e.g. ["_left", "_right"]  (default: ["_x", "_y"])
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

    left_reader  = catalog.resolve(context.config.left.dataset)
    right_reader = catalog.resolve(context.config.right.dataset)

    df_left  = left_reader.read()
    df_right = right_reader.read()
    print(f"  left  {context.config.left.dataset}: {len(df_left)} rows @ {left_reader.version}")
    print(f"  right {context.config.right.dataset}: {len(df_right)} rows @ {right_reader.version}")

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
