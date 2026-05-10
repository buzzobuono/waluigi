"""
JoinDatasets — joins two datasets horizontally (pd.merge).

Expected context.config:
    catalog_source: str
    left:
        dataset: str
    right:
        dataset: str
    join:
        on:       str | list   # column(s) to join on
        how:      str          # inner | left | right | outer  (default: inner)
        suffixes: list         # e.g. ["_left", "_right"]  (default: ["_x", "_y"])
    output:
        dataset:     str
        format:      str       # parquet | csv  (default: parquet)
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

    left_reader  = catalog.resolve(context.config.left.dataset)
    right_reader = catalog.resolve(context.config.right.dataset)

    df_left  = left_reader.read()
    df_right = right_reader.read()
    print(f"  left  {context.config.left.dataset}: {len(df_left)} rows @ {left_reader.version}")
    print(f"  right {context.config.right.dataset}: {len(df_right)} rows @ {right_reader.version}")

    j = context.config.join
    joined = pd.merge(
        df_left, df_right,
        on=j.on,
        how=getattr(j, "how", "inner"),
        suffixes=getattr(j, "suffixes", ["_x", "_y"]),
    )
    print(f"After join ({j.how}): {len(joined)} rows")

    lineage = [
        {"dataset_id": left_reader.dataset_id,  "version": left_reader.version},
        {"dataset_id": right_reader.dataset_id, "version": right_reader.version},
    ]

    out = context.config.output
    fmt = getattr(out, "format", "parquet").upper()

    dataset = DatasetCreateRequest(
        id=out.dataset,
        format=DatasetFormat[fmt],
        description=getattr(out, "description", ""),
        source_id=context.config.catalog_source,
    )

    with catalog.produce(dataset, metadata=vars(context.params), inputs=lineage) as writer:
        writer.write(joined)

    if writer.skipped:
        print(f"Skipped — same metadata, existing version: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(joined)} rows)")


if __name__ == "__main__":
    run()
