"""
Shared I/O helpers for all built-in task types.

context.config is an AttrDict — supports both attribute access (.key) and
dict access (["key"] / .get("key")) without any conversion.

Dataset config shape for reads:
    dataset: str

Dataset config shape for writes:
    dataset:     str
    source_id:   str   # must already exist in catalog
    format:      str   # parquet | csv  (default: parquet)
    description: str   # optional
"""
from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog, CatalogError


def read_input():
    dataset = context.config.input["dataset"]
    reader = catalog.read_dataset(dataset)
    df = reader.read()
    print(f"  read {dataset}: {len(df)} rows @ {reader.version}")
    return reader, df


def read_prev_output():
    """Read the latest committed version of the *output* dataset (the "gold_prev"
    pattern used by incremental tasks like AccumulateDataset / UpsertDataset).

    Returns (reader, df) or (None, None) when no prior version exists — a normal
    condition on the first run, not an error.
    """
    dataset = context.config.output["dataset"]
    try:
        reader = catalog.read_dataset(dataset)
    except CatalogError:
        return None, None
    df = reader.read()
    print(f"  read previous {dataset}: {len(df)} rows @ {reader.version}")
    return reader, df


def write_output(df, lineage):
    out = context.config.output
    source_id = out.get("source_id")
    if not source_id:
        raise ValueError(
            f"output.source_id is required — register the source first "
            f"(dataset: {out.get('dataset')})"
        )
    handle = catalog.create_dataset(
        out["dataset"],
        format=out.get("format", "parquet"),
        source_id=source_id,
        description=out.get("description", ""),
    )
    with handle.create_version(metadata=vars(context.params), inputs=lineage) as writer:
        writer.write(df)
    if writer.skipped:
        print(f"Skipped — same metadata: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")
