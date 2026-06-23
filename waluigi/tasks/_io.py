"""
Shared I/O helpers for all built-in task types.

Each dataset config requires only `dataset` for reads.  Writes additionally
require `source_id` (a pre-existing source ID string) and optionally
`format` and `description`.

Dataset config shape for reads:
    dataset: str

Dataset config shape for writes:
    dataset:     str
    source_id:   str   # must already exist in catalog
    format:      str   # parquet | csv  (default: parquet)
    description: str   # optional
"""
from types import SimpleNamespace

from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog, CatalogError


def _to_dict(obj):
    """Recursively convert SimpleNamespace → plain dict."""
    if isinstance(obj, SimpleNamespace):
        return {k: _to_dict(v) for k, v in vars(obj).items()}
    if isinstance(obj, list):
        return [_to_dict(i) for i in obj]
    return obj


def read_input():
    inp = _to_dict(context.config.input)
    reader = catalog.read_dataset(inp["dataset"])
    df = reader.read()
    print(f"  read {inp['dataset']}: {len(df)} rows @ {reader.version}")
    return reader, df


def read_prev_output():
    """Read the latest committed version of the *output* dataset (the "gold_prev"
    pattern used by incremental tasks like AccumulateDataset / UpsertDataset).

    Returns (reader, df) or (None, None) when no prior version exists — a normal
    condition on the first run, not an error.
    """
    out = _to_dict(context.config.output)
    try:
        reader = catalog.read_dataset(out["dataset"])
    except CatalogError:
        return None, None
    df = reader.read()
    print(f"  read previous {out['dataset']}: {len(df)} rows @ {reader.version}")
    return reader, df


def write_output(df, lineage):
    out = _to_dict(context.config.output)
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
