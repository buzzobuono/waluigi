"""
Shared I/O helpers for all built-in task types.

Source is declared on each dataset individually — not at config root level.
This allows inputs and output to live on different source systems.

Dataset config shape (used in input / output / left / right / inputs[*]):
    dataset:     str
    source:
        id:          str
        type:        str    # LOCAL | S3 | SQL | SFTP | API
        description: str
        config:      dict   # connector-specific (optional)
    format:      str        # output only — parquet | csv  (default: parquet)
    description: str        # output only
"""
from types import SimpleNamespace

from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.api.schemas import SourceCreateRequest, SourceType


def _to_dict(obj):
    """Recursively convert SimpleNamespace → plain dict."""
    if isinstance(obj, SimpleNamespace):
        return {k: _to_dict(v) for k, v in vars(obj).items()}
    if isinstance(obj, list):
        return [_to_dict(i) for i in obj]
    return obj


def _ensure_source(dataset_cfg: dict):
    """Upsert the source declared on a dataset config dict.
    No-op if 'source' key is absent.
    Exported for use by multi-input tasks (merge, join)."""
    src = dataset_cfg.get("source")
    if not src:
        return
    catalog.create_source(SourceCreateRequest(
        id=src["id"],
        type=SourceType[src["type"].upper()],
        config=src.get("config", {}),
        description=src.get("description", "Waluigi managed source"),
    ))


def read_input():
    inp = _to_dict(context.config.input)
    _ensure_source(inp)
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
    _ensure_source(out)
    try:
        reader = catalog.read_dataset(out["dataset"])
    except CatalogError:
        return None, None
    df = reader.read()
    print(f"  read previous {out['dataset']}: {len(df)} rows @ {reader.version}")
    return reader, df


def write_output(df, lineage):
    out = _to_dict(context.config.output)
    _ensure_source(out)
    src = out.get("source")
    if not src:
        raise ValueError(f"output.source is required (dataset: {out.get('dataset')})")
    handle = catalog.create_dataset(
        out["dataset"],
        format=out.get("format", "parquet"),
        source_id=src["id"],
        description=out.get("description", ""),
    )
    with handle.create_version(metadata=vars(context.params), inputs=lineage) as writer:
        writer.write(df)
    if writer.skipped:
        print(f"Skipped — same metadata: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")
