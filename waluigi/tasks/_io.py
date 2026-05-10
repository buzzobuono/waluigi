"""
Shared I/O helpers for all built-in task types.

Source config — two forms accepted:

  Simple (LOCAL shorthand):
    catalog_source: "my-source-id"

  Full (any source type):
    source:
      id:          "my-source-id"
      type:        "S3"           # LOCAL | S3 | SQL | SFTP | API
      description: "..."
      config:                     # connector-specific keys
        bucket: "my-bucket"
        region: "eu-west-1"
"""
from types import SimpleNamespace

from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import DatasetCreateRequest, DatasetFormat, SourceCreateRequest, SourceType


def _to_dict(obj):
    """Recursively convert SimpleNamespace → plain dict (for connector config)."""
    if isinstance(obj, SimpleNamespace):
        return {k: _to_dict(v) for k, v in vars(obj).items()}
    if isinstance(obj, list):
        return [_to_dict(i) for i in obj]
    return obj


def _source_id() -> str:
    c = context.config
    if hasattr(c, "source"):
        return c.source.id
    return c.catalog_source


def create_source():
    c = context.config
    if hasattr(c, "source"):
        src = c.source
        catalog.create_source(SourceCreateRequest(
            id=src.id,
            type=SourceType[src.type.upper()],
            config=_to_dict(getattr(src, "config", {})),
            description=getattr(src, "description", "Waluigi managed source"),
        ))
    else:
        catalog.create_source(SourceCreateRequest(
            id=c.catalog_source,
            type=SourceType.LOCAL,
            config={},
            description=getattr(c, "catalog_source_description", "Waluigi managed source"),
        ))


def read_input():
    reader = catalog.resolve(context.config.input.dataset)
    df = reader.read()
    print(f"  read {context.config.input.dataset}: {len(df)} rows @ {reader.version}")
    return reader, df


def write_output(df, lineage):
    out = context.config.output
    fmt = getattr(out, "format", "parquet").upper()
    dataset = DatasetCreateRequest(
        id=out.dataset,
        format=DatasetFormat[fmt],
        description=getattr(out, "description", ""),
        source_id=_source_id(),
    )
    with catalog.produce(dataset, metadata=vars(context.params), inputs=lineage) as writer:
        writer.write(df)
    if writer.skipped:
        print(f"Skipped — same metadata: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")
