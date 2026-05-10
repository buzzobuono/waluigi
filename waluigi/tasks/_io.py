from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import DatasetCreateRequest, DatasetFormat, SourceCreateRequest, SourceType


def create_source():
    catalog.create_source(SourceCreateRequest(
        id=context.config.catalog_source,
        type=SourceType.LOCAL,
        config={},
        description=getattr(context.config, "catalog_source_description", "Waluigi managed source"),
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
        source_id=context.config.catalog_source,
    )
    with catalog.produce(dataset, metadata=vars(context.params), inputs=lineage) as writer:
        writer.write(df)
    if writer.skipped:
        print(f"Skipped — same metadata: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")
