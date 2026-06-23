"""
AggregateDataset — group by + aggregation.

config:
    input:
        dataset: str
    output:
        dataset:     str
        source_id:   str   # must already exist in catalog
        format:      str   (default: parquet)
        description: str
    group_by: list[str]
    agg:
        <column>: <func>   # sum | mean | count | min | max | std | first | last
"""
from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context


def run():
    inp_dataset = context.config.input["dataset"]
    reader = catalog.read_dataset(inp_dataset)
    df = reader.read()
    print(f"  read {inp_dataset}: {len(df)} rows @ {reader.version}")
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    group_by = context.config.group_by
    agg_dict = dict(context.config.agg)

    df = df.groupby(group_by).agg(agg_dict).reset_index()
    print(f"  group_by={group_by} agg={agg_dict} → {len(df)} groups")

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
        writer.write(df)
    if writer.skipped:
        print(f"Skipped — same metadata: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")


if __name__ == "__main__":
    run()
