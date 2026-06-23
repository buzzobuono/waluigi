"""
DeduplicateDataset — removes duplicate rows.

config:
    input:   {dataset: str}
    output:  {dataset: str, source_id: str, format: str, description: str}
    subset:  list[str]   # columns to consider — if absent, all columns
    keep:    str         # first | last | false  (default: first)
"""
from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context


def run():
    inp_dataset = context.config.input["dataset"]
    reader = catalog.read_dataset(inp_dataset)
    df = reader.read()
    print(f"  read {inp_dataset}: {len(df)} rows @ {reader.version}")
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    subset = context.config.get("subset")
    keep   = context.config.get("keep", "first")

    before = len(df)
    df = df.drop_duplicates(subset=subset, keep=keep)
    print(f"  dedup subset={subset} keep={keep}: {before} → {len(df)} rows (removed {before - len(df)})")

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
