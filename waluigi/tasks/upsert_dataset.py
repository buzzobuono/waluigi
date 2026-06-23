"""
UpsertDataset — SCD Type 1 dimension table.

The canonical built-in for daily dimension tables in a medallion architecture.
Each run reads the previous output (gold) version, concatenates today's input,
and keeps the last record per business ``key`` (``keep="last"``), so newer rows
always win on a key collision. Records that disappear from the source are *not*
deleted — they remain in the output. Running the same day twice is idempotent:
reserving with ``force=False`` skips the write when metadata is identical.

Lineage records both inputs: today's bronze and the previous gold version.

config:
    input:   {dataset: str}
    output:  {dataset: str, source_id: str, format: str, description: str}
    key:     str | list[str]   # business key column(s) — required
"""
import pandas as pd

from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.sdk.context import context


def run():
    key = context.config.get("key")
    if not key:
        raise ValueError("UpsertDataset: 'key' is required")
    key = [key] if isinstance(key, str) else list(key)

    inp_dataset = context.config.input["dataset"]
    reader = catalog.read_dataset(inp_dataset)
    df_today = reader.read()
    print(f"  read {inp_dataset}: {len(df_today)} rows @ {reader.version}")
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    missing = [k for k in key if k not in df_today.columns]
    if missing:
        raise KeyError(f"UpsertDataset: key column(s) {missing} not found for upsert key")

    out_dataset = context.config.output["dataset"]
    try:
        prev_reader = catalog.read_dataset(out_dataset)
        df_prev = prev_reader.read()
        print(f"  read previous {out_dataset}: {len(df_prev)} rows @ {prev_reader.version}")
        frames = [df_prev, df_today]
        lineage.append({"dataset_id": prev_reader.dataset_id, "version": prev_reader.version})
    except CatalogError:
        print("  first run — no previous output")
        frames = [df_today]

    df_all  = pd.concat(frames, ignore_index=True)
    before  = len(df_all)
    df_gold = df_all.drop_duplicates(subset=key, keep="last").reset_index(drop=True)
    print(f"Upsert key={key}: {before} → {len(df_gold)} rows")

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
        writer.write(df_gold)
    if writer.skipped:
        print(f"Skipped — same metadata: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df_gold)} rows)")


if __name__ == "__main__":
    run()
