"""
AccumulateDeduplicateDataset — fact table with cross-day deduplication by state.

A variant of AccumulateDataset for "operational funnel" / state-history tables.
Instead of appending today's snapshot verbatim, it keeps a single row per unique
state (all columns except ``date_column``), dated with the *first* day that state
was observed. Rows that do not change day-to-day are therefore not duplicated, and
the dataset grows only on real state changes.

Each run reads the previous output (gold) version, concatenates today's input
(prev first, today after), sorts by ``date_column`` and drops duplicates on every
column except the date with ``keep="first"`` — so the oldest observed date wins for
each distinct state. Same-day re-runs are idempotent: the dedup absorbs the repeat,
and reserving with ``force=False`` skips identical metadata.

Lineage records both inputs: today's input and the previous gold version.

config:
    input:   {dataset: str}
    output:  {dataset: str, source_id: str, format: str, description: str}
    date_column: str   # date column used for ordering / partition (default: "date")
    date_param:  str   # job param holding today's date value      (default: "date")
"""
import pandas as pd

from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.sdk.context import context


def run():
    date_column = context.config.get("date_column", "date")
    date_param  = context.config.get("date_param",  "date")
    date_value  = getattr(context.params, date_param, None)
    if date_value is None:
        raise ValueError(
            f"AccumulateDeduplicateDataset: job param '{date_param}' is required "
            f"(used as date_param)")

    inp_dataset = context.config.input["dataset"]
    reader = catalog.read_dataset(inp_dataset)
    df_today = reader.read()
    print(f"  read {inp_dataset}: {len(df_today)} rows @ {reader.version}")
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    if date_column not in df_today.columns:
        df_today[date_column] = date_value

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

    df_all   = pd.concat(frames, ignore_index=True)
    before   = len(df_all)
    key_cols = [c for c in df_all.columns if c != date_column]
    df_gold  = (df_all
                .sort_values(date_column)
                .drop_duplicates(subset=key_cols, keep="first")
                .reset_index(drop=True))
    print(f"Cross-day dedup on {key_cols}: {before} → {len(df_gold)} rows "
          f"({before - len(df_gold)} removed)")

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
