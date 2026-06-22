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
and ``write_output`` reserves with ``force=False`` so identical metadata skips.

Lineage records both inputs: today's input and the previous gold version.

config:
    input:   {dataset: str, source: {id, type, ...}}
    output:  {dataset: str, format: str, description: str, source: {id, type, ...}}
    date_column: str   # date column used for ordering / partition (default: "date")
    date_param:  str   # job param holding today's date value      (default: "date")
"""
import pandas as pd

from waluigi.sdk.context import context
from waluigi.tasks._io import read_input, read_prev_output, write_output


def run():
    date_column = getattr(context.config, "date_column", "date")
    date_param  = getattr(context.config, "date_param",  "date")
    date_value  = getattr(context.params, date_param, None)
    if date_value is None:
        raise ValueError(
            f"AccumulateDeduplicateDataset: job param '{date_param}' is required "
            f"(used as date_param)")

    reader, df_today = read_input()
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    # Ensure today's data carries the date column (needed for ordering / dedup).
    if date_column not in df_today.columns:
        df_today[date_column] = date_value

    prev_reader, df_prev = read_prev_output()
    if df_prev is not None:
        # prev first, today after → sort + keep="first" preserves the oldest date.
        frames = [df_prev, df_today]
        lineage.append({"dataset_id": prev_reader.dataset_id,
                        "version": prev_reader.version})
    else:
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

    write_output(df_gold, lineage)


if __name__ == "__main__":
    run()
