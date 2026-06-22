"""
UpsertDataset — SCD Type 1 dimension table.

The canonical built-in for daily dimension tables in a medallion architecture.
Each run reads the previous output (gold) version, concatenates today's input,
and keeps the last record per business ``key`` (``keep="last"``), so newer rows
always win on a key collision. Records that disappear from the source are *not*
deleted — they remain in the output. Running the same day twice is idempotent:
``write_output`` reserves with ``force=False``, so identical metadata skips.

Lineage records both inputs: today's bronze and the previous gold version.

config:
    input:   {dataset: str, source: {id, type, ...}}
    output:  {dataset: str, format: str, description: str, source: {id, type, ...}}
    key:     str | list[str]   # business key column(s) — required
"""
import pandas as pd

from waluigi.sdk.context import context
from waluigi.tasks._io import read_input, read_prev_output, write_output


def run():
    key = getattr(context.config, "key", None)
    if not key:
        raise ValueError("UpsertDataset: 'key' is required")
    key = [key] if isinstance(key, str) else list(key)

    reader, df_today = read_input()
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    missing = [k for k in key if k not in df_today.columns]
    if missing:
        raise KeyError(f"UpsertDataset: key column(s) {missing} not found for upsert key")

    prev_reader, df_prev = read_prev_output()
    if df_prev is not None:
        # Order is deliberate: [prev, today] + keep="last" → today wins on collision.
        frames = [df_prev, df_today]
        lineage.append({"dataset_id": prev_reader.dataset_id,
                        "version": prev_reader.version})
    else:
        print("  first run — no previous output")
        frames = [df_today]

    df_all  = pd.concat(frames, ignore_index=True)
    before  = len(df_all)
    df_gold = df_all.drop_duplicates(subset=key, keep="last").reset_index(drop=True)
    print(f"Upsert key={key}: {before} → {len(df_gold)} rows")

    write_output(df_gold, lineage)


if __name__ == "__main__":
    run()
