"""
AccumulateDataset — append-only fact table with per-date idempotency.

The canonical built-in for daily fact tables in a medallion (Bronze→Silver→Gold)
architecture. Each run reads the previous output (gold) version, drops the rows
belonging to the current date, appends today's input, and writes a new output
version. Running the same day twice is idempotent on two levels:

  * row-level — rows for ``date_param`` are removed from the previous gold before
    today's input is appended, so a re-run never duplicates the day;
  * version-level — ``write_output`` reserves with ``force=False``, so an identical
    metadata set (same params) skips the write entirely.

Lineage records both inputs: today's bronze and the previous gold version.

config:
    input:   {dataset: str}
    output:  {dataset: str, source_id: str, format: str, description: str}
    date_column: str   # date partition column in the dataframe (default: "date")
    date_param:  str   # job param holding today's date value   (default: "date")
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
            f"AccumulateDataset: job param '{date_param}' is required (used as date_param)")

    reader, df_today = read_input()
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    # Ensure today's data carries the partition column.
    if date_column not in df_today.columns:
        df_today[date_column] = date_value

    prev_reader, df_prev = read_prev_output()
    if df_prev is not None:
        before = len(df_prev)
        # Compare as strings: the partition column is stored ISO 'YYYY-MM-DD' and
        # params arrive as strings — avoids forcing dtype conversions on the frame.
        keep = df_prev[date_column].astype(str) != str(date_value)
        df_prev = df_prev[keep]
        print(f"  removed {before - len(df_prev)} existing rows for "
              f"{date_column}={date_value}")
        frames = [df_prev, df_today]
        lineage.append({"dataset_id": prev_reader.dataset_id,
                        "version": prev_reader.version})
    else:
        print("  first run — no previous output")
        frames = [df_today]

    df_gold = pd.concat(frames, ignore_index=True)
    print(f"Accumulated: {len(df_gold)} rows total")

    write_output(df_gold, lineage)


if __name__ == "__main__":
    run()
