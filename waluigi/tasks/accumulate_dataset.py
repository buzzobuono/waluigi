"""
AccumulateDataset — append-only fact table with per-date idempotency.

The canonical built-in for daily fact tables in a medallion (Bronze→Silver→Gold)
architecture. Each run reads the previous output (gold) version, drops the rows
belonging to the current date, appends today's input, and writes a new output
version. Running the same day twice is idempotent on two levels:

  * row-level — rows for ``date_param`` are removed from the previous gold before
    today's input is appended, so a re-run never duplicates the day;
  * version-level — reserving with ``force=False`` skips the write entirely when
    the metadata set (same params) is identical.

Lineage records both inputs: today's bronze and the previous gold version.

config:
    input:   {dataset: str}
    output:  {dataset: str, source_id: str, format: str, description: str}
    date_column: str   # date partition column in the dataframe (default: "date")
    date_param:  str   # job param holding today's date value   (default: "date")
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
            f"AccumulateDataset: job param '{date_param}' is required (used as date_param)")

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
        before = len(df_prev)
        keep = df_prev[date_column].astype(str) != str(date_value)
        df_prev = df_prev[keep]
        print(f"  removed {before - len(df_prev)} existing rows for {date_column}={date_value}")
        frames = [df_prev, df_today]
        lineage.append({"dataset_id": prev_reader.dataset_id, "version": prev_reader.version})
    except CatalogError:
        print("  first run — no previous output")
        frames = [df_today]

    df_gold = pd.concat(frames, ignore_index=True)
    print(f"Accumulated: {len(df_gold)} rows total")

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
