"""
ReindexTimeSeries — gap-filling on time series datasets.

Generates a complete date index for the given frequency and range, merges it
with the input dataset, and fills missing values using the configured strategy.
Rows already present are kept as-is; only gaps get new rows.

config:
    input:
        dataset:    str
    output:
        dataset:    str
        source_id:  str
        format:     str        (default: parquet)
        description: str
    date_column: str           (default: "date")
    frequency:   str           day | week | month | year  (default: day)
    start:       str           optional — ISO date/month/year; default: min in data
    end:         str           optional — ISO date/month/year; default: max in data
    fill:
        strategy: str          ffill | bfill | zero | null | interpolate
                               (default: null)
        columns:               optional per-column override
            <col>: str         ffill | bfill | zero | null | interpolate

Frequency / date_column format:
    day   → "YYYY-MM-DD"   (pandas freq "D")
    week  → "YYYY-MM-DD"   first day of each week (freq "W-MON")
    month → "YYYY-MM"      (freq "MS")
    year  → "YYYY"         (freq "YS")

Fill strategies:
    ffill       forward-fill (carry last known value forward)
    bfill       backward-fill
    zero        fill numeric columns with 0, strings with ""
    null        leave as NaN (default)
    interpolate linear interpolation (numeric columns only; others left null)
"""
import pandas as pd

from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.sdk.context import context

_FREQ = {
    "day":   ("D",     "%Y-%m-%d"),
    "week":  ("W-MON", "%Y-%m-%d"),
    "month": ("MS",    "%Y-%m"),
    "year":  ("YS",    "%Y"),
}


def _parse_date(value: str, fmt: str) -> pd.Timestamp:
    """Parse a partial date string (e.g. '2024-01') into a Timestamp."""
    return pd.to_datetime(value)


def _apply_fill(df: pd.DataFrame, strategy: str, columns: list) -> pd.DataFrame:
    if strategy == "ffill":
        df[columns] = df[columns].ffill()
    elif strategy == "bfill":
        df[columns] = df[columns].bfill()
    elif strategy == "zero":
        for col in columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(0)
            else:
                df[col] = df[col].fillna("")
    elif strategy == "interpolate":
        for col in columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].interpolate(method="linear", limit_direction="both")
    # "null" → do nothing


def run():
    cfg         = context.config
    date_column = cfg.get("date_column", "date")
    frequency   = cfg.get("frequency", "day")

    if frequency not in _FREQ:
        raise ValueError(
            f"ReindexTimeSeries: frequency must be one of {list(_FREQ)}, got '{frequency}'")

    freq_alias, date_fmt = _FREQ[frequency]
    fill_cfg  = cfg.get("fill") or {}
    default_strategy = fill_cfg.get("strategy", "null") if isinstance(fill_cfg, dict) else fill_cfg
    col_strategies   = fill_cfg.get("columns", {}) if isinstance(fill_cfg, dict) else {}

    inp_dataset = cfg.input["dataset"]
    reader = catalog.read_dataset(inp_dataset)
    df = reader.read()
    print(f"  read {inp_dataset}: {len(df)} rows @ {reader.version}")

    if date_column not in df.columns:
        raise ValueError(
            f"ReindexTimeSeries: date_column '{date_column}' not found in dataset "
            f"(columns: {list(df.columns)})")

    df[date_column] = pd.to_datetime(df[date_column].astype(str).str[:len("2024-01")
                                     if frequency == "month" else
                                     (len("2024") if frequency == "year" else 10)])

    start_val = _parse_date(cfg.get("start"), date_fmt) if cfg.get("start") else df[date_column].min()
    end_val   = _parse_date(cfg.get("end"),   date_fmt) if cfg.get("end")   else df[date_column].max()

    full_index = pd.date_range(start=start_val, end=end_val, freq=freq_alias)
    print(f"  full index: {len(full_index)} periods ({frequency}) "
          f"from {start_val.date()} to {end_val.date()}")

    index_df = pd.DataFrame({date_column: full_index})
    merged   = index_df.merge(df, on=date_column, how="left")

    gaps = merged[date_column].isin(full_index) & merged.iloc[:, 1:].isna().all(axis=1)
    print(f"  gaps filled: {gaps.sum()} missing periods")

    value_cols = [c for c in merged.columns if c != date_column]

    # Apply per-column strategies first, then default for the rest
    handled = set()
    for col, strategy in col_strategies.items():
        if col in merged.columns:
            _apply_fill(merged, strategy, [col])
            handled.add(col)

    remaining = [c for c in value_cols if c not in handled]
    if remaining and default_strategy != "null":
        _apply_fill(merged, default_strategy, remaining)

    # Format date_column back to string using the frequency format
    merged[date_column] = merged[date_column].dt.strftime(date_fmt)

    out = cfg.output
    source_id = out.get("source_id")
    if not source_id:
        raise ValueError(f"output.source_id is required (dataset: {out.get('dataset')})")
    handle = catalog.create_dataset(
        out["dataset"],
        format=out.get("format", "parquet"),
        source_id=source_id,
        description=out.get("description", ""),
    )
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]
    with handle.create_version(metadata=vars(context.params), inputs=lineage) as writer:
        writer.write(merged)
    if writer.skipped:
        print(f"Skipped — same metadata: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(merged)} rows)")


if __name__ == "__main__":
    run()
