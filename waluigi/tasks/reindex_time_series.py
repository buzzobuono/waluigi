"""
ReindexTimeSeries — gap-filling on time series datasets.

Generates a complete date index for the given frequency and range, merges it
with the input dataset, and fills missing values using the configured strategy.
Rows already present are kept as-is; only gaps get new rows.

When `group_by` is set, the full index is the cross-product of every distinct
group value × every period. Fill strategies are then applied independently
within each group (ffill/bfill never bleed across group boundaries).

config:
    input:
        dataset:    str
    output:
        dataset:    str
        source_id:  str
        format:     str              (default: parquet)
        description: str
    group_by:    str | list[str]     optional — column(s) identifying each series
    date_column: str                 (default: "date")
    frequency:   str                 day | week | month | year  (default: day)
    start:       str                 optional — ISO date/month/year; default: min in data
    end:         str                 optional — ISO date/month/year; default: max in data
    fill:
        strategy: str                ffill | bfill | zero | null | interpolate (default: null)
        columns:                     optional per-column override
            <col>: str

Frequency / date_column format:
    day   → "YYYY-MM-DD"   (pandas freq "D")
    week  → "YYYY-MM-DD"   first Monday of each week (freq "W-MON")
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

from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context

_FREQ = {
    "day":   ("D",     "%Y-%m-%d"),
    "week":  ("W-MON", "%Y-%m-%d"),
    "month": ("MS",    "%Y-%m"),
    "year":  ("YS",    "%Y"),
}


def _parse_date(value: str, fmt: str) -> pd.Timestamp:
    return pd.to_datetime(value)


def _fill_series(s: pd.Series, strategy: str) -> pd.Series:
    if strategy == "ffill":
        return s.ffill()
    if strategy == "bfill":
        return s.bfill()
    if strategy == "zero":
        return s.fillna(0) if pd.api.types.is_numeric_dtype(s) else s.fillna("")
    if strategy == "interpolate" and pd.api.types.is_numeric_dtype(s):
        return s.interpolate(method="linear", limit_direction="both")
    return s


def _apply_fill(df: pd.DataFrame, strategy: str, columns: list,
                group_cols: list | None = None) -> None:
    for col in columns:
        if col not in df.columns:
            continue
        if group_cols:
            df[col] = df.groupby(group_cols)[col].transform(
                lambda s: _fill_series(s, strategy))
        else:
            df[col] = _fill_series(df[col], strategy)


def run():
    cfg         = context.config
    group_by    = cfg.get("group_by")
    date_column = cfg.get("date_column", "date")
    frequency   = cfg.get("frequency", "day")

    if frequency not in _FREQ:
        raise ValueError(
            f"ReindexTimeSeries: frequency must be one of {list(_FREQ)}, got '{frequency}'")

    group_cols = ([group_by] if isinstance(group_by, str) else list(group_by)) if group_by else []
    freq_alias, date_fmt = _FREQ[frequency]
    fill_cfg         = cfg.get("fill") or {}
    default_strategy = fill_cfg.get("strategy", "null") if isinstance(fill_cfg, dict) else fill_cfg
    col_strategies   = fill_cfg.get("columns", {}) if isinstance(fill_cfg, dict) else {}

    inp_dataset = cfg.input["dataset"]
    reader = catalog.read_dataset(inp_dataset)
    df = reader.read()
    print(f"  read {inp_dataset}: {len(df)} rows @ {reader.version}")

    for col in group_cols + [date_column]:
        if col not in df.columns:
            raise ValueError(
                f"ReindexTimeSeries: column '{col}' not found "
                f"(columns: {list(df.columns)})")

    df[date_column] = pd.to_datetime(
        df[date_column].astype(str).str[:
            len("2024-01") if frequency == "month" else
            (len("2024") if frequency == "year" else 10)
        ]
    )

    start_val = _parse_date(cfg.get("start"), date_fmt) if cfg.get("start") else df[date_column].min()
    end_val   = _parse_date(cfg.get("end"),   date_fmt) if cfg.get("end")   else df[date_column].max()
    full_periods = pd.date_range(start=start_val, end=end_val, freq=freq_alias)

    if group_cols:
        groups = df[group_cols].drop_duplicates().copy()
        groups["_key"] = 1
        periods_df = pd.DataFrame({date_column: full_periods, "_key": 1})
        full_index = groups.merge(periods_df, on="_key").drop(columns="_key")
        print(f"  groups: {len(groups)}  ×  periods: {len(full_periods)} ({frequency}) "
              f"→ {len(full_index)} expected rows")
        merged = full_index.merge(df, on=group_cols + [date_column], how="left")
        merged = merged.sort_values(group_cols + [date_column]).reset_index(drop=True)
    else:
        print(f"  full index: {len(full_periods)} periods ({frequency}) "
              f"from {start_val.date()} to {end_val.date()}")
        index_df = pd.DataFrame({date_column: full_periods})
        merged   = index_df.merge(df, on=date_column, how="left")

    value_cols = [c for c in merged.columns if c not in group_cols + [date_column]]
    gaps = merged[value_cols].isna().all(axis=1).sum()
    print(f"  gaps filled: {gaps} missing {'group×period' if group_cols else 'period'} combinations")

    handled = set()
    for col, strategy in col_strategies.items():
        if col in merged.columns:
            _apply_fill(merged, strategy, [col], group_cols or None)
            handled.add(col)

    remaining = [c for c in value_cols if c not in handled]
    if remaining and default_strategy != "null":
        _apply_fill(merged, default_strategy, remaining, group_cols or None)

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
