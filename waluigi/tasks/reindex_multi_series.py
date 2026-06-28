"""
ReindexMultiSeries — gap-filling on grouped time series datasets.

Extends ReindexTimeSeries with a `group_by` parameter: generates the full
cross-product (every group × every period), left-joins the input data onto it,
and applies fill strategies independently within each group.

Use this when your dataset has one time series per category (e.g. revenue per
product, applications per year) and you need every group to have a row for
every period in the range — even periods with no data.

config:
    input:
        dataset:    str
    output:
        dataset:    str
        source_id:  str
        format:     str        (default: parquet)
        description: str
    group_by:    str | list[str]   column(s) that identify each series
    date_column: str               (default: "date")
    frequency:   str               day | week | month | year  (default: day)
    start:       str               optional — ISO date/month/year; default: min in data
    end:         str               optional — ISO date/month/year; default: max in data
    fill:
        strategy: str              ffill | bfill | zero | null | interpolate (default: null)
        columns:                   optional per-column override
            <col>: str

Fill strategies are applied independently within each group — ffill/bfill
will not bleed values across group boundaries.

Frequency / date_column format:
    day   → "YYYY-MM-DD"
    week  → "YYYY-MM-DD"  (first Monday of each week)
    month → "YYYY-MM"
    year  → "YYYY"
"""
import pandas as pd

from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context
from waluigi.tasks.reindex_time_series import _FREQ, _parse_date, _apply_fill


def run():
    cfg         = context.config
    group_by    = cfg.get("group_by")
    date_column = cfg.get("date_column", "date")
    frequency   = cfg.get("frequency", "day")

    if not group_by:
        raise ValueError("ReindexMultiSeries: group_by is required")
    if frequency not in _FREQ:
        raise ValueError(
            f"ReindexMultiSeries: frequency must be one of {list(_FREQ)}, got '{frequency}'")

    group_cols = [group_by] if isinstance(group_by, str) else list(group_by)
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
                f"ReindexMultiSeries: column '{col}' not found "
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
    groups       = df[group_cols].drop_duplicates()
    print(f"  groups: {len(groups)}  ×  periods: {len(full_periods)} ({frequency}) "
          f"→ {len(groups) * len(full_periods)} expected rows")

    # Cross-join: every group × every period
    periods_df = pd.DataFrame({date_column: full_periods})
    periods_df["_key"] = 1
    groups = groups.copy()
    groups["_key"] = 1
    full_index = groups.merge(periods_df, on="_key").drop(columns="_key")

    merged = full_index.merge(df, on=group_cols + [date_column], how="left")

    gaps = merged.iloc[:, len(group_cols) + 1:].isna().all(axis=1).sum()
    print(f"  gaps filled: {gaps} missing group×period combinations")

    value_cols = [c for c in merged.columns if c not in group_cols + [date_column]]

    # Apply fill strategies independently within each group
    merged = merged.sort_values(group_cols + [date_column]).reset_index(drop=True)

    handled = set()
    for col, strategy in col_strategies.items():
        if col in merged.columns:
            merged[col] = (merged.groupby(group_cols)[col]
                           .transform(lambda s: _fill_series(s, strategy)))
            handled.add(col)

    if default_strategy != "null":
        remaining = [c for c in value_cols if c not in handled]
        for col in remaining:
            merged[col] = (merged.groupby(group_cols)[col]
                           .transform(lambda s: _fill_series(s, default_strategy)))

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


if __name__ == "__main__":
    run()
