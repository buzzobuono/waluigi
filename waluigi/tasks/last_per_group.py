"""
LastPerGroup — keep the last row per group after sorting.
FirstPerGroup — keep the first row per group after sorting.

Sorts the dataset by `order_by`, then retains only the last (or first) row
for each unique combination of `group_by` columns. All columns are preserved —
no aggregation, no column list required.

config:
    input:
        dataset:    str
    output:
        dataset:    str
        source_id:  str
        format:     str     (default: parquet)
        description: str
    group_by:   str | list[str]   column(s) that define each group
    order_by:   str | list[str]   column(s) to sort by before picking
    ascending:  bool              sort order  (default: true)
"""
import pandas as pd

from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context


def _run(keep: str):
    cfg = context.config
    group_by = cfg.get("group_by")
    order_by = cfg.get("order_by")

    if not group_by:
        raise ValueError(f"{keep}PerGroup: 'group_by' is required")
    if not order_by:
        raise ValueError(f"{keep}PerGroup: 'order_by' is required")

    group_cols = [group_by] if isinstance(group_by, str) else list(group_by)
    order_cols = [order_by] if isinstance(order_by, str) else list(order_by)
    ascending  = cfg.get("ascending", True)

    inp_dataset = cfg.input["dataset"]
    reader = catalog.read_dataset(inp_dataset)
    df = reader.read()
    print(f"  read {inp_dataset}: {len(df)} rows @ {reader.version}")

    for col in group_cols + order_cols:
        if col not in df.columns:
            raise ValueError(
                f"column '{col}' not found (columns: {list(df.columns)})")

    df = df.sort_values(order_cols, ascending=ascending)
    agg = df.groupby(group_cols, sort=False)
    df = (agg.last() if keep == "last" else agg.first()).reset_index()

    print(f"  {keep} per group → {len(df)} rows ({len(group_cols)} group col(s))")

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
        writer.write(df)
    if writer.skipped:
        print(f"Skipped — same metadata: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")


def run():
    _run("last")


if __name__ == "__main__":
    run()
