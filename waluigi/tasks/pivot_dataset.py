"""
PivotDataset — pivot table or unpivot (melt).

Pivot config:
    input:   {dataset: str}
    output:  {dataset: str, source_id: str, format: str, description: str}
    mode:    "pivot"        # default
    index:   str | list
    columns: str
    values:  str
    aggfunc: str            # sum | mean | count | min | max  (default: sum)
    fill_value: any         # (optional, default: 0)

Unpivot (melt) config:
    mode:       "unpivot"
    id_vars:    list[str]
    value_vars: list[str]   # optional — if absent, all non-id columns
    var_name:   str         # (default: "variable")
    value_name: str         # (default: "value")
"""
import pandas as pd
from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context


def run():
    inp_dataset = context.config.input["dataset"]
    reader = catalog.read_dataset(inp_dataset)
    df = reader.read()
    print(f"  read {inp_dataset}: {len(df)} rows @ {reader.version}")
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    c    = context.config
    mode = c.get("mode", "pivot")

    if mode == "unpivot":
        id_vars    = c.id_vars
        value_vars = c.get("value_vars")
        df = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=c.get("var_name", "variable"),
            value_name=c.get("value_name", "value"),
        )
        print(f"  unpivot id_vars={id_vars} → {len(df)} rows")
    else:
        df = pd.pivot_table(
            df,
            index=c.index,
            columns=c.columns,
            values=c.values,
            aggfunc=c.get("aggfunc", "sum"),
            fill_value=c.get("fill_value", 0),
        ).reset_index()
        df.columns = [str(col) for col in df.columns]   # flatten MultiIndex
        print(f"  pivot index={c.index} columns={c.columns} → {len(df)} rows, {len(df.columns)} cols")

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
        writer.write(df)
    if writer.skipped:
        print(f"Skipped — same metadata: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")


if __name__ == "__main__":
    run()
