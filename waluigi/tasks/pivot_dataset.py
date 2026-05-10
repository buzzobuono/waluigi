"""
PivotDataset — pivot table or unpivot (melt).

Pivot config:
    input:   {dataset: str, source: {id, type, ...}}
    output:  {dataset: str, format: str, description: str, source: {id, type, ...}}
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
from waluigi.sdk.context import context
from waluigi.tasks._io import read_input, write_output


def run():
    reader, df = read_input()
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    c    = context.config
    mode = getattr(c, "mode", "pivot")

    if mode == "unpivot":
        id_vars    = c.id_vars
        value_vars = getattr(c, "value_vars", None)
        df = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=getattr(c, "var_name", "variable"),
            value_name=getattr(c, "value_name", "value"),
        )
        print(f"  unpivot id_vars={id_vars} → {len(df)} rows")
    else:
        df = pd.pivot_table(
            df,
            index=c.index,
            columns=c.columns,
            values=c.values,
            aggfunc=getattr(c, "aggfunc", "sum"),
            fill_value=getattr(c, "fill_value", 0),
        ).reset_index()
        df.columns = [str(col) for col in df.columns]   # flatten MultiIndex
        print(f"  pivot index={c.index} columns={c.columns} → {len(df)} rows, {len(df.columns)} cols")

    write_output(df, lineage)


if __name__ == "__main__":
    run()
