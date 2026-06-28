"""
AddDerivedColumns — appends computed columns to a dataset.

Each column entry supports three modes (mutually exclusive):
  expr    — pandas expression; `x` refers to the full DataFrame, enabling
             string methods, type casts, and any pandas operation.
             Pure arithmetic expressions (e.g. "a + b") continue to work.
  mapping — static value→label lookup via dict. `source` names the input
             column; unmatched values become NaN.
  (both)  — not allowed; raises ValueError.

Columns are applied sequentially, so later entries can reference columns
defined earlier.

config:
    input:   {dataset: str}
    output:  {dataset: str, source_id: str, format: str, description: str}
    columns:
        - name:   str          # new column name
          expr:   str          # pandas expression; x = full DataFrame
        - name:   str
          source: str          # column to map from
          mapping: {val: label, ...}
"""
import builtins

import pandas as pd

from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context

_SAFE_BUILTINS = {k: getattr(builtins, k) for k in (
    "int", "float", "str", "bool", "list", "dict", "tuple", "set",
    "len", "round", "abs", "min", "max", "sum", "zip", "enumerate",
    "range", "sorted", "reversed", "any", "all", "isinstance", "type",
)}


def _apply_column(df: pd.DataFrame, col: dict) -> pd.DataFrame:
    name    = col["name"]
    expr    = col.get("expr")
    mapping = col.get("mapping")
    source  = col.get("source")

    if expr and mapping:
        raise ValueError(
            f"AddDerivedColumns: column '{name}' — use either 'expr' or 'mapping', not both")

    if mapping is not None:
        if not source:
            raise ValueError(
                f"AddDerivedColumns: column '{name}' with 'mapping' requires 'source'")
        # YAML parses all dict keys as strings; coerce to match source dtype
        dtype = df[source].dtype
        if pd.api.types.is_integer_dtype(dtype):
            mapping = {int(k): v for k, v in mapping.items()}
        elif pd.api.types.is_float_dtype(dtype):
            mapping = {float(k): v for k, v in mapping.items()}
        df[name] = df[source].map(mapping)
        print(f"  + {name} = map({source}, {len(mapping)} entries)")
        return df

    # expr mode — expose full DataFrame as `x` so string methods and
    # type casts work (e.g. x['col'].str[5:7].astype(int))
    local_ns = {"x": df, "pd": pd}
    df[name] = eval(expr, {"__builtins__": _SAFE_BUILTINS}, local_ns)  # noqa: S307
    print(f"  + {name} = {expr}")
    return df


def run():
    inp_dataset = context.config.input["dataset"]
    reader = catalog.read_dataset(inp_dataset)
    df = reader.read()
    print(f"  read {inp_dataset}: {len(df)} rows @ {reader.version}")
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    for col in context.config.columns:
        df = _apply_column(df, col)

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
