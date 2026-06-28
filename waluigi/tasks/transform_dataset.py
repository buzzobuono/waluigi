"""
TransformDataset — apply an inline Python code block to a dataset.

Reads the input dataset into `df`, executes the `eval` block with `df`
in scope, then writes the resulting `df` to the output dataset.
Lineage is recorded automatically.

`df` can be reassigned inside the block (e.g. `df = df.groupby(...)`).
`pd` (pandas) and `context` are available without importing.

config:
    input:
        dataset:    str
    output:
        dataset:    str
        source_id:  str
        format:     str         (default: parquet)
        description: str
    eval: |
        # Python code block; df is the input DataFrame
        df["anno"] = pd.to_datetime(df["data"]).dt.year
        df = df.groupby("anno").sum().reset_index()
"""
import pandas as pd

from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context


def run():
    cfg = context.config
    inp_dataset = cfg.input["dataset"]
    reader = catalog.read_dataset(inp_dataset)
    df = reader.read()
    print(f"  read {inp_dataset}: {len(df)} rows @ {reader.version}")

    code = cfg.get("eval")
    if not code:
        raise ValueError("TransformDataset: 'eval' block is required")

    local_ns = {"df": df, "pd": pd, "context": context}
    exec(code, {"__builtins__": __builtins__}, local_ns)  # noqa: S102
    df = local_ns["df"]
    print(f"  eval → {len(df)} rows")

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


if __name__ == "__main__":
    run()
