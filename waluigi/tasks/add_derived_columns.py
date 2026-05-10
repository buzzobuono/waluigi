"""
AddDerivedColumns — appends computed columns using pandas eval expressions.
Columns are applied sequentially, so later expressions can reference earlier ones.

config:
    input:   {dataset: str, source: {id, type, ...}}
    output:  {dataset: str, format: str, description: str, source: {id, type, ...}}
    columns:
        - name: str    # new column name
          expr: str    # pandas eval expression referencing existing columns
"""
from waluigi.sdk.context import context
from waluigi.tasks._io import read_input, write_output


def run():
    reader, df = read_input()
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    for col in context.config.columns:
        df = df.eval(f"{col['name']} = {col['expr']}")
        print(f"  + {col['name']} = {col['expr']}")

    write_output(df, lineage)


if __name__ == "__main__":
    run()
