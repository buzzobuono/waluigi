"""
SelectColumns — projection: keeps only the specified columns.

config:
    input:
        dataset: str
        source:  {id, type, description, config}
    output:
        dataset: str
        format:      str   (default: parquet)
        description: str
        source:  {id, type, description, config}   # required
    columns: list[str]
"""
from waluigi.sdk.context import context
from waluigi.tasks._io import read_input, write_output


def run():
    reader, df = read_input()
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    cols = context.config.columns
    df = df[cols]
    print(f"  selected {len(cols)} columns: {cols}")

    write_output(df, lineage)


if __name__ == "__main__":
    run()
