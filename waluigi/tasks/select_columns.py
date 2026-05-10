"""
SelectColumns — projection: keeps only the specified columns.

config:
    catalog_source: str
    input:
        dataset: str
    output:
        dataset: str
        format:      str   (default: parquet)
        description: str
    columns: list[str]
"""
from waluigi.sdk.context import context
from waluigi.tasks._io import create_source, read_input, write_output


def run():
    create_source()
    reader, df = read_input()
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    cols = context.config.columns
    df = df[cols]
    print(f"  selected {len(cols)} columns: {cols}")

    write_output(df, lineage)


if __name__ == "__main__":
    run()
