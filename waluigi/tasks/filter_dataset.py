"""
FilterDataset — keeps rows matching a pandas query expression.

config:
    input:
        dataset: str
    output:
        dataset:     str
        source_id:   str   # must already exist in catalog
        format:      str   (default: parquet)
        description: str
    where: str             # pandas query expression, e.g. "value > 1000 and category == 'finance'"
"""
from waluigi.sdk.context import context
from waluigi.tasks._io import read_input, write_output


def run():
    reader, df = read_input()
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    before = len(df)
    df = df.query(context.config.where)
    print(f"  filter '{context.config.where}': {before} → {len(df)} rows")

    write_output(df, lineage)


if __name__ == "__main__":
    run()
