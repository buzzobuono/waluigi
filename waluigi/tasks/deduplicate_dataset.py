"""
DeduplicateDataset — removes duplicate rows.

config:
    input:   {dataset: str}
    output:  {dataset: str, source_id: str, format: str, description: str}
    subset:  list[str]   # columns to consider — if absent, all columns
    keep:    str         # first | last | false  (default: first)
"""
from waluigi.sdk.context import context
from waluigi.tasks._io import read_input, write_output


def run():
    reader, df = read_input()
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    subset = getattr(context.config, "subset", None)
    keep   = getattr(context.config, "keep",   "first")

    before = len(df)
    df = df.drop_duplicates(subset=subset, keep=keep)
    print(f"  dedup subset={subset} keep={keep}: {before} → {len(df)} rows (removed {before - len(df)})")

    write_output(df, lineage)


if __name__ == "__main__":
    run()
