"""
AggregateDataset — group by + aggregation.

config:
    input:
        dataset: str
    output:
        dataset:     str
        source_id:   str   # must already exist in catalog
        format:      str   (default: parquet)
        description: str
    group_by: list[str]
    agg:
        <column>: <func>   # sum | mean | count | min | max | std | first | last
"""
from waluigi.sdk.context import context
from waluigi.tasks._io import read_input, write_output


def run():
    reader, df = read_input()
    lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

    group_by = context.config.group_by
    agg_dict = dict(context.config.agg)

    df = df.groupby(group_by).agg(agg_dict).reset_index()
    print(f"  group_by={group_by} agg={agg_dict} → {len(df)} groups")

    write_output(df, lineage)


if __name__ == "__main__":
    run()
