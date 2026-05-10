"""
CatalogCreateDataset — register or update a dataset in the catalog.

config:
    dataset:     str              # dataset id (path-like, e.g. "sales/raw/orders")
    source_id:   str   (optional) # registered source id
    format:      str   (optional) # parquet | csv | json | … (default: parquet)
    description: str   (optional)
"""
from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog
from waluigi.tasks._io import _to_dict


def run():
    cfg = _to_dict(context.config)
    dataset_id  = cfg["dataset"]
    source_id   = cfg.get("source_id", "")
    fmt         = cfg.get("format", "parquet")
    description = cfg.get("description", "")

    catalog.create_dataset(dataset_id, format=fmt, source_id=source_id, description=description)
    print(f"Dataset '{dataset_id}' registered (format: {fmt})")


if __name__ == "__main__":
    run()
