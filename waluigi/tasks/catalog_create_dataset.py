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


def run():
    cfg = context.config
    catalog.create_dataset(
        cfg["dataset"],
        format=cfg.get("format", "parquet"),
        source_id=cfg.get("source_id", ""),
        description=cfg.get("description", ""),
    )
    print(f"Dataset '{cfg['dataset']}' registered (format: {cfg.get('format', 'parquet')})")


if __name__ == "__main__":
    run()
