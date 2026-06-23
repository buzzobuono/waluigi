"""
CatalogCreateSource — register or update a data source in the catalog.

Idempotent: if the source already exists with the same type, it is updated.

config:
    id:          str              # unique source identifier
    type:        str              # local | s3 | sql | sftp | api
    description: str   (optional)
    config:      dict  (optional) # connector-specific (url, bucket, host, …)
"""
from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog


def run():
    cfg = context.config
    catalog.create_source(
        id=cfg["id"],
        type=cfg["type"],
        config=cfg.get("config") or {},
        description=cfg.get("description") or "",
    )
    print(f"Source '{cfg['id']}' registered (type: {cfg['type']})")


if __name__ == "__main__":
    run()
