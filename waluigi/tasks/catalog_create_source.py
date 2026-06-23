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
from waluigi.tasks._io import _to_dict


def run():
    cfg = context.config
    connector_cfg = _to_dict(getattr(cfg, "config", None) or {})
    catalog.create_source(
        id=cfg.id,
        type=cfg.type,
        config=connector_cfg,
        description=getattr(cfg, "description", "") or "",
    )
    print(f"Source '{cfg.id}' registered (type: {cfg.type})")


if __name__ == "__main__":
    run()
