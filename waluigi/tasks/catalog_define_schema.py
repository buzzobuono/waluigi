"""
CatalogDefineSchema — set semantic metadata on dataset schema columns.

Patches each listed column and optionally publishes all columns.

config:
    dataset: str
    publish: bool   (optional, default false) — promote ALL columns to published
    columns: list   (optional)
        - name:         str
          logical_type: str   (optional)
          description:  str   (optional)
          nullable:     bool  (optional)
          pii:          bool  (optional)
          pii_type:     str   (optional)  # none | direct | indirect | sensitive
          pii_notes:    str   (optional)
"""
from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog
from waluigi.tasks._io import _to_dict

_PATCH_FIELDS = ("logical_type", "description", "nullable", "pii", "pii_type", "pii_notes")


def run():
    cfg        = _to_dict(context.config)
    dataset_id = cfg["dataset"]
    columns    = cfg.get("columns") or []
    publish    = bool(cfg.get("publish", False))

    for col in columns:
        fields = {k: col[k] for k in _PATCH_FIELDS if k in col}
        if fields:
            catalog.patch_schema_column(dataset_id, col["name"], **fields)

    if publish:
        catalog.publish_schema(dataset_id)
        print(f"Dataset '{dataset_id}': {len(columns)} column(s) defined, schema published")
    else:
        print(f"Dataset '{dataset_id}': {len(columns)} column(s) defined (draft)")


if __name__ == "__main__":
    run()
