"""
CatalogSetExpectations — replace all DQ expectations on a dataset.

Idempotent: existing expectations are deleted and replaced entirely.

config:
    dataset:      str          # dataset id
    expectations: list         # list of expectation rule dicts:
        - rule_id:   str       # e.g. expect_column_values_to_not_be_null
          inputs:    dict      # e.g. {x: "this.metric"}
          params:    dict      # optional rule parameters
          tolerance: float     # 0.0–1.0 (default 1.0)
"""
from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog
from waluigi.tasks._io import _to_dict


def run():
    cfg        = _to_dict(context.config)
    dataset_id = cfg["dataset"]
    rules      = cfg.get("expectations") or []

    catalog.set_expectations(dataset_id, rules)
    print(f"Dataset '{dataset_id}': {len(rules)} expectation(s) set")


if __name__ == "__main__":
    run()
