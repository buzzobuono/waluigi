"""
CatalogSetCharts — upsert chart definitions on a dataset.

Idempotent: each chart is upserted by key (created if absent, updated if present).

config:
    dataset: str     # dataset id
    charts:  list    # list of chart dicts:
        - key:   str               # unique chart key
          title: str
          spec:  dict              # chart spec (type, x, y, bins, …)
"""
from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog
from waluigi.tasks._io import _to_dict


def run():
    cfg        = _to_dict(context.config)
    dataset_id = cfg["dataset"]
    charts     = cfg.get("charts") or []

    existing = {c["key"]: c for c in catalog.list_charts(dataset_id)}
    for i, chart in enumerate(charts):
        key   = chart["key"]
        title = chart["title"]
        spec  = chart.get("spec", {})
        body  = {"key": key, "title": title, "spec": spec, "position": i}
        if key in existing:
            catalog._patch(catalog._ns_url(f"/datasets/{dataset_id}/charts/{existing[key]['id']}"), json=body)
        else:
            catalog._post(catalog._ns_url(f"/datasets/{dataset_id}/charts"), json=body)

    print(f"Dataset '{dataset_id}': {len(charts)} chart(s) set")


if __name__ == "__main__":
    run()
