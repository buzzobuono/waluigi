"""
test_graph.py — create a dataset, define charts, verify rendered ECharts option.

Charts defined via YAML spec (parsed to dict by the test, sent as JSON to the API).
Run with: python tests/catalog/test_graph.py
"""
import json
import yaml
from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.models import DatasetCreateRequest, DatasetFormat, SourceCreateRequest, SourceType

# ── Dataset ───────────────────────────────────────────────────────────────────

SOURCE_ID  = "local_graph_test"
DATASET_ID = "analytics/graph/sales_chart_test"

catalog.create_source(SourceCreateRequest(
    id=SOURCE_ID,
    type=SourceType.LOCAL,
    description="Local source for chart test",
))

dataset_req = DatasetCreateRequest(
    id=DATASET_ID,
    format=DatasetFormat.CSV,
    description="Sales data for chart visualisation test",
    source_id=SOURCE_ID,
)

rows = [
    {"date": "2026-01", "category": "Electronics", "revenue": 12400.0, "units": 310},
    {"date": "2026-01", "category": "Clothing",    "revenue":  5200.0, "units": 420},
    {"date": "2026-01", "category": "Food",        "revenue":  3100.0, "units": 890},
    {"date": "2026-02", "category": "Electronics", "revenue": 14800.0, "units": 370},
    {"date": "2026-02", "category": "Clothing",    "revenue":  6100.0, "units": 510},
    {"date": "2026-02", "category": "Food",        "revenue":  3500.0, "units": 940},
    {"date": "2026-03", "category": "Electronics", "revenue": 11900.0, "units": 298},
    {"date": "2026-03", "category": "Clothing",    "revenue":  7300.0, "units": 590},
    {"date": "2026-03", "category": "Food",        "revenue":  4200.0, "units": 1100},
]

print("Writing dataset ...")
with catalog.produce(dataset_req, {"period": "Q1-2026"}, force=True) as writer:
    writer.write(rows)
    ver = writer.version
print(f"  version: {ver}")

# ── Chart specs (YAML) ────────────────────────────────────────────────────────

SPECS = {
    "Revenue by Category (bar)": yaml.safe_load("""
type: bar
x:
  field: category
  label: Category
y:
  field: revenue
  agg: sum
  label: Total Revenue (€)
"""),
    "Revenue over Time grouped by Category (line)": yaml.safe_load("""
type: line
x:
  field: date
  label: Month
y:
  field: revenue
  agg: sum
  label: Revenue (€)
color: category
"""),
    "Revenue share by Category (pie)": yaml.safe_load("""
type: pie
x:
  field: category
y:
  field: revenue
  agg: sum
"""),
    "Units distribution (histogram)": yaml.safe_load("""
type: histogram
x:
  field: units
  label: Units Sold
bins: 6
"""),
    "Revenue vs Units (scatter)": yaml.safe_load("""
type: scatter
x:
  field: units
  label: Units Sold
y:
  field: revenue
  label: Revenue (€)
limit: 100
"""),
}

# ── Create charts via API, then render ───────────────────────────────────────

print(f"\nCreating {len(SPECS)} chart(s) for dataset '{DATASET_ID}' ...")

for title, spec in SPECS.items():
    chart = catalog._post(f"/datasets/{DATASET_ID}/charts",
                          json={"title": title, "spec": spec})
    chart_id = chart["id"]

    rendered = catalog._get(f"/datasets/{DATASET_ID}/charts/{chart_id}/render")
    option   = rendered["option"]

    series_types = [s.get("type") for s in option.get("series", [])]
    has_data     = any(len(s.get("data", [])) > 0 for s in option.get("series", []))

    status = "✅" if has_data else "⚠️  no data"
    print(f"\n  [{status}] {title}")
    print(f"       chart_id : {chart_id}")
    print(f"       series   : {series_types}")
    if option.get("xAxis"):
        cats = option["xAxis"].get("data", [])
        print(f"       x cats   : {cats[:5]}{'…' if len(cats) > 5 else ''}")

# ── Final list ────────────────────────────────────────────────────────────────

all_charts = catalog._get(f"/datasets/{DATASET_ID}/charts")
print(f"\n{'─'*55}")
print(f"  Total charts registered: {len(all_charts)}")
for c in all_charts:
    print(f"  #{c['id']:>3}  [{c['spec'].get('type','?'):>9}]  {c['title']}")
print(f"{'─'*55}")
print(f"\n  Frontend: /charts/{DATASET_ID}")
