"""
test_graph.py — create a dataset, define charts, verify rendered ECharts option.

Charts are an admin/UI feature, so this test calls the REST API directly
via the catalog client's internal transport (not part of the integrator SDK).
Run with: python tests/catalog/test_graph.py
"""
import json
import yaml
import httpx
from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.models import DatasetCreateRequest, DatasetFormat, SourceCreateRequest, SourceType

BASE = catalog.url

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
    {"date": "2026-01", "category": "Electronics", "revenue": 12400.0, "units": 310, "returns": 18},
    {"date": "2026-01", "category": "Clothing",    "revenue":  5200.0, "units": 420, "returns": 42},
    {"date": "2026-01", "category": "Food",        "revenue":  3100.0, "units": 890, "returns":  5},
    {"date": "2026-02", "category": "Electronics", "revenue": 14800.0, "units": 370, "returns": 21},
    {"date": "2026-02", "category": "Clothing",    "revenue":  6100.0, "units": 510, "returns": 55},
    {"date": "2026-02", "category": "Food",        "revenue":  3500.0, "units": 940, "returns":  7},
    {"date": "2026-03", "category": "Electronics", "revenue": 11900.0, "units": 298, "returns": 15},
    {"date": "2026-03", "category": "Clothing",    "revenue":  7300.0, "units": 590, "returns": 61},
    {"date": "2026-03", "category": "Food",        "revenue":  4200.0, "units": 1100, "returns":  9},
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
    "Category performance radar (radar)": yaml.safe_load("""
type: radar
group_by: category
axes:
  - field: revenue
    label: Revenue (€)
    max: 50000
  - field: units
    label: Units Sold
    max: 3500
  - field: returns
    label: Returns
    max: 200
agg: sum
"""),
}

# ── Create charts via API, then render ───────────────────────────────────────

print(f"\nCreating {len(SPECS)} chart(s) for dataset '{DATASET_ID}' ...")

def _add_chart(dataset_id, title, spec):
    r = httpx.post(f"{BASE}/datasets/{dataset_id}/charts",
                   json={"title": title, "spec": spec})
    r.raise_for_status()
    return r.json()["data"]

def _render_chart(dataset_id, chart_id):
    r = httpx.get(f"{BASE}/datasets/{dataset_id}/charts/{chart_id}/render")
    r.raise_for_status()
    return r.json()["data"]

def _list_charts(dataset_id):
    r = httpx.get(f"{BASE}/datasets/{dataset_id}/charts")
    r.raise_for_status()
    return r.json()["data"]

for title, spec in SPECS.items():
    chart    = _add_chart(DATASET_ID, title, spec)
    chart_id = chart["id"]

    rendered = _render_chart(DATASET_ID, chart_id)
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
    if option.get("radar"):
        axes = [i["name"] for i in option["radar"].get("indicator", [])]
        print(f"       radar ax : {axes}")

# ── Final list ────────────────────────────────────────────────────────────────

all_charts = _list_charts(DATASET_ID)
print(f"\n{'─'*55}")
print(f"  Total charts registered: {len(all_charts)}")
for c in all_charts:
    print(f"  #{c['id']:>3}  [{c['spec'].get('type','?'):>9}]  {c['title']}")
print(f"{'─'*55}")
print(f"\n  Frontend: /charts/{DATASET_ID}")
