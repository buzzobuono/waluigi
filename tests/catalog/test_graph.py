"""
test_graph.py — write a dataset, define charts, verify rendered ECharts option.

Data pipeline operations use the SDK (catalog.produce / catalog.resolve).
Chart management is an admin/UI feature not in the SDK, so those calls
use raw httpx — clearly isolated in a small helper block below.

Run with: python tests/catalog/test_graph.py
"""
import yaml
import httpx
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import (
    DatasetCreateRequest, DatasetFormat,
    SourceCreateRequest, SourceType,
)

# ── 1. Source & dataset setup ─────────────────────────────────────────────────

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

# ── 2. Write data ─────────────────────────────────────────────────────────────

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
    version = writer.version
print(f"  version : {version}")

# ── 3. Read back & verify ─────────────────────────────────────────────────────

reader = catalog.resolve(DATASET_ID)
df     = reader.read()
print(f"  rows    : {len(df)}")
print(f"  columns : {list(df.columns)}")

# ── 4. Chart admin (REST API — not part of the integrator SDK) ────────────────

BASE = catalog.url

def _add_chart(title, spec):
    r = httpx.post(f"{BASE}/datasets/{DATASET_ID}/charts",
                   json={"title": title, "spec": spec})
    r.raise_for_status()
    return r.json()["data"]

def _render_chart(chart_id):
    r = httpx.get(f"{BASE}/datasets/{DATASET_ID}/charts/{chart_id}/render")
    r.raise_for_status()
    return r.json()["data"]

def _list_charts():
    r = httpx.get(f"{BASE}/datasets/{DATASET_ID}/charts")
    r.raise_for_status()
    return r.json()["data"]

# ── 5. Define & render charts ─────────────────────────────────────────────────

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

print(f"\nCreating {len(SPECS)} chart(s) for '{DATASET_ID}' ...")

for title, spec in SPECS.items():
    chart    = _add_chart(title, spec)
    rendered = _render_chart(chart["id"])
    option   = rendered["option"]

    has_data = any(len(s.get("data", [])) > 0 for s in option.get("series", []))
    status   = "✅" if has_data else "⚠️  no data"
    print(f"\n  [{status}] {title}")
    print(f"       chart_id : {chart['id']}")
    print(f"       series   : {[s.get('type') for s in option.get('series', [])]}")
    if option.get("xAxis"):
        cats = option["xAxis"].get("data", [])
        print(f"       x cats   : {cats[:5]}{'…' if len(cats) > 5 else ''}")
    if option.get("radar"):
        print(f"       radar ax : {[i['name'] for i in option['radar'].get('indicator', [])]}")

# ── 6. Summary ────────────────────────────────────────────────────────────────

versions = catalog.list_versions(DATASET_ID)
lineage  = catalog.get_lineage(DATASET_ID, version)

print(f"\n{'─'*55}")
print(f"  Dataset  : {DATASET_ID}")
print(f"  Versions : {len(versions)}")
print(f"  Upstream : {len(lineage.get('upstream', []))} / Downstream: {len(lineage.get('downstream', []))}")
print(f"  Charts   : {len(_list_charts())}")
print(f"{'─'*55}")
print(f"\n  Frontend : /charts/{DATASET_ID}")
