"""
test_graph.py — write a dataset and apply chart definitions from a YAML file.

Run with: python tests/catalog/test_graph.py
"""
import yaml
import httpx
from pathlib import Path
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import SourceCreateRequest, SourceType

DATASET_ID  = "analytics/graph/sales_chart_test"
CHARTS_YAML = Path(__file__).parent / "sales_charts.yaml"

# ── 1. Source & dataset ───────────────────────────────────────────────────────

catalog.create_source(SourceCreateRequest(
    id="local_graph_test",
    type=SourceType.LOCAL,
    description="Local source for chart test",
))

handle = catalog.create_dataset(
    DATASET_ID,
    format="csv",
    source_id="local_graph_test",
    description="Sales data for chart visualisation test",
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
with handle.create_version(metadata={"period": "Q1-2026"}, force=False) as writer:
    writer.write(rows)
    version = writer.version
print(f"  version : {version}")

# ── 3. Read back & verify ─────────────────────────────────────────────────────

reader = catalog.read_dataset(DATASET_ID)
df     = reader.read()
print(f"  rows    : {len(df)}")
print(f"  columns : {list(df.columns)}")

# ── 4. Apply chart definitions from YAML (idempotent) ─────────────────────────

chart_defs = yaml.safe_load(CHARTS_YAML.read_text())
charts     = [handle.set_chart(c["key"], c["title"], c["spec"]) for c in chart_defs]
print(f"\nApplied {len(charts)} chart(s) from {CHARTS_YAML.name}")

# ── 5. Verify renders ─────────────────────────────────────────────────────────

BASE = catalog.url

for c in charts:
    r      = httpx.get(f"{BASE}/datasets/{DATASET_ID}/charts/{c['id']}/render")
    option = r.json()["data"]["option"]

    has_data = any(len(s.get("data", [])) > 0 for s in option.get("series", []))
    status   = "✅" if has_data else "⚠️  no data"
    print(f"  [{status}] {c['title']}")
    if option.get("radar"):
        print(f"         axes: {[i['name'] for i in option['radar'].get('indicator', [])]}")

# ── 6. Summary ────────────────────────────────────────────────────────────────

versions = catalog.list_versions(DATASET_ID)

print(f"\n{'─'*55}")
print(f"  Dataset  : {DATASET_ID}")
print(f"  Versions : {len(versions)}")
print(f"  Charts   : {len(charts)}")
print(f"{'─'*55}")
print(f"\n  Frontend : /charts/{DATASET_ID}")
