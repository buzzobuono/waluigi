from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.models import *

# ---------------------------------------------------------------------------
# Expectations — stored in the DB, no file needed
# ---------------------------------------------------------------------------

EXPECTATIONS = [
    # completeness
    {"rule_id": "expect_column_values_to_not_be_null",
     "inputs": {"x": "this.product"}},
    {"rule_id": "expect_column_values_to_not_be_null",
     "inputs": {"x": "this.quantity"}},
    {"rule_id": "expect_column_values_to_not_be_null",
     "inputs": {"x": "this.revenue"}},
    # range checks
    {"rule_id": "expect_column_values_to_be_between",
     "inputs": {"x": "this.quantity"},
     "params": {"min_val": 1, "max_val": 100}},
    {"rule_id": "expect_column_mean_to_be_between",
     "inputs": {"x": "this.revenue"},
     "params": {"min_avg": 10.0, "max_avg": 5000.0}},
    # uniqueness
    {"rule_id": "expect_column_values_to_be_unique",
     "inputs": {"x": "this.product"}},
    # format
    {"rule_id": "expect_column_values_to_match_regex",
     "inputs": {"x": "this.product"},
     "params": {"pattern": r"^PROD_\d{4}$"}},
    # domain
    {"rule_id": "expect_column_values_to_be_in_set",
     "inputs": {"x": "this.category"},
     "params": {"allowed_values": ["A", "B", "C", "D", "E"]}},
]

# ---------------------------------------------------------------------------
# Source / Dataset
# ---------------------------------------------------------------------------

SOURCE_ID  = "local_dq"
DATASET_ID = "sales/dq/sales_dq_test"

catalog.create_source(SourceCreateRequest(
    id=SOURCE_ID,
    type=SourceType.LOCAL,
    description="Local source for DQ test",
))

dataset_req = DatasetCreateRequest(
    id=DATASET_ID,
    format=DatasetFormat.CSV,
    description="Sales dataset with DQ monitoring",
    source_id=SOURCE_ID,
)
catalog.create_dataset(dataset_req)

# Set expectations in DB (replaces file-based dq_suite)
saved = catalog.set_expectations(DATASET_ID, EXPECTATIONS)
print(f"Expectations set : {len(saved)} rules registered in DB")

# ---------------------------------------------------------------------------
# Helper: print DQ summary from version metadata
# ---------------------------------------------------------------------------

def print_dq(label: str, meta: dict):
    print(f"\n{'─'*50}")
    print(f"  {label}")
    print(f"{'─'*50}")
    if "sys.dq.error" in meta:
        print(f"  ⚠️  DQ error : {meta['sys.dq.error']}")
        return
    if "sys.dq.score" not in meta:
        print("  ℹ️  No DQ results (no expectations configured or run skipped)")
        return
    score   = float(meta["sys.dq.score"])
    passed  = meta["sys.dq.passed"]
    total   = meta["sys.dq.total"]
    success = meta["sys.dq.success"] == "True"
    status  = "✅ PASSED" if success else "❌ FAILED"
    print(f"  Status  : {status}")
    print(f"  Score   : {score*100:.1f}%  ({passed}/{total} rules passed)")
    print()
    biz = {k: v for k, v in sorted(meta.items()) if not k.startswith("sys.")}
    for k, v in biz.items():
        print(f"  {k}: {v}")

# ---------------------------------------------------------------------------
# Version 1: clean data — expect all 8 rules to pass
# ---------------------------------------------------------------------------

rows_clean = [
    {"product": "PROD_0001", "quantity": 10, "revenue": 100.0, "category": "A"},
    {"product": "PROD_0002", "quantity": 25, "revenue": 250.0, "category": "B"},
    {"product": "PROD_0003", "quantity": 50, "revenue": 500.0, "category": "C"},
    {"product": "PROD_0004", "quantity":  5, "revenue":  50.0, "category": "D"},
    {"product": "PROD_0005", "quantity": 80, "revenue": 800.0, "category": "E"},
]

print("\nProducendo versione 1 — dati puliti ...")
with catalog.produce(dataset_req, {"batch": "v1"}) as writer:
    writer.write(rows_clean)
    ver1 = writer.version

meta1 = catalog._get(f"/datasets/{DATASET_ID}/versions/{ver1}/metadata")
print_dq("Versione 1 — dati puliti (atteso: 8/8)", meta1)

# ---------------------------------------------------------------------------
# Version 2: dirty data — several rules should fail:
#   - PROD_0001 duplicated       → unique rule fails
#   - quantity 150               → between rule fails (row score < 1)
#   - revenue = None             → not_null fails + mean could be affected
#   - category "X"               → in_set fails
# ---------------------------------------------------------------------------

rows_dirty = [
    {"product": "PROD_0001", "quantity":  10, "revenue": 100.0, "category": "A"},
    {"product": "PROD_0001", "quantity":  25, "revenue": 250.0, "category": "B"},  # duplicate
    {"product": "PROD_0003", "quantity": 150, "revenue": 500.0, "category": "C"},  # qty out of range
    {"product": "PROD_0004", "quantity":   5, "revenue":  None, "category": "X"},  # null revenue + bad category
]

print("\nProducendo versione 2 — dati sporchi ...")
with catalog.produce(dataset_req, {"batch": "v2"}) as writer:
    writer.write(rows_dirty)
    ver2 = writer.version

meta2 = catalog._get(f"/datasets/{DATASET_ID}/versions/{ver2}/metadata")
print_dq("Versione 2 — dati sporchi (atteso: fallimenti)", meta2)

# ---------------------------------------------------------------------------
# Resolve & sanity read
# ---------------------------------------------------------------------------

reader = catalog.resolve(DATASET_ID)
print(f"\nResolve → version {reader.version}")
print(reader.read().to_string(index=False))
