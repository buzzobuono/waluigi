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
DATASET_ID_CLEAN = "sales/quality/sales_dq_clean"
DATASET_ID_DIRTY = "sales/quality/sales_dq_dirty"

catalog.create_source(SourceCreateRequest(
    id=SOURCE_ID,
    type=SourceType.SQL,
    config={
        "url": f"sqlite:///test.db"
    },
    description="Local source for DQ test",
))

# ---------------------------------------------------------------------------
# Version 1: clean data — expect all 8 rules to pass
# ---------------------------------------------------------------------------

dataset_clean = DatasetCreateRequest(
    id=DATASET_ID_CLEAN,
    format=DatasetFormat.SQL,
    description="Sales dataset with DQ monitoring",
    source_id=SOURCE_ID,
)
catalog.create_dataset(dataset_clean)
#catalog.set_expectations(DATASET_ID_CLEAN, EXPECTATIONS)

rows_clean = [
    {"product": "PROD_0001", "quantity": 10, "revenue": 100.0, "category": "A"},
    {"product": "PROD_0002", "quantity": 25, "revenue": 250.0, "category": "B"},
    {"product": "PROD_0003", "quantity": 50, "revenue": 500.0, "category": "C"},
    {"product": "PROD_0004", "quantity":  5, "revenue":  50.0, "category": "D"},
    {"product": "PROD_0005", "quantity": 80, "revenue": 800.0, "category": "E"},
]

with catalog.produce(dataset_clean, {"quality": "clean"}) as writer:
    writer.write(rows_clean)
    
# ---------------------------------------------------------------------------
# Version 2: dirty data — several rules should fail:
#   - PROD_0001 duplicated       → unique rule fails
#   - quantity 150               → between rule fails (row score < 1)
#   - revenue = None             → not_null fails + mean could be affected
#   - category "X"               → in_set fails
# ---------------------------------------------------------------------------

dataset_dirty = DatasetCreateRequest(
    id=DATASET_ID_DIRTY,
    format=DatasetFormat.SQL,
    description="Sales dataset with DQ monitoring",
    source_id=SOURCE_ID,
)
catalog.create_dataset(dataset_dirty)
catalog.set_expectations(DATASET_ID_DIRTY, EXPECTATIONS)

rows_dirty = [
    {"product": "PROD_0001", "quantity":  10, "revenue": 100.0, "category": "A"},
    {"product": "PROD_0001", "quantity":  25, "revenue": 250.0, "category": "B"},  # duplicate
    {"product": "PROD_0003", "quantity": 150, "revenue": 500.0, "category": "C"},  # qty out of range
    {"product": "PROD_0004", "quantity":   5, "revenue":  None, "category": "X"},  # null revenue + bad category
]

with catalog.produce(dataset_dirty, {"quality": "dirty"}, force=True) as writer:
    writer.write(rows_dirty)
    
