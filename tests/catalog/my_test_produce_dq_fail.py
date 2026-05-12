from waluigi.sdk.catalog import catalog
from waluigi.catalog.api.schemas import *

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

SOURCE_ID  = "local_dq"
DATASET_ID = "sales/quality/sales_dq_fail"

catalog.create_source(SourceCreateRequest(
    id=SOURCE_ID,
    type=SourceType.LOCAL,
    description="Local source for DQ test",
))

handle = catalog.create_dataset(
    DATASET_ID,
    format="csv",
    source_id=SOURCE_ID,
    description="Sales dataset with DQ monitoring",
)
handle.set_expectations(EXPECTATIONS)

rows_dirty = [
    {"product": "PROD_0001", "quantity":  10, "revenue": 100.0, "category": "A"},
    {"product": "PROD_0001", "quantity":  25, "revenue": 250.0, "category": "B"},  # duplicate
    {"product": "PROD_0003", "quantity": 150, "revenue": 500.0, "category": "C"},  # qty out of range
    {"product": "PROD_0004", "quantity":   5, "revenue":  None, "category": "X"},  # null revenue + bad category
]

with handle.create_version(metadata={"quality": "fail"}) as writer:
    writer.write(rows_dirty)
    ver2 = writer.version
