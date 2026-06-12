# Data Quality

Waluigi includes a rule-based data quality framework. Rules are YAML files; suites group rules into a test plan; `DQManager` evaluates suites against pandas DataFrames.

DQ results are stored per dataset version in the Catalog and visible in the Console.

---

## Rule format

Rules live in the `rules/` directory. Each rule is a YAML file with a formula evaluated against the input DataFrames.

```yaml
description: "Human-readable rule description"
formula: <expression>              # pandas/Python expression
inputs_schema:
  <name>: "Description"           # named inputs (column Series)
params_schema:
  <name>: "Description"           # optional rule parameters
```

The `formula` is a Python expression evaluated in a sandboxed context:
- `inputs_schema` keys are bound to the named input Series
- `params_schema` keys are bound to the provided parameter values
- The expression must evaluate to a boolean Series (row-level pass/fail)
- Formulas are validated through an AST whitelist before execution — arbitrary code cannot be injected

**Example rule (`expect_column_values_to_not_be_null.yaml`):**

```yaml
description: "Column values must not be null"
formula: "x.notnull()"
inputs_schema:
  x: "Column to check"
```

**Example with params (`expect_column_values_to_be_between.yaml`):**

```yaml
description: "Column values must be between min and max"
formula: "(x >= min_val) & (x <= max_val)"
inputs_schema:
  x: "Column to check"
params_schema:
  min_val: "Minimum value (inclusive)"
  max_val: "Maximum value (inclusive)"
```

---

## Suite format

A suite YAML file groups multiple rule applications with their input mappings:

```yaml
- rule_id: <string>             # filename without .yaml extension
  inputs:
    <input_name>: <reference>   # "this.<column>" → column from "this" dataset
  params:
    <param_name>: <value>       # rule-specific parameters
  tolerance: <float>            # fraction of rows that can fail  (default: 1.0)
                                # 1.0 = all rows must pass
                                # 0.95 = at most 5% of rows can fail
```

**Input references:**

| Reference | Meaning |
|-----------|---------|
| `this.<column>` | Column from the `this` dataset |
| `other.<column>` | Column from the `other` dataset (multi-dataset rules) |

**Example suite (`rules/suites/sales_dq_suite.yaml`):**

```yaml
- rule_id: expect_column_values_to_not_be_null
  inputs:
    x: this.order_id
  tolerance: 1.0

- rule_id: expect_column_values_to_not_be_null
  inputs:
    x: this.customer_id
  tolerance: 1.0

- rule_id: expect_column_values_to_be_between
  inputs:
    x: this.quantity
  params:
    min_val: 1
    max_val: 10000
  tolerance: 1.0

- rule_id: expect_column_values_to_be_of_type
  inputs:
    x: this.revenue
  params:
    target_type: "float"
  tolerance: 1.0

- rule_id: expect_column_values_to_be_unique
  inputs:
    x: this.order_id
  tolerance: 1.0
```

---

## DQManager

Evaluate a suite programmatically:

```python
from waluigi.sdk.dataquality import DQManager

dq = DQManager()

result = dq.run_suite(
    suite_path="/rules/suites/sales_dq_suite.yaml",
    datasets={"this": df_orders},
)

if not result.success:
    print(f"DQ FAILED: {result.failed}/{result.total} rules failed (score: {result.score:.1%})")
    for r in result.results:
        if not r.success:
            print(f"  ❌ {r.rule_id}: score={r.score:.1%}")
            if r.failed_indices:
                print(f"     Failing rows: {r.failed_indices[:10]}")
    raise ValueError("Dataset failed DQ checks")
else:
    print(f"DQ OK: {result.passed}/{result.total} rules passed")
```

Multi-dataset suite:

```python
result = dq.run_suite(
    suite_path="/rules/suites/referential_integrity.yaml",
    datasets={
        "this":  df_orders,
        "other": df_products,
    },
)
```

---

## Available built-in rules

| Rule ID | Description | Inputs | Params |
|---------|-------------|--------|--------|
| `expect_column_values_to_not_be_null` | Column has no nulls | `x` | — |
| `expect_column_values_to_be_unique` | Column has no duplicates | `x` | — |
| `expect_column_values_to_be_between` | Values in `[min, max]` | `x` | `min_val`, `max_val` |
| `expect_column_values_to_be_in_set` | Values are in an allowed set | `x` | `allowed_values` (list) |
| `expect_column_values_to_match_regex` | Values match a regex pattern | `x` | `pattern` |
| `expect_column_values_to_be_of_type` | Values are of the expected Python type | `x` | `target_type` (`int`, `float`, `str`, …) |
| `expect_column_value_lengths_to_be_between` | String length in `[min, max]` | `x` | `min_length`, `max_length` |
| `expect_column_mean_to_be_between` | Column mean in `[min, max]` | `x` | `min_val`, `max_val` |
| `expect_column_pair_values_a_to_be_greater_than_b` | `a > b` for each row | `a`, `b` | — |
| `expect_compound_columns_to_be_unique` | Combination of two columns is unique | `a`, `b` | — |
| `expect_column_values_to_be_in_another_dataset` | Values exist in another dataset's column | `x` | *(references `other`)* |
| `expect_cf_birthdate_coherence` | Italian tax code (codice fiscale) matches birth date | `cf`, `birthdate` | — |

---

## Attaching DQ to a dataset via the Catalog

### Via SDK

```python
catalog.add_expectation(
    dataset_id="sales/clean/orders",
    rule_id="expect_column_values_to_not_be_null",
    inputs={"x": "this.order_id"},
    tolerance=1.0,
)
```

### Via built-in task

```yaml
- id: set_expectations
  taskRef:
    name: CatalogSetExpectations
  config:
    dataset: sales/clean/orders
    expectations:
      - rule_id: expect_column_values_to_not_be_null
        inputs: {x: "this.order_id"}
        tolerance: 1.0
      - rule_id: expect_column_values_to_be_between
        inputs: {x: "this.quantity"}
        params: {min_val: 1, max_val: 10000}
        tolerance: 0.99
```

DQ results are computed automatically on each `_commit` if expectations are defined. Results are stored per version and visible in the Console.

---

## Writing custom rules

Add a YAML file to the `rules/` directory:

```yaml
# rules/expect_revenue_to_be_positive.yaml
description: "Revenue must be positive (greater than zero)"
formula: "x > 0"
inputs_schema:
  x: "Revenue column"
```

Custom rules are immediately available in suites and via `DQManager`.

More complex rules with multiple inputs:

```yaml
# rules/expect_discount_to_not_exceed_price.yaml
description: "Discount must not exceed unit price"
formula: "discount <= unit_price"
inputs_schema:
  discount:   "Discount amount"
  unit_price: "Unit price"
```

```yaml
# in suite:
- rule_id: expect_discount_to_not_exceed_price
  inputs:
    discount:   this.discount
    unit_price: this.unit_price
```

**Formula constraints (AST whitelist):**
- Allowed: comparison operators (`>`, `<`, `>=`, `<=`, `==`, `!=`), boolean operators (`&`, `|`, `~`), arithmetic (`+`, `-`, `*`, `/`), attribute access, method calls on Series (`notnull`, `isin`, `str.match`, etc.)
- Disallowed: `import`, `exec`, `eval`, `open`, `__builtins__`, subprocess calls, or any construct that could execute arbitrary code
