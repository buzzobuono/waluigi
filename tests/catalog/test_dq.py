import pytest
import uuid
from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.api.schemas import DatasetStatus, SourceCreateRequest, SourceType

SOURCE_ID = "test_dq_local"

@pytest.fixture(scope="module", autouse=True)
def ensure_source():
    try:
        catalog.create_source(SourceCreateRequest(
            id=SOURCE_ID, type=SourceType.LOCAL,
            config={}, description="source for dataset and dq tests"))
    except Exception:
        pass
    yield
    try:
        catalog.delete_source(SOURCE_ID)
    except Exception:
        pass

@pytest.fixture
def dataset_id():
    uid = str(uuid.uuid4())[:8]
    id_ = f"test/unit/ds_{uid}"
    yield id_
    try:
        catalog._delete(f"/datasets/{id_}")
    except Exception:
        pass

# ── Basic CRUD ────────────────────────────────────────────────────────────────

def test_list_datasets_returns_list():
    result = catalog.list_datasets()
    assert isinstance(result, list)

def test_create_and_get_dataset(dataset_id):
    handle = catalog.create_dataset(dataset_id, format="parquet",
                                    description="Dataset di test unitario")
    assert handle is not None

    dataset = catalog.get_dataset(dataset_id)
    assert dataset["id"] == dataset_id
    assert dataset["format"] == "parquet"
    assert dataset["status"] == DatasetStatus.DRAFT

def test_create_dataset_idempotent(dataset_id):
    catalog.create_dataset(dataset_id, format="csv", description="first")
    catalog.create_dataset(dataset_id, format="csv", description="second")
    dataset = catalog.get_dataset(dataset_id)
    assert dataset["id"] == dataset_id

def test_get_nonexistent_dataset():
    with pytest.raises(CatalogError):
        catalog.get_dataset("does/not/exist")

def test_delete_dataset(dataset_id):
    catalog.create_dataset(dataset_id, format="csv")
    catalog._delete(f"/datasets/{dataset_id}")
    with pytest.raises(CatalogError):
        catalog.get_dataset(dataset_id)

# ── DQ Expectations Management ────────────────────────────────────────────────

def test_add_and_list_expectations(dataset_id):
    catalog.create_dataset(dataset_id, format="parquet")
    
    catalog._post(f"/datasets/{dataset_id}/expectations", json={
        "rule_id": "expect_column_values_to_not_be_null",
        "inputs": {"column": "user_id"},
        "params": {},
        "tolerance": 1.0,
        "position": 0
    })

    expectations = catalog._get(f"/datasets/{dataset_id}/expectations")
    assert len(expectations) == 1
    assert expectations[0]["rule_id"] == "expect_column_values_to_not_be_null"

def test_update_expectation(dataset_id):
    catalog.create_dataset(dataset_id, format="parquet")
    exp = catalog._post(f"/datasets/{dataset_id}/expectations", json={
        "rule_id": "expect_column_values_to_be_between",
        "inputs": {"column": "age"},
        "params": {"min_value": 0, "max_value": 120},
        "tolerance": 0.80,
        "position": 1
    })
    
    catalog._patch(f"/datasets/{dataset_id}/expectations/{exp['id']}", json={
        "tolerance": 0.99
    })
    
    expectations = catalog._get(f"/datasets/{dataset_id}/expectations")
    updated = next(e for e in expectations if e["id"] == exp["id"])
    assert float(updated["tolerance"]) == 0.99

# ── DQ Execution (Flow) ───────────────────────────────────────────────────────

def test_dq_flow_on_commit(dataset_id, sample_data):
    # Warmup delle regole
    catalog._get("/dq/rules")

    handle = catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    
    catalog._post(f"/datasets/{dataset_id}/expectations", json={
        "rule_id": "expect_column_values_to_not_be_null",
        "inputs": {"x": "this.id"},
        "params": {},
        "tolerance": 1.0,
        "position": 0
    })

    with handle.create_version(metadata={"ref": "dq-test"}, force=True) as ctx:
        ctx.write(sample_data)
    
    results = catalog._get(f"/datasets/{dataset_id}/dq")
    assert len(results) > 0
    
    latest_res = results[0]
    if not latest_res.get("success"):
        pytest.fail(f"DQ failed. Details: {latest_res.get('details')}")
    
    # La regola ha avuto successo (passed 1/1 regola o score 100%)
    assert latest_res["success"] is True
    assert latest_res["score"] == 1.0
    # Se il sistema conta le regole passate invece dei singoli record:
    assert latest_res["passed"] >= 1 

# ── DQ Catalog ────────────────────────────────────────────────────────────────

def test_list_dq_rules():
    rules = catalog._get("/dq/rules")
    assert isinstance(rules, list)
    # Verifichiamo che una delle tue regole sia presente
    assert any(r["id"] == "expect_column_values_to_not_be_null" for r in rules)

# ── Ulteriori Test di Integrazione DQ ─────────────────────────────────────────

def test_get_dq_suite_enrichment(tmp_path):
    # Creiamo un file suite temporaneo
    suite_file = tmp_path / "my_suite.yaml"
    suite_file.write_text("- rule_id: expect_column_values_to_not_be_null\n  inputs: {x: col1}")
    
    # Testiamo l'arricchimento della suite
    res = catalog._get("/dq/suite", params={"path": str(suite_file)})
    assert isinstance(res, list)
    assert res[0]["found"] is True
    assert "formula" in res[0]
    assert res[0]["rule_id"] == "expect_column_values_to_not_be_null"


def test_list_dq_results_not_found():
    with pytest.raises(CatalogError):
        catalog._get("/datasets/non_existent_ds/dq")


def test_add_expectation_invalid_dataset():
    with pytest.raises(CatalogError):
        catalog._post("/datasets/ghost_ds/expectations", json={
            "rule_id": "expect_column_values_to_not_be_null",
            "inputs": {"x": "this.id"},
            "params": {}, "tolerance": 1.0, "position": 0
        })


def test_dq_result_error_handling(dataset_id):
    """Testa che se la DQ fallisce per errore tecnico, il risultato venga comunque salvato con l'errore."""
    # create_dataset restituisce già l'handle
    handle = catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    
    # Forza un'expectation con una colonna che non esiste per generare un errore nel run
    catalog._post(f"/datasets/{dataset_id}/expectations", json={
        "rule_id": "expect_column_values_to_not_be_null",
        "inputs": {"x": "this.non_existent_column"},
        "params": {}, "tolerance": 1.0, "position": 0
    })

    # Scriviamo dati validi (la DQ fallirà internamente cercando la colonna)
    with handle.create_version(metadata={"ref": "error-test"}, force=True) as ctx:
        ctx.write([{"id": 1}])
    
    results = catalog._get(f"/datasets/{dataset_id}/dq")
    assert len(results) > 0
    # In base al tuo DQService, in caso di Exception success diventa False
    assert results[0]["success"] is False
    assert results[0]["passed"] == 0


def test_list_rules_is_sorted():
    rules = catalog._get("/dq/rules")
    rule_ids = [r["id"] for r in rules]
    assert rule_ids == sorted(rule_ids), "Le regole DQ non sono restituite in ordine alfabetico"

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_data():
    return [{"id": i, "val": f"row_{i}"} for i in range(5)]
