import pytest
import uuid
from waluigi.sdk.catalog import CatalogError
from waluigi.catalog.api.schemas import SourceCreateRequest, SourceType

SOURCE_ID = "lineage_test_source"

@pytest.fixture(scope="module", autouse=True)
def setup_lineage_source(catalog):
    try:
        catalog.create_source(SourceCreateRequest(
            id=SOURCE_ID, type=SourceType.LOCAL, config={}, description="Source for lineage tests"))
    except Exception: pass
    yield
    try: catalog.delete_source(SOURCE_ID)
    except Exception: pass

@pytest.fixture
def ds_names(catalog):
    uid = str(uuid.uuid4())[:8]
    names = {
        "raw": f"lineage/raw_{uid}",
        "silver": f"lineage/silver_{uid}",
        "gold": f"lineage/gold_{uid}"
    }
    yield names
    for name in names.values():
        try: catalog._delete(catalog._ns_url(f"/datasets/{name}"))
        except Exception: pass

# ── Suite di Test ────────────────────────────────────────────────────────────

def test_lineage_full_chain(catalog, ds_names):
    """Test della catena completa: RAW -> SILVER -> GOLD."""
    # 1. Produzione RAW
    raw_h = catalog.create_dataset(ds_names["raw"], source_id=SOURCE_ID)
    with raw_h.create_version() as ctx:
        ctx.write([{"id": 1, "data": "raw"}])
        v_raw = ctx.version

    # 2. Produzione SILVER (consuma RAW)
    silver_h = catalog.create_dataset(ds_names["silver"], source_id=SOURCE_ID)
    with silver_h.create_version(inputs=[{"dataset_id": ds_names["raw"], "version": v_raw}]) as ctx:
        ctx.write([{"id": 1, "data": "silver"}])
        v_silver = ctx.version

    # 3. Produzione GOLD (consuma SILVER)
    gold_h = catalog.create_dataset(ds_names["gold"], source_id=SOURCE_ID)
    with gold_h.create_version(inputs=[{"dataset_id": ds_names["silver"], "version": v_silver}]) as ctx:
        ctx.write([{"id": 1, "data": "gold"}])
        v_gold = ctx.version

    # VERIFICA SILVER (Punto centrale della catena)
    lineage = catalog.get_lineage(ds_names["silver"], v_silver)
    
    assert lineage["dataset_id"].endswith(ds_names["silver"])
    assert lineage["version"] == v_silver

    # Check Upstream
    assert len(lineage["upstream"]) == 1
    assert lineage["upstream"][0]["dataset_id"].endswith(ds_names["raw"])
    assert lineage["upstream"][0]["version"] == v_raw

    # Check Downstream
    assert len(lineage["downstream"]) == 1
    assert lineage["downstream"][0]["dataset_id"].endswith(ds_names["gold"])
    assert lineage["downstream"][0]["version"] == v_gold


def test_lineage_version_isolation(catalog, ds_names):
    """Verifica che il lineage sia specifico per versione e non 'sporcato' da altre versioni."""
    raw_h = catalog.create_dataset(ds_names["raw"], source_id=SOURCE_ID)
    
    # Creiamo due versioni di RAW
    with raw_h.create_version() as ctx:
        ctx.write([{"v": 1}])
        v1 = ctx.version
    with raw_h.create_version(force=True) as ctx:
        ctx.write([{"v": 2}])
        v2 = ctx.version

    # SILVER usa solo la v1
    silver_h = catalog.create_dataset(ds_names["silver"], source_id=SOURCE_ID)
    with silver_h.create_version(inputs=[{"dataset_id": ds_names["raw"], "version": v1}]) as ctx:
        ctx.write([{"v": "silver"}])
        v_silver = ctx.version

    lineage = catalog.get_lineage(ds_names["silver"], v_silver)
    
    # Deve esserci solo v1 negli upstream
    assert len(lineage["upstream"]) == 1
    assert lineage["upstream"][0]["version"] == v1
    assert all(u["version"] != v2 for u in lineage["upstream"])


def test_lineage_multiple_inputs(catalog, ds_names):
    """Test per dataset che hanno più di un upstream."""
    # Produciamo due sorgenti diverse
    raw_a = ds_names["raw"] + "_a"
    raw_b = ds_names["raw"] + "_b"
    
    h_a = catalog.create_dataset(raw_a, source_id=SOURCE_ID)
    h_b = catalog.create_dataset(raw_b, source_id=SOURCE_ID)
    
    with h_a.create_version() as ctx:
        ctx.write([{"a": 1}])
        v_a = ctx.version
    with h_b.create_version() as ctx:
        ctx.write([{"b": 1}])
        v_b = ctx.version

    # SILVER consuma sia A che B
    silver_h = catalog.create_dataset(ds_names["silver"], source_id=SOURCE_ID)
    inputs = [
        {"dataset_id": raw_a, "version": v_a},
        {"dataset_id": raw_b, "version": v_b}
    ]
    with silver_h.create_version(inputs=inputs) as ctx:
        ctx.write([{"merged": True}])
        v_silver = ctx.version

    lineage = catalog.get_lineage(ds_names["silver"], v_silver)
    assert len(lineage["upstream"]) == 2
    
    upstream_ids = [u["dataset_id"] for u in lineage["upstream"]]
    assert any(uid.endswith(raw_a) for uid in upstream_ids)
    assert any(uid.endswith(raw_b) for uid in upstream_ids)


def test_lineage_mandatory_version_error(catalog):
    """Verifica che la mancanza della versione nel path causi errore (404 o 405)."""
    # L'SDK, se passiamo stringa vuota, genera un URL troncato che non matcha la route
    with pytest.raises(CatalogError) as exc:
        catalog.get_lineage("any/dataset", "")
    
    # Il server risponde 404 perché la route /{dataset_id}/lineage/{version} non è soddisfatta
    assert "404" in str(exc.value)


def test_lineage_non_existent_version(catalog, ds_names):
    """Verifica 404 se la versione è sintatticamente corretta nel path ma inesistente nel DB."""
    with pytest.raises(CatalogError) as exc:
        catalog.get_lineage(ds_names["raw"], "ghost_version_99")
    
    assert "404" in str(exc.value)
    assert "not found" in str(exc.value).lower()
