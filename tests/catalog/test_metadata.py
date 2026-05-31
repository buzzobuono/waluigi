import pytest
import uuid
from waluigi.sdk.catalog import CatalogError
from waluigi.catalog.api.schemas import SourceCreateRequest, SourceType

SOURCE_ID = "test_metadata_local"

@pytest.fixture(scope="module", autouse=True)
def ensure_source(catalog):
    try:
        catalog.create_source(SourceCreateRequest(
            id=SOURCE_ID, type=SourceType.LOCAL,
            config={}, description="source for metadata tests"))
    except Exception:
        pass
    yield
    try:
        catalog.delete_source(SOURCE_ID)
    except Exception:
        pass

@pytest.fixture
def dataset_id(catalog):
    uid = str(uuid.uuid4())[:8]
    id_ = f"unit/meta_{uid}"
    yield id_
    try:
        catalog._delete(catalog._ns_url(f"/datasets/{id_}"))
    except Exception:
        pass

@pytest.fixture
def sample_data():
    return [{"id": 1, "data": "test_metadata"}]

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_latest_version_str(catalog, dataset_id: str) -> str:
    """Recupera la stringa 'version' dell'ultima versione disponibile."""
    versions = catalog.list_versions(dataset_id)
    if not versions:
        pytest.fail(f"No versions found for dataset {dataset_id}")
    # Come visto nei test di versione, la chiave corretta è 'version'
    return versions[0]["version"]

# ── Metadata Management Tests ─────────────────────────────────────────────────

def test_metadata_crud_flow(catalog, dataset_id, sample_data):
    handle = catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    
    with handle.create_version(metadata={"ref": "v1.0"}, force=True) as ctx:
        ctx.write(sample_data)
    
    v_str = get_latest_version_str(catalog, dataset_id)
    base_url = catalog._ns_url(f"/datasets/{dataset_id}/versions/{v_str}/metadata")

    # POST
    catalog._post(base_url, json={"key": "team", "value": "data-engineers"})

    # GET
    metadata = catalog._get(base_url)
    assert metadata["team"] == "data-engineers"
    # I metadati passati durante la create_version finiscono in sys.* o simili
    assert any("ref" in k and v == "v1.0" for k, v in metadata.items())

    # DELETE
    catalog._delete(f"{base_url}/team")

    # Verify deletion
    metadata_after = catalog._get(base_url)
    assert "team" not in metadata_after


def test_metadata_version_not_found(catalog, dataset_id):
    catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    with pytest.raises(CatalogError) as exc:
        catalog._get(catalog._ns_url(f"/datasets/{dataset_id}/versions/fake-version-2026/metadata"))
    assert "404" in str(exc.value)


def test_set_metadata_reserved_sys_key(catalog, dataset_id, sample_data):
    handle = catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    with handle.create_version(force=True) as ctx:
        ctx.write(sample_data)

    v_str = get_latest_version_str(catalog, dataset_id)

    # Tentativo di usare sys.* (riservato)
    with pytest.raises(CatalogError) as exc:
        catalog._post(catalog._ns_url(f"/datasets/{dataset_id}/versions/{v_str}/metadata"),
                     json={"key": "sys.internal", "value": "illegal"})
    
    assert "reserved" in str(exc.value).lower()


def test_delete_protected_sys_metadata(catalog, dataset_id, sample_data):
    handle = catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    with handle.create_version(metadata={"ref": "audit-v1"}, force=True) as ctx:
        ctx.write(sample_data)

    v_str = get_latest_version_str(catalog, dataset_id)

    # Non si possono cancellare chiavi di sistema tramite questo servizio
    with pytest.raises(CatalogError):
        # Cerchiamo di cancellare la chiave sys che contiene il ref
        catalog._delete(catalog._ns_url(f"/datasets/{dataset_id}/versions/{v_str}/metadata/sys.ref"))


def test_metadata_isolation_between_versions(catalog, dataset_id, sample_data):
    handle = catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    
    # Versione A
    with handle.create_version(metadata={"tag": "alpha"}, force=True) as ctx:
        ctx.write(sample_data)
    v_alpha = get_latest_version_str(catalog, dataset_id)
    
    # Versione B
    with handle.create_version(metadata={"tag": "beta"}, force=True) as ctx:
        ctx.write(sample_data)
    v_beta = get_latest_version_str(catalog, dataset_id)
    
    assert v_alpha != v_beta

    meta_a = catalog._get(catalog._ns_url(f"/datasets/{dataset_id}/versions/{v_alpha}/metadata"))
    meta_b = catalog._get(catalog._ns_url(f"/datasets/{dataset_id}/versions/{v_beta}/metadata"))
    
    # Verifichiamo l'isolamento dei metadati di sistema creati al commit
    assert any("tag" in k and v == "alpha" for k, v in meta_a.items())
    assert any("tag" in k and v == "beta" for k, v in meta_b.items())

# ── Extended Metadata Tests ───────────────────────────────────────────────────

def test_metadata_update_existing_key(catalog, dataset_id, sample_data):
    """Testa l'aggiornamento di una chiave esistente (POST sovrascrive)."""
    handle = catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    with handle.create_version(force=True) as ctx:
        ctx.write(sample_data)
    
    v_str = get_latest_version_str(catalog, dataset_id)
    base_url = catalog._ns_url(f"/datasets/{dataset_id}/versions/{v_str}/metadata")

    # Primo set
    catalog._post(base_url, json={"key": "status", "value": "initial"})
    # Sovrascrittura
    catalog._post(base_url, json={"key": "status", "value": "updated"})

    metadata = catalog._get(base_url)
    assert metadata["status"] == "updated"


def test_metadata_special_characters_in_value(catalog, dataset_id, sample_data):
    """Testa la gestione di stringhe complesse (JSON, spazi, caratteri speciali)."""
    handle = catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    with handle.create_version(force=True) as ctx:
        ctx.write(sample_data)
    
    v_str = get_latest_version_str(catalog, dataset_id)
    complex_value = '{"project": "waluigi", "tags": ["test", "🚀"], "path": "C:\\\\temp"}'
    
    catalog._post(catalog._ns_url(f"/datasets/{dataset_id}/versions/{v_str}/metadata"),
                 json={"key": "config_json", "value": complex_value})

    metadata = catalog._get(catalog._ns_url(f"/datasets/{dataset_id}/versions/{v_str}/metadata"))
    assert metadata["config_json"] == complex_value


def test_delete_nonexistent_key(catalog, dataset_id, sample_data):
    """Verifica che la cancellazione di una chiave inesistente dia 404."""
    handle = catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    with handle.create_version(force=True) as ctx:
        ctx.write(sample_data)
    
    v_str = get_latest_version_str(catalog, dataset_id)
    
    with pytest.raises(CatalogError) as exc:
        catalog._delete(catalog._ns_url(f"/datasets/{dataset_id}/versions/{v_str}/metadata/ghost_key"))
    assert "404" in str(exc.value)


def test_set_metadata_on_deprecated_version(catalog, dataset_id, sample_data):
    """Verifica se è possibile operare sui metadati di una versione deprecata."""
    handle = catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    with handle.create_version(force=True) as ctx:
        ctx.write(sample_data)
    
    v_str = get_latest_version_str(catalog, dataset_id)
    
    # Depreca la versione
    catalog._delete(catalog._ns_url(f"/datasets/{dataset_id}/_deprecate/{v_str}"))

    # Il MetadataService dovrebbe comunque trovare la versione nel DB
    # (a meno di logiche di hard-delete nel service)
    catalog._post(catalog._ns_url(f"/datasets/{dataset_id}/versions/{v_str}/metadata"),
                 json={"key": "post_deprecate", "value": "true"})

    meta = catalog._get(catalog._ns_url(f"/datasets/{dataset_id}/versions/{v_str}/metadata"))
    assert meta["post_deprecate"] == "true"


def test_metadata_large_number_of_keys(catalog, dataset_id, sample_data):
    """Testa la tenuta con un numero elevato di metadati."""
    handle = catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    with handle.create_version(force=True) as ctx:
        ctx.write(sample_data)
    
    v_str = get_latest_version_str(catalog, dataset_id)
    base_url = catalog._ns_url(f"/datasets/{dataset_id}/versions/{v_str}/metadata")

    for i in range(50):
        catalog._post(base_url, json={"key": f"key_{i}", "value": f"val_{i}"})

    metadata = catalog._get(base_url)
    assert len(metadata) >= 50
    assert metadata["key_49"] == "val_49"


def test_metadata_error_status_codes(catalog, dataset_id, sample_data):
    """Verifica specificamente i codici HTTP ritornati dal router."""
    handle = catalog.create_dataset(dataset_id, format="parquet", source_id=SOURCE_ID)
    with handle.create_version(force=True) as ctx:
        ctx.write(sample_data)
    v_str = get_latest_version_str(catalog, dataset_id)

    # Caso 422: Chiave riservata (str(e) contiene "reserved")
    with pytest.raises(CatalogError) as exc:
        catalog._post(catalog._ns_url(f"/datasets/{dataset_id}/versions/{v_str}/metadata"),
                     json={"key": "sys.anything", "value": "fail"})
    # Se il tuo router mappa "reserved" -> 422
    assert "422" in str(exc.value)

    # Caso 404: Dataset o versione inesistente
    with pytest.raises(CatalogError) as exc:
        catalog._get(catalog._ns_url(f"/datasets/wrong_id/versions/{v_str}/metadata"))
    assert "404" in str(exc.value)
