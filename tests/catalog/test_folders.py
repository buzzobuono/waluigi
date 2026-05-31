import pytest
from waluigi.catalog.api.schemas import SourceCreateRequest, SourceType

SOURCE_ID = "folders_local"
PREFIX    = "folders"

# datasets planted for folder tests
DATASETS = [
    f"{PREFIX}/alpha",
    f"{PREFIX}/beta",
    f"{PREFIX}/sub/gamma",
    f"{PREFIX}/sub/delta",
    f"{PREFIX}/sub/deep/epsilon",
]


@pytest.fixture(scope="module", autouse=True)
def setup_folder_datasets(catalog):
    try:
        catalog.create_source(SourceCreateRequest(
            id=SOURCE_ID, type=SourceType.LOCAL,
            config={}, description="source for folder tests"))
    except Exception:
        pass

    for ds_id in DATASETS:
        try:
            catalog.create_dataset(ds_id, format="csv", source_id=SOURCE_ID,
                                   description=f"folder test: {ds_id}")
        except Exception:
            pass

    yield

    for ds_id in DATASETS:
        try: catalog._delete(catalog._ns_url(f"/datasets/{ds_id}"))
        except Exception: pass
    try: catalog.delete_source(SOURCE_ID)
    except Exception: pass


# ── Response shape ────────────────────────────────────────────────────────────

def test_root_returns_dict(catalog):
    result = catalog.list_folders("")
    assert isinstance(result, dict)
    assert "prefix"   in result
    assert "datasets" in result
    assert "prefixes" in result


def test_prefix_field_has_trailing_slash(catalog):
    result = catalog.list_folders(PREFIX)
    assert result["prefix"].endswith("/")


# ── Direct children ───────────────────────────────────────────────────────────

def test_direct_datasets_listed(catalog):
    result = catalog.list_folders(PREFIX)
    ids = [d["id"] for d in result["datasets"]]
    assert any(id_.endswith(f"{PREFIX}/alpha") for id_ in ids)
    assert any(id_.endswith(f"{PREFIX}/beta")  for id_ in ids)


def test_deep_datasets_not_in_direct_list(catalog):
    result = catalog.list_folders(PREFIX)
    ids = [d["id"] for d in result["datasets"]]
    assert not any(id_.endswith(f"{PREFIX}/sub/gamma")        for id_ in ids)
    assert not any(id_.endswith(f"{PREFIX}/sub/deep/epsilon") for id_ in ids)


# ── Sub-prefixes ──────────────────────────────────────────────────────────────

def test_sub_prefix_appears_as_prefix(catalog):
    result = catalog.list_folders(PREFIX)
    assert f"{PREFIX}/sub/" in result["prefixes"]


def test_sub_prefix_not_in_datasets(catalog):
    result = catalog.list_folders(PREFIX)
    ids = [d["id"] for d in result["datasets"]]
    assert f"{PREFIX}/sub/" not in ids


def test_drill_into_sub_prefix(catalog):
    result = catalog.list_folders(f"{PREFIX}/sub")
    ids = [d["id"] for d in result["datasets"]]
    assert any(id_.endswith(f"{PREFIX}/sub/gamma") for id_ in ids)
    assert any(id_.endswith(f"{PREFIX}/sub/delta") for id_ in ids)


def test_drill_into_sub_prefix_shows_deeper_prefix(catalog):
    result = catalog.list_folders(f"{PREFIX}/sub")
    assert f"{PREFIX}/sub/deep/" in result["prefixes"]


def test_deepest_level_has_no_further_prefixes(catalog):
    result = catalog.list_folders(f"{PREFIX}/sub/deep")
    ids = [d["id"] for d in result["datasets"]]
    assert any(id_.endswith(f"{PREFIX}/sub/deep/epsilon") for id_ in ids)
    assert result["prefixes"] == []


# ── Edge cases ────────────────────────────────────────────────────────────────

def test_nonexistent_prefix_returns_empty(catalog):
    result = catalog.list_folders("does/not/exist/anywhere")
    assert result["datasets"] == []
    assert result["prefixes"] == []


def test_dataset_fields_present(catalog):
    result = catalog.list_folders(PREFIX)
    assert len(result["datasets"]) >= 2
    for d in result["datasets"]:
        assert "id"     in d
        assert "format" in d
        assert "status" in d
