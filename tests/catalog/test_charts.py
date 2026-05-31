"""
test_graph.py — end-to-end chart lifecycle: create dataset, write data,
define charts from YAML, verify all render endpoints return valid ECharts options.
"""
import pytest
import yaml
from pathlib import Path

from waluigi.sdk.catalog import CatalogError
from waluigi.catalog.api.schemas import SourceCreateRequest, SourceType

DATASET_ID   = "chart/sales_graph"
SOURCE_ID    = "local_graph_test"
CHARTS_YAML  = Path(__file__).parent / "sales_charts.yaml"

ROWS = [
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

@pytest.fixture(scope="module", autouse=True)
def setup_dataset(catalog):
    """Create source, dataset and write one version; clean up afterwards."""
    try:
        catalog.create_source(SourceCreateRequest(
            id=SOURCE_ID,
            type=SourceType.LOCAL,
            description="Local source for chart tests",
        ))
    except CatalogError:
        pass  # already exists from a previous run

    catalog.create_dataset(
        DATASET_ID,
        format="csv",
        source_id=SOURCE_ID,
        description="Sales data for chart visualisation tests",
    )

    handle = catalog.create_dataset(DATASET_ID, format="csv", source_id=SOURCE_ID)
    with handle.create_version(metadata={"period": "Q1-2026"}, force=True) as writer:
        writer.write(ROWS)

    yield

    try:
        catalog._delete(catalog._ns_url(f"/datasets/{DATASET_ID}"))
    except Exception:
        pass
    try:
        catalog.delete_source(SOURCE_ID)
    except Exception:
        pass


@pytest.fixture(scope="module")
def chart_defs():
    return yaml.safe_load(CHARTS_YAML.read_text())


@pytest.fixture(scope="module")
def charts(catalog, chart_defs):
    """Apply chart definitions from YAML (upsert by key) and return created charts."""
    handle = catalog.create_dataset(DATASET_ID, format="csv", source_id=SOURCE_ID)
    return [
        handle.set_chart(c["key"], c["title"], c["spec"])
        for c in chart_defs
    ]


# ── Data layer ────────────────────────────────────────────────────────────────

def test_dataset_has_version(catalog):
    versions = catalog.list_versions(DATASET_ID)
    assert len(versions) >= 1
    assert versions[0]["status"] == "committed"


def test_dataset_readable(catalog):
    reader = catalog.read_dataset(DATASET_ID)
    df = reader.read()
    assert len(df) == len(ROWS)
    assert set(df.columns) >= {"date", "category", "revenue", "units", "returns"}


# ── Chart CRUD ────────────────────────────────────────────────────────────────

def test_charts_created(charts, chart_defs):
    assert len(charts) == len(chart_defs)
    for chart in charts:
        assert "id" in chart
        assert "key" in chart
        assert "title" in chart


def test_list_charts(catalog, charts):
    listed = catalog.list_charts(DATASET_ID)
    listed_ids = {c["id"] for c in listed}
    for chart in charts:
        assert chart["id"] in listed_ids


def test_chart_upsert_is_idempotent(catalog, chart_defs):
    handle = catalog.create_dataset(DATASET_ID, format="csv", source_id=SOURCE_ID)
    first_run  = [handle.set_chart(c["key"], c["title"], c["spec"]) for c in chart_defs]
    second_run = [handle.set_chart(c["key"], c["title"], c["spec"]) for c in chart_defs]
    ids_first  = {c["id"] for c in first_run}
    ids_second = {c["id"] for c in second_run}
    assert ids_first == ids_second, "Re-applying charts should return the same IDs"


def test_chart_update(catalog, charts):
    chart = charts[0]
    new_title = "Updated Title"
    updated = catalog._patch(
        catalog._ns_url(f"/datasets/{DATASET_ID}/charts/{chart['id']}"),
        json={"title": new_title},
    )
    assert updated["title"] == new_title


def test_chart_delete(catalog):
    handle = catalog.create_dataset(DATASET_ID, format="csv", source_id=SOURCE_ID)
    tmp = handle.set_chart("_tmp_delete_me", "Temporary", {"type": "bar", "x": {"field": "date"}, "y": {"field": "revenue"}})
    chart_id = tmp["id"]
    catalog._delete(catalog._ns_url(f"/datasets/{DATASET_ID}/charts/{chart_id}"))
    listed = catalog.list_charts(DATASET_ID)
    assert all(c["id"] != chart_id for c in listed)


# ── Render ────────────────────────────────────────────────────────────────────

def test_render_by_id(catalog, charts):
    for chart in charts:
        result = catalog._get(catalog._ns_url(f"/datasets/{DATASET_ID}/charts/{chart['id']}/render"))
        assert "option" in result
        assert "version" in result
        assert "rows" in result
        assert result["rows"] == len(ROWS)


def test_render_by_key(catalog, chart_defs, charts):
    for cdef in chart_defs:
        result = catalog._get(
            catalog._ns_url(f"/datasets/{DATASET_ID}/charts/_render"),
            params={"key": cdef["key"]},
        )
        assert "option" in result
        option = result["option"]
        assert "series" in option
        assert len(option["series"]) > 0


def test_render_each_chart_type_has_data(catalog, charts):
    for chart in charts:
        result = catalog._get(catalog._ns_url(f"/datasets/{DATASET_ID}/charts/{chart['id']}/render"))
        option = result["option"]
        series = option.get("series", [])
        has_data = any(len(s.get("data", [])) > 0 for s in series)
        assert has_data, f"Chart '{chart['title']}' rendered no data"


def test_render_bar_structure(catalog, charts, chart_defs):
    bar_def = next((c for c in chart_defs if c["spec"].get("type") == "bar"), None)
    if bar_def is None:
        pytest.skip("No bar chart in YAML")
    bar_chart = next(c for c in charts if c["key"] == bar_def["key"])
    result = catalog._get(catalog._ns_url(f"/datasets/{DATASET_ID}/charts/{bar_chart['id']}/render"))
    option = result["option"]
    assert "xAxis" in option
    assert "yAxis" in option
    assert option["xAxis"]["type"] == "category"


def test_render_pie_structure(catalog, charts, chart_defs):
    pie_def = next((c for c in chart_defs if c["spec"].get("type") == "pie"), None)
    if pie_def is None:
        pytest.skip("No pie chart in YAML")
    pie_chart = next(c for c in charts if c["key"] == pie_def["key"])
    result = catalog._get(catalog._ns_url(f"/datasets/{DATASET_ID}/charts/{pie_chart['id']}/render"))
    option = result["option"]
    series = option["series"]
    assert series[0]["type"] == "pie"
    assert all("name" in p and "value" in p for p in series[0]["data"])


def test_render_radar_has_indicator(catalog, charts, chart_defs):
    radar_def = next((c for c in chart_defs if c["spec"].get("type") == "radar"), None)
    if radar_def is None:
        pytest.skip("No radar chart in YAML")
    radar_chart = next(c for c in charts if c["key"] == radar_def["key"])
    result = catalog._get(catalog._ns_url(f"/datasets/{DATASET_ID}/charts/{radar_chart['id']}/render"))
    option = result["option"]
    assert "radar" in option
    axes = radar_def["spec"]["axes"]
    assert len(option["radar"]["indicator"]) == len(axes)


def test_render_nonexistent_chart(catalog):
    with pytest.raises(CatalogError):
        catalog._get(catalog._ns_url(f"/datasets/{DATASET_ID}/charts/999999/render"))


def test_render_by_nonexistent_key(catalog):
    with pytest.raises(CatalogError):
        catalog._get(
            catalog._ns_url(f"/datasets/{DATASET_ID}/charts/_render"),
            params={"key": "_no_such_chart"},
        )
