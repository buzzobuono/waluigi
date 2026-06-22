"""
End-to-end tests for the incremental built-in tasks AccumulateDataset and
UpsertDataset. The task run() functions are driven directly: the shared
`context` singleton is populated with config/params, and `_io.catalog` is
pointed at the test Catalog client.
"""
import pytest

from waluigi.catalog.api.schemas import SourceCreateRequest, SourceType
from waluigi.sdk.context import context, _to_namespace
import waluigi.tasks._io as io_mod
from waluigi.tasks import (
    accumulate_dataset,
    accumulate_deduplicate_dataset,
    upsert_dataset,
)


SOURCE      = "incr_local"
BRONZE      = "bronze/incr/orders"
GOLD        = "gold/incr/orders"
BRONZE_DIM  = "bronze/incr/clienti"
GOLD_DIM    = "gold/incr/clienti"

ALL_DATASETS = [BRONZE, GOLD, BRONZE_DIM, GOLD_DIM]


@pytest.fixture(autouse=True)
def wire_catalog(catalog, monkeypatch):
    """Point the task I/O helpers at the test Catalog and ensure a clean slate."""
    monkeypatch.setattr(io_mod, "catalog", catalog)

    def _clean():
        for ds in ALL_DATASETS:
            try: catalog._delete(catalog._ns_url(f"/datasets/{ds}"))
            except Exception: pass
        try: catalog.delete_source(SOURCE)
        except Exception: pass

    _clean()
    catalog.create_source(SourceCreateRequest(
        id=SOURCE, type=SourceType.LOCAL, config={}, description="Local"))
    yield
    _clean()


# ── helpers ────────────────────────────────────────────────────────────────────

def _write_bronze(catalog, dataset, rows, metadata):
    handle = catalog.create_dataset(dataset, format="parquet", source_id=SOURCE)
    with handle.create_version(metadata=metadata) as w:
        w.write(rows)


def _set_context(input_ds, output_ds, params, **extra_cfg):
    cfg = {
        "input":  {"dataset": input_ds,  "source": {"id": SOURCE, "type": "local"}},
        "output": {"dataset": output_ds, "format": "parquet",
                   "source": {"id": SOURCE, "type": "local"}},
        **extra_cfg,
    }
    context.config = _to_namespace(cfg)
    context.params = _to_namespace(params)


def _read_gold(catalog, dataset):
    return catalog.read_dataset(dataset).read()


# ── AccumulateDataset ────────────────────────────────────────────────────────────

def test_accumulate_first_run(catalog):
    rows = [{"date": "2026-06-18", "order": 1}, {"date": "2026-06-18", "order": 2}]
    _write_bronze(catalog, BRONZE, rows, {"date": "2026-06-18"})

    _set_context(BRONZE, GOLD, {"date": "2026-06-18"})
    accumulate_dataset.run()

    gold = _read_gold(catalog, GOLD)
    assert len(gold) == 2
    assert len(catalog.list_versions(GOLD)) == 1


def test_accumulate_appends_second_day(catalog):
    _write_bronze(catalog, BRONZE,
                  [{"date": "2026-06-18", "order": 1}], {"date": "2026-06-18"})
    _set_context(BRONZE, GOLD, {"date": "2026-06-18"})
    accumulate_dataset.run()

    # Day 2: bronze gets a new version with tomorrow's rows.
    _write_bronze(catalog, BRONZE,
                  [{"date": "2026-06-19", "order": 2},
                   {"date": "2026-06-19", "order": 3}], {"date": "2026-06-19"})
    _set_context(BRONZE, GOLD, {"date": "2026-06-19"})
    accumulate_dataset.run()

    gold = _read_gold(catalog, GOLD)
    assert len(gold) == 3
    assert set(gold["date"].astype(str)) == {"2026-06-18", "2026-06-19"}
    assert len(catalog.list_versions(GOLD)) == 2


def test_accumulate_idempotent_same_day(catalog):
    rows = [{"date": "2026-06-18", "order": 1}, {"date": "2026-06-18", "order": 2}]
    _write_bronze(catalog, BRONZE, rows, {"date": "2026-06-18"})

    _set_context(BRONZE, GOLD, {"date": "2026-06-18"})
    accumulate_dataset.run()
    accumulate_dataset.run()   # same params → version-level dedup skips the write

    gold = _read_gold(catalog, GOLD)
    assert len(gold) == 2                          # no duplication
    assert len(catalog.list_versions(GOLD)) == 1   # no extra version


def test_accumulate_reprocess_same_day_no_duplicate_rows(catalog):
    """Force a same-day reprocess via a differing param so the version-level dedup
    does NOT short-circuit, and verify the row-level idempotency still holds."""
    _write_bronze(catalog, BRONZE,
                  [{"date": "2026-06-18", "order": 1}], {"date": "2026-06-18"})
    _set_context(BRONZE, GOLD, {"date": "2026-06-18", "run": "1"})
    accumulate_dataset.run()

    # Same business day, new bronze content, different metadata (run=2).
    _write_bronze(catalog, BRONZE,
                  [{"date": "2026-06-18", "order": 1},
                   {"date": "2026-06-18", "order": 9}], {"date": "2026-06-18", "v": "2"})
    _set_context(BRONZE, GOLD, {"date": "2026-06-18", "run": "2"})
    accumulate_dataset.run()

    gold = _read_gold(catalog, GOLD)
    # Old day-rows were dropped and replaced by the new bronze — not appended.
    assert len(gold) == 2
    assert sorted(gold["order"].tolist()) == [1, 9]


def test_accumulate_adds_missing_date_column(catalog):
    _write_bronze(catalog, BRONZE, [{"order": 1}, {"order": 2}], {"date": "2026-06-18"})
    _set_context(BRONZE, GOLD, {"date": "2026-06-18"})
    accumulate_dataset.run()

    gold = _read_gold(catalog, GOLD)
    assert "date" in gold.columns
    assert set(gold["date"].astype(str)) == {"2026-06-18"}


# ── AccumulateDeduplicateDataset ─────────────────────────────────────────────────

def test_accdedup_first_run(catalog):
    rows = [{"date": "2026-06-18", "id": 1, "state": "new"},
            {"date": "2026-06-18", "id": 2, "state": "new"}]
    _write_bronze(catalog, BRONZE, rows, {"date": "2026-06-18"})

    _set_context(BRONZE, GOLD, {"date": "2026-06-18"})
    accumulate_deduplicate_dataset.run()

    gold = _read_gold(catalog, GOLD)
    assert len(gold) == 2
    assert len(catalog.list_versions(GOLD)) == 1


def test_accdedup_drops_unchanged_keeps_oldest_date(catalog):
    # Day 1: id=1 new, id=2 new
    _write_bronze(catalog, BRONZE,
                  [{"date": "2026-06-18", "id": 1, "state": "new"},
                   {"date": "2026-06-18", "id": 2, "state": "new"}], {"date": "2026-06-18"})
    _set_context(BRONZE, GOLD, {"date": "2026-06-18"})
    accumulate_deduplicate_dataset.run()

    # Day 2: id=1 unchanged (state=new), id=2 changed to screening
    _write_bronze(catalog, BRONZE,
                  [{"date": "2026-06-19", "id": 1, "state": "new"},
                   {"date": "2026-06-19", "id": 2, "state": "screening"}], {"date": "2026-06-19"})
    _set_context(BRONZE, GOLD, {"date": "2026-06-19"})
    accumulate_deduplicate_dataset.run()

    gold = _read_gold(catalog, GOLD)
    # id=1/new keeps the first-seen date and is NOT duplicated; id=2 has two states.
    assert len(gold) == 3
    id1 = gold[(gold["id"] == 1) & (gold["state"] == "new")]
    assert id1["date"].astype(str).tolist() == ["2026-06-18"]   # oldest date kept
    id2_states = set(gold[gold["id"] == 2]["state"])
    assert id2_states == {"new", "screening"}
    assert len(catalog.list_versions(GOLD)) == 2


def test_accdedup_idempotent_same_day(catalog):
    rows = [{"date": "2026-06-18", "id": 1, "state": "new"}]
    _write_bronze(catalog, BRONZE, rows, {"date": "2026-06-18"})

    _set_context(BRONZE, GOLD, {"date": "2026-06-18"})
    accumulate_deduplicate_dataset.run()
    accumulate_deduplicate_dataset.run()   # repeat absorbed by dedup + version skip

    gold = _read_gold(catalog, GOLD)
    assert len(gold) == 1
    assert len(catalog.list_versions(GOLD)) == 1


def test_accdedup_adds_missing_date_column(catalog):
    _write_bronze(catalog, BRONZE,
                  [{"id": 1, "state": "new"}], {"date": "2026-06-18"})
    _set_context(BRONZE, GOLD, {"date": "2026-06-18"})
    accumulate_deduplicate_dataset.run()

    gold = _read_gold(catalog, GOLD)
    assert "date" in gold.columns
    assert set(gold["date"].astype(str)) == {"2026-06-18"}


# ── UpsertDataset ────────────────────────────────────────────────────────────────

def test_upsert_first_run_dedups_input(catalog):
    rows = [{"id": 1, "name": "A"}, {"id": 1, "name": "A2"}, {"id": 2, "name": "B"}]
    _write_bronze(catalog, BRONZE_DIM, rows, {"date": "2026-06-18"})

    _set_context(BRONZE_DIM, GOLD_DIM, {"date": "2026-06-18"}, key="id")
    upsert_dataset.run()

    gold = _read_gold(catalog, GOLD_DIM).sort_values("id")
    assert gold["id"].tolist() == [1, 2]
    assert gold[gold["id"] == 1]["name"].iloc[0] == "A2"   # keep="last"


def test_upsert_updates_and_adds(catalog):
    _write_bronze(catalog, BRONZE_DIM,
                  [{"id": 1, "name": "A"}], {"date": "2026-06-18"})
    _set_context(BRONZE_DIM, GOLD_DIM, {"date": "2026-06-18"}, key="id")
    upsert_dataset.run()

    _write_bronze(catalog, BRONZE_DIM,
                  [{"id": 1, "name": "A-updated"}, {"id": 2, "name": "B"}],
                  {"date": "2026-06-19"})
    _set_context(BRONZE_DIM, GOLD_DIM, {"date": "2026-06-19"}, key="id")
    upsert_dataset.run()

    gold = _read_gold(catalog, GOLD_DIM).set_index("id")
    assert gold.loc[1, "name"] == "A-updated"      # existing record updated
    assert gold.loc[2, "name"] == "B"              # new record added
    assert len(gold) == 2
    assert len(catalog.list_versions(GOLD_DIM)) == 2


def test_upsert_keeps_records_removed_from_source(catalog):
    _write_bronze(catalog, BRONZE_DIM,
                  [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}], {"date": "2026-06-18"})
    _set_context(BRONZE_DIM, GOLD_DIM, {"date": "2026-06-18"}, key="id")
    upsert_dataset.run()

    # Day 2 source no longer contains id=2.
    _write_bronze(catalog, BRONZE_DIM,
                  [{"id": 1, "name": "A2"}], {"date": "2026-06-19"})
    _set_context(BRONZE_DIM, GOLD_DIM, {"date": "2026-06-19"}, key="id")
    upsert_dataset.run()

    gold = _read_gold(catalog, GOLD_DIM).set_index("id")
    assert set(gold.index) == {1, 2}              # id=2 retained
    assert gold.loc[1, "name"] == "A2"


def test_upsert_composite_key(catalog):
    _write_bronze(catalog, BRONZE_DIM, [
        {"region": "EU", "id": 1, "v": "old"},
        {"region": "US", "id": 1, "v": "x"},
    ], {"date": "2026-06-18"})
    _set_context(BRONZE_DIM, GOLD_DIM, {"date": "2026-06-18"}, key=["region", "id"])
    upsert_dataset.run()

    _write_bronze(catalog, BRONZE_DIM,
                  [{"region": "EU", "id": 1, "v": "new"}], {"date": "2026-06-19"})
    _set_context(BRONZE_DIM, GOLD_DIM, {"date": "2026-06-19"}, key=["region", "id"])
    upsert_dataset.run()

    gold = _read_gold(catalog, GOLD_DIM)
    assert len(gold) == 2
    eu = gold[(gold["region"] == "EU") & (gold["id"] == 1)]
    assert eu["v"].iloc[0] == "new"


def test_upsert_missing_key_raises(catalog):
    _write_bronze(catalog, BRONZE_DIM, [{"id": 1, "name": "A"}], {"date": "2026-06-18"})
    _set_context(BRONZE_DIM, GOLD_DIM, {"date": "2026-06-18"}, key="not_a_column")
    with pytest.raises(KeyError):
        upsert_dataset.run()
