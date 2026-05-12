from waluigi.sdk.catalog import catalog, CatalogError
from waluigi.catalog.api.schemas import *
import pandas as pd
import time
import os
# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SOURCE_ID  = "local_stream"
DATASET_ID = "sales/raw/sales_stream"
SUITE_DIR  = os.path.join(os.getcwd(), "rules", "suites")
SUITE_PATH = os.path.join(SUITE_DIR, "sales_dq_suite.yaml")

TOTAL_ROWS  = 100_000   # 1 milione di righe
CHUNK_SIZE  =    1_000   # 10k righe per chunk → 100 chiamate al connettore

# ---------------------------------------------------------------------------
# Source / Dataset (idempotente)
# ---------------------------------------------------------------------------

catalog.create_source(SourceCreateRequest(
    id=SOURCE_ID,
    type=SourceType.LOCAL,
    description="Source locale per test streaming",
))

handle = catalog.create_dataset(
    DATASET_ID,
    format="parquet",
    source_id=SOURCE_ID,
    description="Dataset grande scritto in streaming",
)

# ---------------------------------------------------------------------------
# Generator: produce chunk da CHUNK_SIZE righe alla volta
# ---------------------------------------------------------------------------

def generate_chunks(total: int, chunk_size: int):
    """Yield DataFrames senza mai tenere tutto in memoria."""
    produced = 0
    chunk_n  = 0
    while produced < total:
        size = min(chunk_size, total - produced)
        chunk_n += 1
        df = pd.DataFrame({
            "id":       range(produced, produced + size),
            "date":     "2026-01-01",
            "product":  [f"PROD_{i % 100:04d}" for i in range(produced, produced + size)],
            "quantity": [i % 50 + 1             for i in range(produced, produced + size)],
            "revenue":  [(i % 50 + 1) * 9.99    for i in range(produced, produced + size)],
            "category": [f"CAT_{i % 10}"        for i in range(produced, produced + size)],
        })
        print(f"  chunk {chunk_n:4d}  rows {produced:>9,} – {produced + size - 1:>9,}")
        yield df
        produced += size

# ---------------------------------------------------------------------------
# Produce
# ---------------------------------------------------------------------------

metadata = {
    "source":     "STRESS_TEST",
    "total_rows": str(TOTAL_ROWS),
    "chunk_size": str(CHUNK_SIZE),
}

print(f"Produco {TOTAL_ROWS:,} righe in chunk da {CHUNK_SIZE:,} ({TOTAL_ROWS // CHUNK_SIZE} chunk) ...")
t0 = time.perf_counter()

with handle.create_version(metadata=metadata) as writer:
    written = writer.write(generate_chunks(TOTAL_ROWS, CHUNK_SIZE))

elapsed = time.perf_counter() - t0
print(f"\nScritti {written:,} righe in {elapsed:.1f}s  ({written / elapsed:,.0f} righe/s)")

# ---------------------------------------------------------------------------
# Resolve e verifica
# ---------------------------------------------------------------------------

reader = catalog.read_dataset(DATASET_ID)
print(f"\nResolve:")
print(f"  dataset_id : {reader.dataset_id}")
print(f"  version    : {reader.version}")
print(f"  location   : {reader.location}")

df_check = reader.read(limit=5)
print(f"\nPrime 5 righe:")
print(df_check.to_string(index=False))

df_tail = reader.read(limit=5, offset=TOTAL_ROWS - 5)
print(f"\nUltime 5 righe:")
print(df_tail.to_string(index=False))
