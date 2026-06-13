import httpx
import pandas as pd
from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog
from waluigi.catalog.api.schemas import SourceCreateRequest, SourceType

# ── config ────────────────────────────────────────────────────────────────────

BASE_URL   = "https://jsonplaceholder.typicode.com"
ENDPOINT   = "/users"
SOURCE_ID  = "api-local"
DATASET_ID = "api/users/raw"
FORMAT     = "parquet"

# ── params ────────────────────────────────────────────────────────────────────

date = context.params.date

# ── fetch ─────────────────────────────────────────────────────────────────────

print(f"Fetching {BASE_URL}{ENDPOINT} for date={date}")

r = httpx.get(f"{BASE_URL}{ENDPOINT}", timeout=30)
r.raise_for_status()
raw = r.json()   # list[dict] with nested address, company

# ── flatten nested fields ─────────────────────────────────────────────────────

def _flatten(obj: dict, prefix: str = "", sep: str = "_") -> dict:
    out = {}
    for k, v in obj.items():
        key = f"{prefix}{sep}{k}" if prefix else k
        if isinstance(v, dict):
            out.update(_flatten(v, key, sep))
        elif isinstance(v, list):
            out[key] = ", ".join(str(i) for i in v)
        else:
            out[key] = v
    return out

records = [_flatten(item) for item in raw]
df = pd.DataFrame(records)
print(f"Fetched {len(df)} rows — columns: {list(df.columns)}")

# ── write to catalog ──────────────────────────────────────────────────────────

catalog.create_source(SourceCreateRequest(
    id=SOURCE_ID,
    type=SourceType.LOCAL,
    config={},
    description="Local storage for API extracts",
))

handle = catalog.create_dataset(
    DATASET_ID,
    format=FORMAT,
    source_id=SOURCE_ID,
    description="Users from external API",
)

with handle.create_version(metadata={"date": date}) as writer:
    writer.write(df)

if writer.skipped:
    print(f"Skipped — same data already committed: {writer.version}")
else:
    print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")
