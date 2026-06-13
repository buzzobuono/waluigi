"""
Generic HTTP API → Catalog dataset task.

Calls a paginated JSON endpoint, flattens nested objects, and stores
the result as a Catalog dataset via the SDK.

Params (WALUIGI_PARAM_*):
  url           Full URL to call (e.g. https://api.example.com/v1/items)

Config (WALUIGI_CONFIG, from task config block in YAML):
  dataset_id    Catalog dataset path (e.g. api/users/raw)
  source_id     Catalog source ID for local storage
  format        Dataset format: parquet (default), csv, json
  headers       HTTP request headers dict (optional)
  params        Extra query params dict (optional)
  data_key      Key in response body that contains the list — auto-detected if omitted
  next_key      Key for next-page URL in response (default: "next")
  page_param    Query param name for page number pagination (optional)
  page_size     Value for page_size query param (optional)
  description   Dataset description (optional)
"""
import httpx
import pandas as pd
from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog
from waluigi.catalog.api.schemas import SourceCreateRequest, SourceType


# ── helpers ───────────────────────────────────────────────────────────────────

def _flatten(obj: dict, prefix: str = "", sep: str = "_") -> dict:
    out = {}
    for k, v in obj.items():
        key = f"{prefix}{sep}{k}" if prefix else k
        if isinstance(v, dict):
            out.update(_flatten(v, key, sep))
        elif isinstance(v, list):
            out[key] = (str(v) if (v and isinstance(v[0], dict))
                        else ", ".join(str(i) for i in v))
        else:
            out[key] = v
    return out


def _extract_items(body, data_key: str | None) -> list:
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        if data_key and data_key in body:
            val = body[data_key]
            return val if isinstance(val, list) else []
        for key in ("data", "results", "items", "records", "content", "entries", "rows"):
            if key in body and isinstance(body[key], list):
                return body[key]
        # single-list-value dict
        candidates = [v for v in body.values() if isinstance(v, list)]
        if len(candidates) == 1:
            return candidates[0]
    return []


def _next_url(body, next_key: str) -> str | None:
    if not isinstance(body, dict):
        return None
    val = body.get(next_key)
    if val and isinstance(val, str):
        return val
    return None


def _fetch_all(url: str, headers: dict, extra_params: dict,
               data_key: str | None, next_key: str,
               page_param: str | None, page_size: int | None) -> list[dict]:
    records = []
    current_url = url
    page = 1

    # Mimic a browser so bot-detection / consent gates let the request through
    req_headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
        **headers,   # caller-supplied headers override defaults
    }

    qp = dict(extra_params)
    if page_size:
        qp["page_size"] = page_size

    with httpx.Client(timeout=30, follow_redirects=True) as client:
        while current_url:
            call_params = {**qp, page_param: page} if page_param and page > 1 else qp
            print(f"  GET {current_url}  (page {page})")
            r = client.get(current_url, params=call_params, headers=req_headers)

            ct = r.headers.get("content-type", "")
            print(f"  → {r.status_code}  content-type: {ct}")

            if r.status_code != 200:
                print(f"  Response body: {r.text[:500]}")
                r.raise_for_status()

            if not r.content:
                raise RuntimeError(f"Empty response body from {current_url}")

            if "json" not in ct and "javascript" not in ct:
                preview = r.text[:300].replace("\n", " ")
                raise RuntimeError(
                    f"Response is not JSON (content-type: {ct!r}).\n"
                    f"Body preview: {preview}\n"
                    f"Tip: the endpoint may require auth cookies or custom headers — "
                    f"add them under config.headers in your YAML descriptor."
                )

            body  = r.json()
            items = _extract_items(body, data_key)
            if not items:
                break
            records.extend([_flatten(item) for item in items])
            nxt = _next_url(body, next_key)
            if nxt and nxt != current_url:
                current_url = nxt
                page += 1
            else:
                break

    return records


# ── main ──────────────────────────────────────────────────────────────────────

url        = context.params.url
cfg        = context.config

dataset_id  = cfg.dataset_id
source_id   = cfg.source_id
fmt         = getattr(cfg, "format",      "parquet")
description = getattr(cfg, "description", f"HTTP extract from {url}")
headers     = dict(getattr(cfg, "headers",     {}) or {})
extra_params= dict(getattr(cfg, "params",      {}) or {})
data_key    = getattr(cfg, "data_key",    None)
next_key    = getattr(cfg, "next_key",    "next")
page_param  = getattr(cfg, "page_param",  None)
page_size   = getattr(cfg, "page_size",   None)

print(f"Fetching: {url}")

records = _fetch_all(url, headers, extra_params, data_key, next_key,
                     page_param, page_size)

if not records:
    raise RuntimeError(f"No records returned from {url}")

df = pd.DataFrame(records)
print(f"Fetched {len(df)} rows, {len(df.columns)} columns: {list(df.columns)}")

# Ensure local source exists
catalog.create_source(SourceCreateRequest(
    id=source_id,
    type=SourceType.LOCAL,
    config={},
    description="Local storage for HTTP extracts",
))

handle = catalog.create_dataset(
    dataset_id,
    format=fmt,
    source_id=source_id,
    description=description,
)

with handle.create_version(metadata={"source_url": url}) as writer:
    writer.write(df)

if writer.skipped:
    print(f"Skipped — same data already committed: {writer.version}")
else:
    print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")
