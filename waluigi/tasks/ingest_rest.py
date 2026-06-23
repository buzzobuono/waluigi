"""
IngestRest — fetch a paginated JSON REST API and store as a Catalog dataset.

config:
    output:
        dataset:     str        Catalog dataset path (e.g. bronze/api/users)
        source_id:   str        Pre-registered source ID
        format:      str        parquet (default) | csv | json
        description: str
    http:
        url:         str        Required — endpoint URL
        method:      str        GET (default) | POST
        headers:     dict       HTTP headers — ${VAR} placeholders are expanded
        params:      dict       Query-string params — ${VAR} expanded
        body:        dict       Request body for POST (optional)
        data_key:    str        Key in JSON body containing the list (auto-detect if omitted)
        next_key:    str        Key for next-page URL in response (default: "next")
        page_param:  str        Query param name for page-number pagination (optional)
        page_size:   int        Value for page_size query param (optional)
"""
import httpx
import pandas as pd

from waluigi.sdk.context import context
from waluigi.tasks._io import _to_dict, write_output


# ── JSON traversal helpers ────────────────────────────────────────────────────

def _extract_items(body, data_key: str | None) -> list:
    if isinstance(body, list):
        return body
    if not isinstance(body, dict):
        return []
    if data_key and data_key in body:
        val = body[data_key]
        return val if isinstance(val, list) else []
    for key in ("data", "results", "items", "records", "content", "entries", "rows"):
        if key in body and isinstance(body[key], list):
            return body[key]
    candidates = [v for v in body.values() if isinstance(v, list)]
    if len(candidates) == 1:
        return candidates[0]
    return []


def _next_url(body, next_key: str) -> str | None:
    if not isinstance(body, dict):
        return None
    val = body.get(next_key)
    return val if (val and isinstance(val, str)) else None


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


# ── HTTP fetch loop ───────────────────────────────────────────────────────────

def _fetch_all(http_cfg: dict) -> list[dict]:
    url        = http_cfg["url"]
    method     = http_cfg.get("method", "GET").upper()
    headers    = dict(http_cfg.get("headers") or {})
    params     = dict(http_cfg.get("params")  or {})
    body       = http_cfg.get("body")
    data_key   = http_cfg.get("data_key")
    next_key   = http_cfg.get("next_key",   "next")
    page_param = http_cfg.get("page_param")
    page_size  = http_cfg.get("page_size")

    req_headers = {
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
        **headers,
    }

    qp = dict(params)
    if page_size:
        qp["page_size"] = page_size

    records     = []
    current_url = url
    page        = 1

    with httpx.Client(timeout=30, follow_redirects=True) as client:
        while current_url:
            call_params = {**qp, page_param: page} if page_param and page > 1 else qp
            print(f"  {method} {current_url}  (page {page})")

            if method == "POST":
                r = client.post(current_url, params=call_params,
                                headers=req_headers, json=body)
            else:
                r = client.get(current_url, params=call_params,
                               headers=req_headers)

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
                    f"Tip: the endpoint may require auth — add headers under config.http.headers."
                )

            body_json = r.json()
            items     = _extract_items(body_json, data_key)
            if not items:
                break

            records.extend([_flatten(item) for item in items])

            nxt = _next_url(body_json, next_key)
            if nxt and nxt != current_url:
                current_url = nxt
                page += 1
            else:
                break

    return records


# ── entry point ───────────────────────────────────────────────────────────────

def run():
    http_cfg = _to_dict(context.config.http)
    if not http_cfg.get("url"):
        raise ValueError("config.http.url is required")

    print(f"Fetching: {http_cfg['url']}")
    records = _fetch_all(http_cfg)

    if not records:
        raise RuntimeError(f"No records returned from {http_cfg['url']}")

    df = pd.DataFrame(records)
    print(f"Fetched {len(df)} rows, {len(df.columns)} columns: {list(df.columns)}")

    lineage = [{"dataset_id": f"__external__/{http_cfg['url']}", "version": "latest"}]
    write_output(df, lineage)


if __name__ == "__main__":
    run()
