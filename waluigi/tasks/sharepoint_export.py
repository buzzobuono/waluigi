"""
SharePointExport — publish a Catalog dataset to a SharePoint document library.

Reads the input dataset from the Waluigi Catalog and uploads it as a CSV or
Parquet file to a SharePoint folder via the Microsoft Graph API (app-only auth).

Azure AD app registration prerequisites
---------------------------------------
1. Register an app at https://portal.azure.com → Azure Active Directory → App registrations
2. Add an Application permission: Sites.ReadWrite.All  (NOT delegated)
3. Click "Grant admin consent"
4. Create a client secret and store it as a Waluigi Secret, e.g.:
       kind: Secret
       metadata: {namespace: analytics, name: sharepoint}
       spec:
         CLIENT_SECRET: "<your-secret-value>"

config:
  input:
    dataset:  str                # Catalog dataset id (e.g. gold/kpi_revenue)
    source:   {id, type, ...}    # as usual for built-in tasks
  sharepoint:
    tenant_id:  str   # Azure AD tenant GUID or "contoso.onmicrosoft.com"
    client_id:  str   # App registration Application (client) ID
    site_id:    str   # SharePoint site ID (from Graph Explorer — see note below)
                      # If omitted, provide site_url instead
    site_url:   str   # e.g. "https://contoso.sharepoint.com/sites/DataTeam"
                      # Used to auto-resolve site_id when site_id is absent
    drive_id:   str   # Document library ID (optional — defaults to the root drive)
    folder:     str   # Destination folder path inside the library, e.g. "PowerBI/Gold"
    filename:   str   # Filename override (default: last segment of dataset id + format ext)
    format:     str   # csv (default) | parquet

  The client secret is read from WALUIGI_SECRET_CLIENT_SECRET.
  Reference it in config with ${WALUIGI_SECRET_CLIENT_SECRET}.
  (Key name must match the key in your Secret descriptor.)

Job example
-----------
- id: publish_revenue
  taskRef:
    name: SharePointExport
  requires: [gold_revenue]
  secrets: [sharepoint]
  config:
    input:
      dataset: gold/kpi_revenue
      source:
        id: catalog_local
        type: LOCAL
    sharepoint:
      tenant_id:  "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
      client_id:  "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
      site_url:   "https://contoso.sharepoint.com/sites/DataTeam"
      folder:     "PowerBI/Gold"
      format:     csv

How to find site_id
-------------------
  Open Graph Explorer (https://developer.microsoft.com/graph/graph-explorer) and call:
  GET https://graph.microsoft.com/v1.0/sites/{hostname}:{/relative-path}
  e.g. https://graph.microsoft.com/v1.0/sites/contoso.sharepoint.com:/sites/DataTeam
  The "id" field in the response is your site_id.
"""
import io

import httpx
import pandas as pd

from waluigi.sdk.context import context
from waluigi.tasks._io import _to_dict, read_input

_GRAPH = "https://graph.microsoft.com/v1.0"
_LOGIN = "https://login.microsoftonline.com"
_MAX_SIMPLE = 4 * 1024 * 1024   # 4 MB — above this we use an upload session


def _token(tenant_id: str, client_id: str, client_secret: str) -> str:
    r = httpx.post(
        f"{_LOGIN}/{tenant_id}/oauth2/v2.0/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
            "scope":         "https://graph.microsoft.com/.default",
        },
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _resolve_site_id(client: httpx.Client, site_url: str) -> str:
    from urllib.parse import urlparse
    p = urlparse(site_url)
    hostname = p.netloc
    path     = p.path.rstrip("/")
    r = client.get(f"{_GRAPH}/sites/{hostname}:{path}")
    r.raise_for_status()
    return r.json()["id"]


def _resolve_drive_id(client: httpx.Client, site_id: str) -> str:
    r = client.get(f"{_GRAPH}/sites/{site_id}/drive")
    r.raise_for_status()
    return r.json()["id"]


def _upload(client: httpx.Client, drive_base: str, remote_path: str, content: bytes) -> dict:
    if len(content) <= _MAX_SIMPLE:
        r = client.put(
            f"{drive_base}/root:{remote_path}:/content",
            content=content,
            headers={"Content-Type": "application/octet-stream"},
        )
        r.raise_for_status()
        return r.json()

    # Large file: upload session (10 MB chunks)
    r = client.post(
        f"{drive_base}/root:{remote_path}:/createUploadSession",
        json={"item": {"@microsoft.graph.conflictBehavior": "replace"}},
    )
    r.raise_for_status()
    upload_url = r.json()["uploadUrl"]

    chunk_size = 10 * 1024 * 1024
    total  = len(content)
    offset = 0
    result = {}
    while offset < total:
        chunk = content[offset: offset + chunk_size]
        end   = offset + len(chunk) - 1
        resp  = httpx.put(
            upload_url,
            content=chunk,
            headers={
                "Content-Range":  f"bytes {offset}-{end}/{total}",
                "Content-Length": str(len(chunk)),
            },
            timeout=60,
        )
        if resp.status_code in (200, 201):
            result = resp.json()
        offset += len(chunk)
        print(f"  uploaded {min(offset, total):,}/{total:,} bytes")
    return result


def _serialize(df: pd.DataFrame, fmt: str) -> bytes:
    if fmt == "parquet":
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        return buf.getvalue()
    # default: csv
    return df.to_csv(index=False).encode("utf-8-sig")   # utf-8-sig = BOM for Excel/PowerBI


def run():
    cfg = _to_dict(context.config.sharepoint)

    tenant_id     = cfg.get("tenant_id")
    client_id     = cfg.get("client_id")
    client_secret = context.secrets.get("client_secret") or context.secrets.get("CLIENT_SECRET")
    if not all([tenant_id, client_id, client_secret]):
        raise ValueError(
            "sharepoint.tenant_id, sharepoint.client_id and secret client_secret are required"
        )

    fmt    = (cfg.get("format") or "csv").lower()
    folder = (cfg.get("folder") or "").strip("/")

    reader, df = read_input()

    default_name = reader.dataset_id.split("/")[-1] + ("." + fmt)
    filename     = cfg.get("filename") or default_name
    remote_path  = f"/{folder}/{filename}" if folder else f"/{filename}"

    token  = _token(tenant_id, client_id, client_secret)
    bearer = {"Authorization": f"Bearer {token}"}

    with httpx.Client(headers=bearer, timeout=30) as client:
        site_id = cfg.get("site_id") or _resolve_site_id(client, cfg["site_url"])
        drive_id = cfg.get("drive_id") or _resolve_drive_id(client, site_id)
        drive_base = f"{_GRAPH}/sites/{site_id}/drives/{drive_id}"

        content = _serialize(df, fmt)
        print(f"  uploading {len(content):,} bytes → {remote_path}")

        result = _upload(client, drive_base, remote_path, content)

    web_url = result.get("webUrl", "(no URL returned)")
    print(f"  published: {web_url}")
    print(f"Done: {len(df)} rows exported to SharePoint as {filename}")


if __name__ == "__main__":
    run()
