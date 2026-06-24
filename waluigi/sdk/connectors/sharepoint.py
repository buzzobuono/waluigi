import hashlib
import io
import os

import httpx
import pandas as pd

from waluigi.catalog.api.schemas import DatasetFormat
from waluigi.catalog.utils import _infer_schema_from_df
from .base import BaseConnector

_GRAPH = "https://graph.microsoft.com/v1.0"
_LOGIN = "https://login.microsoftonline.com"
_MAX_SIMPLE = 4 * 1024 * 1024   # 4 MB


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
    path = p.path.rstrip("/")
    r = client.get(f"{_GRAPH}/sites/{p.netloc}:{path}")
    r.raise_for_status()
    return r.json()["id"]


def _resolve_drive_id(client: httpx.Client, site_id: str) -> str:
    r = client.get(f"{_GRAPH}/sites/{site_id}/drive")
    r.raise_for_status()
    return r.json()["id"]


def _download(client: httpx.Client, drive_base: str, remote_path: str) -> bytes:
    r = client.get(f"{drive_base}/root:{remote_path}:/content", follow_redirects=True)
    r.raise_for_status()
    return r.content


def _upload(client: httpx.Client, drive_base: str, remote_path: str, content: bytes) -> None:
    if len(content) <= _MAX_SIMPLE:
        r = client.put(
            f"{drive_base}/root:{remote_path}:/content",
            content=content,
            headers={"Content-Type": "application/octet-stream"},
        )
        r.raise_for_status()
        return

    r = client.post(
        f"{drive_base}/root:{remote_path}:/createUploadSession",
        json={"item": {"@microsoft.graph.conflictBehavior": "replace"}},
    )
    r.raise_for_status()
    upload_url = r.json()["uploadUrl"]

    chunk_size = 10 * 1024 * 1024
    total = len(content)
    offset = 0
    while offset < total:
        chunk = content[offset: offset + chunk_size]
        end = offset + len(chunk) - 1
        resp = httpx.put(
            upload_url,
            content=chunk,
            headers={
                "Content-Range":  f"bytes {offset}-{end}/{total}",
                "Content-Length": str(len(chunk)),
            },
            timeout=60,
        )
        resp.raise_for_status()
        offset += len(chunk)


def _delete_item(client: httpx.Client, drive_base: str, remote_path: str) -> None:
    r = client.delete(f"{drive_base}/root:{remote_path}:")
    if r.status_code != 404:
        r.raise_for_status()


def _to_bytes(df: pd.DataFrame, fmt: DatasetFormat) -> bytes:
    if fmt == DatasetFormat.PARQUET:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        return buf.getvalue()
    if fmt == DatasetFormat.CSV:
        return df.to_csv(index=False).encode("utf-8-sig")
    raise NotImplementedError(f"SharePointConnector: unsupported format {fmt}")


def _from_bytes(content: bytes, fmt: DatasetFormat) -> pd.DataFrame:
    if fmt == DatasetFormat.PARQUET:
        return pd.read_parquet(io.BytesIO(content))
    if fmt == DatasetFormat.CSV:
        return pd.read_csv(io.BytesIO(content))
    raise NotImplementedError(f"SharePointConnector: unsupported format {fmt}")


class SharePointConnector(BaseConnector):
    """Store Catalog datasets in a SharePoint document library via Microsoft Graph API.

    Source config keys:
        tenant_id     Azure AD tenant GUID or domain
        client_id     App registration client ID
        client_secret Azure AD client secret (supports ${WALUIGI_SECRET_*} expansion)
        site_url      SharePoint site URL (e.g. https://contoso.sharepoint.com/sites/DataTeam)
        site_id       SharePoint site ID (alternative to site_url)
        drive_id      Document library ID (optional — defaults to root drive)
        folder        Base folder inside the library (optional)

    Dataset location: relative path within the library folder, set automatically
    by resolve_location() as "<dataset_id>/<version>.<ext>".
    """

    def __init__(self, config):
        super().__init__(config)
        self._tenant_id     = config["tenant_id"]
        self._client_id     = config["client_id"]
        self._client_secret = config["client_secret"]
        self._site_url      = config.get("site_url")
        self._site_id       = config.get("site_id")
        self._drive_id      = config.get("drive_id")
        self._base_folder   = (config.get("folder") or "").strip("/")

    def _client(self) -> httpx.Client:
        token = _token(self._tenant_id, self._client_id, self._client_secret)
        return httpx.Client(headers={"Authorization": f"Bearer {token}"}, timeout=30)

    def _drive_base(self, client: httpx.Client) -> str:
        site_id  = self._site_id  or _resolve_site_id(client, self._site_url)
        drive_id = self._drive_id or _resolve_drive_id(client, site_id)
        return f"{_GRAPH}/sites/{site_id}/drives/{drive_id}"

    def _remote_path(self, location: str) -> str:
        """Build the full remote path: /base_folder/location."""
        if self._base_folder:
            return f"/{self._base_folder}/{location}"
        return f"/{location}"

    def resolve_location(self, dataset_id: str, version: str, format: str, data_path: str) -> str:
        safe_ver = version.replace(":", "-")
        ext = f".{format}" if format else ""
        return f"{dataset_id}/{safe_ver}{ext}"

    def exists(self, location: str) -> bool:
        with self._client() as client:
            drive_base = self._drive_base(client)
            remote = self._remote_path(location)
            r = client.get(f"{drive_base}/root:{remote}:")
            return r.status_code == 200

    def checksum(self, location: str) -> str:
        with self._client() as client:
            drive_base = self._drive_base(client)
            content = _download(client, drive_base, self._remote_path(location))
        return hashlib.sha256(content).hexdigest()

    def write(self, location: str, format: DatasetFormat, data) -> int:
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        content = _to_bytes(df, format)
        with self._client() as client:
            drive_base = self._drive_base(client)
            _upload(client, drive_base, self._remote_path(location), content)
        return len(df)

    def read(self, location: str, format: DatasetFormat,
             limit: int = None, offset: int = 0) -> pd.DataFrame:
        with self._client() as client:
            drive_base = self._drive_base(client)
            content = _download(client, drive_base, self._remote_path(location))
        df = _from_bytes(content, format)
        if limit is not None:
            df = df.iloc[offset: offset + limit]
        return df

    def delete(self, location: str) -> None:
        with self._client() as client:
            drive_base = self._drive_base(client)
            _delete_item(client, drive_base, self._remote_path(location))

    def infer_schema(self, location: str) -> list[dict]:
        fmt_str = os.path.splitext(location)[1].lstrip(".").lower()
        fmt_map = {"csv": DatasetFormat.CSV, "parquet": DatasetFormat.PARQUET}
        fmt = fmt_map.get(fmt_str)
        if fmt is None:
            return []
        try:
            df = self.read(location, fmt)
            return _infer_schema_from_df(df)
        except Exception:
            return []
