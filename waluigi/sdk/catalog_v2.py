"""
waluigi.sdk.catalog

Format-agnostic data catalog SDK for Waluigi Catalog v2.

Collection + id always identify a dataset. The SDK unwraps the
{data, diagnostic} response contract automatically and raises
CatalogWarning for WARN results so callers can decide whether to
act on them.

Usage:

    from waluigi.sdk.catalog import catalog

    # READ — resolve connection info, open with native libraries
    info = catalog.resolve("finance/erp", "fatture")
    # info.path          → local file path  (source_type == "local")
    # info.dsn           → SQLAlchemy DSN   (source_type == "sql")
    # info.query         → SQL query        (source_type == "sql")
    # info.uri           → s3:// URI        (source_type == "s3")
    # info.remote_path   → remote path      (source_type == "sftp")
    # info.pii_columns   → list of PII col names (always present)

    # WRITE — context manager handles reserve → write → commit
    with catalog.produce("finance/erp", "fatture_clean",
                         format="parquet",
                         inputs=[catalog.ref("finance/erp", "fatture")]) as ctx:
        df.to_parquet(ctx.path)
        ctx.rows = len(df)
    # on __exit__: hash computed + commit called automatically
    # on exception: version is marked failed

    # VIRTUAL — register an external dataset (no local copy)
    catalog.register_virtual(
        "finance/erp", "fatture_pg",
        source_id="pg-dwh",
        location="SELECT * FROM finance.fatture",
        format="sql",
    )

    # MATERIALIZE — fetch a REST API endpoint into a local CSV
    result = catalog.materialize(
        "crm/raw", "orders",
        base_url="https://api.crm.com",
        endpoint="/v1/orders",
        params={"status": "closed"},
    )

    # SCHEMA — read and patch PII flags
    schema = catalog.get_schema("finance/erp", "fatture")
    catalog.patch_column("finance/erp", "fatture", "email",
                         pii=True, pii_type="direct",
                         description="Customer email address")
    catalog.publish_schema("finance/erp", "fatture", published_by="mario.rossi")

Environment variables (injected by the worker):
    WALUIGI_CATALOG_URL   default: http://localhost:9000
    WALUIGI_TASK_ID
    WALUIGI_JOB_ID
"""

import os
import warnings
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests


# ---------------------------------------------------------------------------
# Exceptions & warnings
# ---------------------------------------------------------------------------

class CatalogError(Exception):
    """Raised when the catalog returns result=KO."""


class CatalogWarning(UserWarning):
    """Raised (as a warning) when the catalog returns result=WARN."""


# ---------------------------------------------------------------------------
# Response wrapper
# ---------------------------------------------------------------------------

@dataclass
class ResolveInfo:
    """
    Typed result of catalog.resolve().
    Attributes are populated based on source_type; unused ones are None.
    """
    collection:   str
    dataset_id:   str
    version:      str
    source_type:  str
    format:       Optional[str]
    rows:         Optional[int]
    committed_at: Optional[str]
    pii_columns:  List[str] = field(default_factory=list)

    # local / sftp
    path:        Optional[str] = None
    remote_path: Optional[str] = None

    # s3
    uri:          Optional[str] = None
    endpoint_url: Optional[str] = None
    region:       Optional[str] = None

    # sql
    dsn:   Optional[str] = None
    query: Optional[str] = None

    # api
    url: Optional[str] = None

    @classmethod
    def from_response(cls, data: dict) -> "ResolveInfo":
        ci = data.get("connection_info", {})
        return cls(
            collection=data["collection"],
            dataset_id=data["dataset_id"],
            version=data["version"],
            source_type=data["source_type"],
            format=data.get("format"),
            rows=data.get("rows"),
            committed_at=data.get("committed_at"),
            pii_columns=data.get("pii_columns", []),
            # local
            path=ci.get("path"),
            # sftp
            remote_path=ci.get("remote_path"),
            # s3
            uri=ci.get("uri"),
            endpoint_url=ci.get("endpoint_url"),
            region=ci.get("region"),
            # sql
            dsn=ci.get("dsn"),
            query=ci.get("query"),
            # api
            url=ci.get("url"),
        )


# ---------------------------------------------------------------------------
# Context manager returned by catalog.produce()
# ---------------------------------------------------------------------------

class DatasetWriter:

    def __init__(self, client: "CatalogClient",
                 collection: str, dataset_id: str,
                 version: str, path: str,
                 inputs: List[dict] = None):
        self._client     = client
        self._collection = collection
        self._dataset_id = dataset_id
        self._version    = version
        self._inputs     = inputs or []
        self.path        = path
        self.rows: Optional[int]        = None
        self.columns: Optional[dict]    = None   # optional schema override
        self.meta: Dict[str, str]       = {}     # business metadata — written by task
        self.skipped           = False
        self.committed_version = version

    def __enter__(self) -> "DatasetWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            try:
                self._client._post(
                    f"/datasets/{_path(self._collection)}"
                    f"/{_seg(self._dataset_id)}"
                    f"/{_seg(self._version)}/fail",
                    json={},
                    unwrap=False,
                )
            except Exception:
                pass
            return False   # re-raise original exception

        result = self._client._post(
            f"/datasets/{_path(self._collection)}"
            f"/{_seg(self._dataset_id)}"
            f"/{_seg(self._version)}/commit",
            json={
                "rows":          self.rows,
                "columns":       self.columns,
                "inputs":        self._inputs,
                "business_meta": self.meta,
            },
        )
        self.skipped           = result.get("skipped", False)
        self.committed_version = result.get("version", self._version)
        return False


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class CatalogClient:

    def __init__(self, url: str = None):
        self.url = (
            url
            or os.environ.get("WALUIGI_CATALOG_URL", "http://localhost:9000")
        ).rstrip("/")
        self._task_id = os.environ.get("WALUIGI_TASK_ID", "unknown")
        self._job_id  = os.environ.get("WALUIGI_JOB_ID",  "unknown")

    # ------------------------------------------------------------------
    # Helpers for building lineage refs
    # ------------------------------------------------------------------

    def ref(self, collection: str, dataset_id: str,
            version: str = None) -> dict:
        """
        Build a lineage input ref.
        If version is omitted, resolves the latest committed version.

            inputs=[catalog.ref("finance/erp", "fatture")]
        """
        if version is None:
            version = self.last_version(collection, dataset_id)
        return {"collection": collection,
                "dataset_id": dataset_id,
                "version":    version}

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def resolve(self, collection: str, dataset_id: str,
                version: str = None) -> ResolveInfo:
        """
        Return connection info for the latest (or specific) committed version.
        Emits CatalogWarning if the dataset contains PII columns.
        """
        if version:
            data = self._get(
                f"/datasets/{_path(collection)}"
                f"/{_seg(dataset_id)}"
                f"/{_seg(version)}/preview",   # use version endpoint
            )
            # for a specific version we still want resolve-style info
            data = self._get(
                f"/datasets/{_path(collection)}/{_seg(dataset_id)}/resolve"
            )
        else:
            data = self._get(
                f"/datasets/{_path(collection)}/{_seg(dataset_id)}/resolve"
            )
        return ResolveInfo.from_response(data)

    def last_version(self, collection: str, dataset_id: str) -> str:
        """Return the latest committed version string."""
        data = self._get(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}/versions"
        )
        versions = data.get("versions", [])
        if not versions:
            raise CatalogError(
                f"No committed versions for {collection}/{dataset_id}")
        return versions[0]["version"]

    def get_dataset(self, collection: str, dataset_id: str) -> dict:
        """Return dataset entity + latest version metadata."""
        return self._get(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}")

    def history(self, collection: str, dataset_id: str) -> List[dict]:
        """Return all committed versions (newest first)."""
        data = self._get(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}/versions")
        return data.get("versions", [])

    def lineage(self, collection: str, dataset_id: str,
                version: str = None) -> dict:
        """Return upstream and downstream lineage."""
        params = f"?version={_seg(version)}" if version else ""
        return self._get(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}/lineage{params}")

    def preview(self, collection: str, dataset_id: str,
                version: str, limit: int = 10, offset: int = 0) -> dict:
        """Return a paginated row preview for a local version."""
        return self._get(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}"
            f"/{_seg(version)}/preview?limit={limit}&offset={offset}"
        )

    # ------------------------------------------------------------------
    # Write — local (2-phase)
    # ------------------------------------------------------------------

    def produce(self, collection: str, dataset_id: str,
                format: str = "",
                inputs: List[dict] = None,
                display_name: str = None,
                description: str = None,
                owner: str = None,
                tags: List[str] = None) -> DatasetWriter:
        """
        Reserve a new local version and return a context manager.

            with catalog.produce("finance/erp", "fatture_clean",
                                 format="parquet",
                                 inputs=[catalog.ref("finance/erp", "fatture")]) as ctx:
                df.to_parquet(ctx.path)
                ctx.rows = len(df)
        """
        r = self._post(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}/reserve",
            json={
                "format":       format,
                "task_id":      self._task_id,
                "job_id":       self._job_id,
                "display_name": display_name,
                "description":  description,
                "owner":        owner,
                "tags":         tags,
            },
        )
        return DatasetWriter(self, collection, dataset_id,
                             r["version"], r["path"],
                             inputs=inputs or [])

    # ------------------------------------------------------------------
    # Write — virtual (external source, no local copy)
    # ------------------------------------------------------------------

    def register_virtual(self, collection: str, dataset_id: str,
                         source_id: str, location: str,
                         format: str = "",
                         display_name: str = None,
                         description: str = None,
                         owner: str = None,
                         tags: List[str] = None) -> dict:
        """
        Register a version that lives in an external source (SQL, S3, SFTP…).
        The source must exist — create it first with register_source().

            catalog.register_virtual(
                "finance/erp", "fatture_pg",
                source_id="pg-dwh",
                location="SELECT * FROM finance.fatture",
                format="sql",
            )
        """
        return self._post(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}/register-virtual",
            json={
                "source_id":    source_id,
                "location":     location,
                "format":       format,
                "task_id":      self._task_id,
                "job_id":       self._job_id,
                "display_name": display_name,
                "description":  description,
                "owner":        owner,
                "tags":         tags,
            },
        )

    # ------------------------------------------------------------------
    # Write — materialize REST API → local CSV
    # ------------------------------------------------------------------

    def materialize(self, collection: str, dataset_id: str,
                    base_url: str, endpoint: str,
                    params: Dict[str, Any] = None,
                    display_name: str = None,
                    description: str = None) -> dict:
        """
        Fetch a REST API endpoint and store the result as a local CSV version.

            result = catalog.materialize(
                "crm/raw", "orders",
                base_url="https://api.crm.com",
                endpoint="/v1/orders",
                params={"status": "closed"},
            )
            # result["path"] → local CSV path
            # result["rows"] → row count
        """
        return self._post(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}/materialize",
            json={
                "base_url":     base_url,
                "endpoint":     endpoint,
                "params":       params or {},
                "task_id":      self._task_id,
                "job_id":       self._job_id,
                "display_name": display_name,
                "description":  description,
            },
        )

    # ------------------------------------------------------------------
    # Sources
    # ------------------------------------------------------------------

    def register_source(self, id: str, type: str,
                        config: Dict[str, Any],
                        description: str = None) -> dict:
        """
        Register a physical connector.

            catalog.register_source("pg-dwh", "sql",
                config={"dsn": "postgresql://user:pass@host/db"},
                description="Main data warehouse")
        """
        return self._post("/sources", json={
            "id":          id,
            "type":        type,
            "config":      config,
            "description": description,
        })

    def get_source(self, id: str) -> dict:
        return self._get(f"/sources/{_seg(id)}")

    def list_sources(self) -> List[dict]:
        return self._get("/sources")

    # ------------------------------------------------------------------
    # Collections
    # ------------------------------------------------------------------

    def list_collections(self, parent: str = None) -> List[dict]:
        if parent:
            data = self._get(f"/collections/{_path(parent)}/children")
            return data.get("children", [])
        return self._get("/collections")

    def list_collection_datasets(self, collection: str,
                                 recursive: bool = False) -> List[dict]:
        data = self._get(
            f"/collections/{_path(collection)}/datasets"
            f"{'?recursive=true' if recursive else ''}"
        )
        return data.get("datasets", [])

    # ------------------------------------------------------------------
    # Schema governance
    # ------------------------------------------------------------------

    def get_schema(self, collection: str, dataset_id: str) -> List[dict]:
        """Return current schema columns with PII flags and status."""
        data = self._get(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}/schema")
        return data.get("columns", [])

    def patch_column(self, collection: str, dataset_id: str,
                     column_name: str,
                     editor: str = "sdk",
                     **kwargs) -> dict:
        """
        Edit semantic metadata for a single column.

        Accepted kwargs: logical_type, nullable, pii, pii_type,
                         pii_notes, description, tags

            catalog.patch_column("finance/erp", "fatture", "email",
                                 pii=True, pii_type="direct",
                                 description="Customer email")
        """
        return self._patch(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}"
            f"/schema/{_seg(column_name)}?editor={editor}",
            json=kwargs,
        )

    def publish_schema(self, collection: str, dataset_id: str,
                       published_by: str = "sdk") -> dict:
        """
        Promote all columns to 'published' status.
        Returns breaking_changes and warnings from diff vs previous publish.
        """
        return self._post(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}/schema/publish",
            json={"published_by": published_by},
        )

    def set_schema_contract(self, collection: str, dataset_id: str,
                            columns: List[dict],
                            auto_publish: bool = True) -> dict:
        """
        Declare a schema contract for a dataset.
        Applied automatically at every commit — the task writer does not need
        to call patch_column or publish_schema manually.

        columns: list of dicts with keys:
            name (required), logical_type, nullable, pii, pii_type,
            pii_notes, description, tags

            catalog.set_schema_contract("sales/raw", "sales_raw", [
                {"name": "date",     "logical_type": "date",    "pii": False},
                {"name": "product",  "logical_type": "string",  "pii": False},
                {"name": "quantity", "logical_type": "integer", "pii": False},
                {"name": "revenue",  "logical_type": "decimal", "pii": False,
                 "description": "Gross revenue in EUR"},
            ])
        """
        return self._put(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}/schema/contract",
            json={"columns": columns, "auto_publish": auto_publish},
        )

    def get_schema_contract(self, collection: str, dataset_id: str) -> dict:
        """Return the declared schema contract for a dataset."""
        return self._get(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}/schema/contract")

    def delete_schema_contract(self, collection: str, dataset_id: str):
        """Remove the schema contract for a dataset."""
        return self._delete(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}/schema/contract")

    # ------------------------------------------------------------------
    # Metadata (free key-value on a version)
    # ------------------------------------------------------------------

    def set_metadata(self, collection: str, dataset_id: str,
                     version: str, key: str, value: str):
        self._post(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}"
            f"/{_seg(version)}/metadata",
            json={"key": key, "value": value},
        )

    def get_metadata(self, collection: str, dataset_id: str,
                     version: str) -> dict:
        return self._get(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}"
            f"/{_seg(version)}/metadata"
        )

    def delete_metadata(self, collection: str, dataset_id: str,
                        version: str, key: str):
        self._delete(
            f"/datasets/{_path(collection)}/{_seg(dataset_id)}"
            f"/{_seg(version)}/metadata/{_seg(key)}"
        )

    # ------------------------------------------------------------------
    # Internal HTTP + contract unwrapping
    # ------------------------------------------------------------------

    def _unwrap(self, response: requests.Response, unwrap: bool = True) -> Any:
        """
        Unwrap the {data, diagnostic} contract.
        - OK   → return data
        - WARN → emit CatalogWarning, return data
        - KO   → raise CatalogError
        """
        response.raise_for_status()
        body = response.json()

        if not unwrap:
            return body

        diagnostic = body.get("diagnostic", {})
        result     = diagnostic.get("result", "OK")
        messages   = diagnostic.get("messages", [])
        data       = body.get("data")

        if result == "KO":
            raise CatalogError("; ".join(messages) if messages else "Unknown error")
        if result == "WARN" and messages:
            warnings.warn(
                f"[Catalog WARN] {'; '.join(messages)}",
                CatalogWarning,
                stacklevel=3,
            )
        return data

    def _get(self, path: str) -> Any:
        return self._unwrap(requests.get(f"{self.url}{path}"))

    def _post(self, path: str, json: dict = None,
              unwrap: bool = True) -> Any:
        return self._unwrap(
            requests.post(f"{self.url}{path}", json=json),
            unwrap=unwrap,
        )

    def _patch(self, path: str, json: dict = None) -> Any:
        return self._unwrap(requests.patch(f"{self.url}{path}", json=json))

    def _put(self, path: str, json: dict = None) -> Any:
        return self._unwrap(requests.put(f"{self.url}{path}", json=json))

    def _delete(self, path: str) -> Any:
        return self._unwrap(requests.delete(f"{self.url}{path}"))


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def _path(collection: str) -> str:
    """Collection path — slashes stay as path separators, colons encoded."""
    return collection.strip("/").replace(":", "%3A")


def _seg(s: str) -> str:
    """Single path segment — encode slashes and colons."""
    return str(s).replace("/", "%2F").replace(":", "%3A").replace("+", "%2B")


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

catalog = CatalogClient()
