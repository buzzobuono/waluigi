"""
waluigi.sdk.catalog

Format-agnostic data catalog SDK.
Namespace is always required and is part of the dataset identity.

Usage:

    from waluigi.sdk.catalog import catalog

    # READ — resolve physical path, open it yourself
    path = catalog.resolve("sales/raw", "sales_raw")
    # then open however you want:
    #   import csv; reader = csv.DictReader(open(path))
    #   import polars as pl; df = pl.read_parquet(path)
    #   with open(path, "rb") as f: ...

    # WRITE — context manager handles reserve + commit
    with catalog.produce("sales/raw", "sales_raw", format="csv",
                         inputs=[("sales/raw", "raw_erp", catalog.last_version("sales/raw", "raw_erp"))]) as ctx:
        with open(ctx.path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["date", "product", "qty"])
            writer.writeheader()
            writer.writerows(rows)
        ctx.rows = len(rows)

    # on __exit__: catalog computes hash and commits automatically
    # on exception: catalog marks the version as failed

    # MATERIALIZE from REST API (creates new snapshot)
    path = catalog.resolve("crm/raw", "orders",
                           source="https://api.crm.com/orders")

Environment variables (injected by the worker):
    WALUIGI_CATALOG_URL   default: http://localhost:9000
    WALUIGI_TASK_ID
    WALUIGI_JOB_ID
"""

import os
import requests


# ---------------------------------------------------------------------------
# Context manager returned by catalog.produce()
# ---------------------------------------------------------------------------

class DatasetWriter:

    def __init__(self, client, namespace, id, version, path):
        self._client    = client
        self._namespace = namespace
        self._id        = id
        self._version   = version
        self.path       = path   # write your file here
        self.rows       = None   # optional — set if you know it
        self.schema     = None   # optional — catalog infers if omitted

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # task crashed — mark as failed, do not suppress exception
            try:
                self._client._post(
                    f"/datasets/{_enc(self._namespace)}/{_enc(self._id)}"
                    f"/{_enc(self._version)}/fail",
                    json={}
                )
            except Exception:
                pass
            return False

        # happy path — commit
        self._client._post(
            f"/datasets/{_enc(self._namespace)}/{_enc(self._id)}"
            f"/{_enc(self._version)}/commit",
            json={"rows": self.rows, "schema": self.schema}
        )
        return False


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class CatalogClient:

    def __init__(self, url=None):
        self.url = (
            url or os.environ.get("WALUIGI_CATALOG_URL", "http://localhost:9000")
        ).rstrip("/")
        self._task_id = os.environ.get("WALUIGI_TASK_ID", "unknown")
        self._job_id  = os.environ.get("WALUIGI_JOB_ID",  "unknown")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve(self, namespace, id, version=None, source=None):
        """
        Return the physical path of a dataset.

        namespace + id   uniquely identify the dataset.
        version          if omitted, uses latest committed version.
        source           if provided, materializes a new snapshot from
                         the given REST API URL and returns its path.
        """
        if source:
            r = self._post(
                f"/datasets/{_enc(namespace)}/{_enc(id)}/materialize",
                json={
                    "source":    source,
                    "task_id":   self._task_id,
                    "job_id":    self._job_id,
                }
            )
            return r["path"]

        if version:
            r = self._get(f"/datasets/{_enc(namespace)}/{_enc(id)}/{_enc(version)}/resolve")
        else:
            r = self._get(f"/datasets/{_enc(namespace)}/{_enc(id)}/resolve")
        return r["path"]

    def produce(self, namespace, id, format="", inputs=None):
        """
        Reserve a new version for dataset `id` in `namespace`.
        Returns a DatasetWriter context manager.
        Write your file to ctx.path inside the `with` block.

        namespace: required — e.g. "sales/europe/clean"
        id:        logical dataset name — e.g. "sales_raw"
        format:    file extension without dot — "csv", "parquet", "pkl", etc.
        inputs:    list of (namespace, id, version) tuples for lineage tracking
        """
        inputs_payload = [
            {"namespace": i[0], "id": i[1], "version": i[2]}
            for i in (inputs or [])
        ]
        r = self._post(f"/datasets/{_enc(namespace)}/{_enc(id)}/reserve", json={
            "format":   format,
            "task_id":  self._task_id,
            "job_id":   self._job_id,
            "inputs":   inputs_payload,
        })
        return DatasetWriter(self, namespace, id, r["version"], r["path"])

    def last_version(self, namespace, id):
        """Return the latest committed version string for a dataset."""
        return self._get(f"/datasets/{_enc(namespace)}/{_enc(id)}/latest")["version"]

    def metadata(self, namespace, id, version=None):
        """Return full metadata for a dataset (latest or specific version)."""
        if version:
            return self._get(f"/datasets/{_enc(namespace)}/{_enc(id)}/{_enc(version)}")
        return self._get(f"/datasets/{_enc(namespace)}/{_enc(id)}/latest")

    def history(self, namespace, id):
        """Return all committed versions for a dataset."""
        return self._get(f"/datasets/{_enc(namespace)}/{_enc(id)}/history")

    def upstream(self, namespace, id, version):
        """Return datasets this version was derived from."""
        return self._get(
            f"/lineage/{_enc(namespace)}/{_enc(id)}/{_enc(version)}")

    def downstream(self, namespace, id, version):
        """Return datasets derived from this version."""
        return self._get(
            f"/lineage/{_enc(namespace)}/{_enc(id)}/{_enc(version)}/downstream")

    def set_metadata(self, namespace, id, key, value):
        """Set a custom metadata key on a dataset (not version-specific)."""
        self._post(f"/datasets/{_enc(namespace)}/{_enc(id)}/metadata",
                   json={"key": key, "value": value})

    def get_metadata(self, namespace, id):
        """Return all custom metadata for a dataset."""
        return self._get(f"/datasets/{_enc(namespace)}/{_enc(id)}/metadata")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get(self, path):
        r = requests.get(f"{self.url}{path}")
        r.raise_for_status()
        return r.json()

    def _post(self, path, json=None):
        r = requests.post(f"{self.url}{path}", json=json)
        r.raise_for_status()
        return r.json()


def _enc(s):
    """URL-encode colons only — namespace slashes must stay as path separators."""
    return str(s).replace(":", "%3A")


# Module-level singleton — reads config from env at import time
catalog = CatalogClient()
