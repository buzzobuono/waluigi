"""
waluigi.sdk.catalog

Format-agnostic data catalog SDK.
The catalog manages metadata, versioning, and lineage.
The task decides how to read and write the file.

Usage:

    from waluigi.sdk.catalog import catalog

    # READ — resolve physical path, open it yourself
    path = catalog.resolve("raw_erp")
    # then do whatever you want:
    #   import polars as pl; df = pl.read_parquet(path)
    #   import pandas as pd; df = pd.read_csv(path)
    #   with open(path, "rb") as f: ...

    # WRITE — context manager handles reserve + commit
    with catalog.produce("clean_erp",
                         namespace="sales/europe/clean",
                         format="parquet",
                         inputs=[("raw_erp", catalog.last_version("raw_erp"))]) as ctx:
        df.write_parquet(ctx.path)     # polars
        ctx.rows = len(df)             # optional
        ctx.schema = {"col": "type"}   # optional — inferred by catalog if omitted

    # on __exit__: catalog computes hash and commits automatically
    # on exception: catalog marks the version as failed

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

    def __init__(self, client, id, version, path):
        self._client = client
        self._id = id
        self._version = version
        self.path = path       # write your file here
        self.rows = None       # optional — set it if you know it
        self.schema = None     # optional — catalog infers it if omitted

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # task crashed — mark as failed, do not suppress exception
            try:
                self._client._post(
                    f"/datasets/{self._id}/{_encode(self._version)}/fail", json={})
            except Exception:
                pass
            return False

        # happy path — commit
        self._client._post(
            f"/datasets/{self._id}/{_encode(self._version)}/commit",
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
        self._job_id = os.environ.get("WALUIGI_JOB_ID", "unknown")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve(self, id, version=None):
        """
        Return the physical path of a dataset.
        Uses the latest committed version unless version is specified.
        The task is responsible for opening the file in the right format.
        """
        if version:
            r = self._get(f"/datasets/{id}/{_encode(version)}/resolve")
        else:
            r = self._get(f"/datasets/{id}/resolve")
        return r["path"]

    def produce(self, id, namespace, format="", inputs=None):
        """
        Reserve a new version for dataset `id` and return a DatasetWriter
        context manager. Write your file to ctx.path inside the `with` block.

        namespace: required — e.g. "sales/europe/clean"
        format:    file extension without dot — "parquet", "csv", "pkl", etc.
        inputs:    list of (id, version) tuples for lineage tracking
        """
        inputs_payload = [{"id": i[0], "version": i[1]} for i in (inputs or [])]
        r = self._post(f"/datasets/{id}/reserve", json={
            "namespace": namespace,
            "format": format,
            "task_id": self._task_id,
            "job_id": self._job_id,
            "inputs": inputs_payload
        })
        return DatasetWriter(self, id, r["version"], r["path"])

    def last_version(self, id):
        """Return the latest committed version string for a dataset."""
        return self._get(f"/datasets/{id}")["version"]

    def metadata(self, id, version=None):
        """Return full metadata for a dataset (latest or specific version)."""
        if version:
            return self._get(f"/datasets/{id}/{_encode(version)}")
        return self._get(f"/datasets/{id}")

    def history(self, id):
        """Return all committed versions for a dataset."""
        return self._get(f"/datasets/{id}/history")

    def upstream(self, id, version):
        """Return datasets this version was derived from."""
        return self._get(f"/lineage/{id}/{_encode(version)}")

    def downstream(self, id, version):
        """Return datasets derived from this version."""
        return self._get(f"/lineage/{id}/{_encode(version)}/downstream")

    def set_metadata(self, id, key, value):
        """Set a custom metadata key on a dataset (not version-specific)."""
        self._post(f"/datasets/{id}/metadata", json={"key": key, "value": value})

    def get_metadata(self, id):
        """Return all custom metadata for a dataset."""
        return self._get(f"/datasets/{id}/metadata")

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


def _encode(version):
    return version.replace(":", "%3A")


# Module-level singleton — reads config from env at import time
catalog = CatalogClient()
