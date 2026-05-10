from __future__ import annotations
import os
import httpx
import logging
from typing import Any, Dict, Iterator, List, Union
import pandas as pd
import pyarrow as pa

from waluigi.core.utils import _model_dump
from waluigi.catalog.models import (
    DatasetCreateRequest, DatasetFormat, DatasetStatus,
    SourceCreateRequest,
)
from waluigi.sdk.connectors import ConnectorFactory
from waluigi.sdk.connectors.base import BaseConnector

logger = logging.getLogger("waluigi")

Tabular = Union[
    list[dict],
    dict[str, list],
    pd.DataFrame,
    pa.Table,
    Iterator[list[dict]],
    Iterator[pd.DataFrame],
]


class CatalogError(Exception):
    """Raised when the catalog returns result=KO."""


class CatalogWarning(UserWarning):
    """Raised (as a warning) when the catalog returns result=WARN."""


class CatalogClient:
    """Client for pipeline tasks: read and write catalog datasets.

    Typical usage::

        catalog = CatalogClient()

        # Create/upsert a dataset, then configure and write a new version
        handle = catalog.create_dataset("sales/raw", format="parquet", source_id="local")
        handle.set_expectations([{"rule_id": "expect_column_values_to_not_be_null", "inputs": {"x": "this.col"}}])
        handle.set_chart("revenue_by_date", "Revenue by date", spec={...})

        with handle.create_version(metadata={"date": "2026-01-01"}) as writer:
            writer.write(df)

        # Read a dataset (latest committed version)
        reader = catalog.read_dataset("sales/raw")
        df = reader.read()
    """

    def __init__(self, url: str = None):
        self.url = (
            url
            or os.environ.get("WALUIGI_CATALOG_URL", "http://localhost:9000")
        ).rstrip("/")
        self._task_id = os.environ.get("WALUIGI_TASK_ID", "unknown")
        self._job_id  = os.environ.get("WALUIGI_JOB_ID",  "unknown")

    # ── BROWSE ────────────────────────────────────────────────────────────────

    def list_folders(self, prefix: str = "") -> dict:
        """List datasets and virtual sub-prefixes under a path prefix."""
        return self._get(f"/folders/{prefix}/")

    # ── SOURCES ───────────────────────────────────────────────────────────────

    def list_sources(self) -> List[dict]:
        return self._get("/sources")

    def get_source(self, id: str) -> dict:
        return self._get(f"/sources/{id}")

    def create_source(self, request: SourceCreateRequest) -> dict:
        return self._post("/sources", json=_model_dump(request))

    # ── DATASETS ──────────────────────────────────────────────────────────────

    def list_datasets(self, status: DatasetStatus = None,
                      description: str = None) -> List[dict]:
        params = {}
        if status:
            params["status"] = status.value if isinstance(status, DatasetStatus) else status
        if description:
            params["description"] = description
        return self._get("/datasets", params=params or None)

    def get_dataset(self, id: str) -> dict:
        return self._get(f"/datasets/{id}")

    def create_dataset(self, id: str, format: Union[str, DatasetFormat] = "parquet",
                       source_id: str = "", description: str = "") -> "DatasetHandle":
        """Create or upsert a dataset and return a handle for further operations.

        Call ``.set_expectations()`` and ``.set_chart()`` on the handle to
        configure the dataset, then ``.create_version()`` to write data::

            handle = catalog.create_dataset("my/dataset", format="parquet", source_id="local")
            handle.set_expectations([...])
            with handle.create_version(metadata={"date": "2026-01-01"}) as writer:
                writer.write(df)
        """
        fmt = DatasetFormat[format.upper()] if isinstance(format, str) else format
        self._post("/datasets", json=_model_dump(DatasetCreateRequest(
            id=id,
            format=fmt,
            source_id=source_id,
            description=description,
        )))
        return DatasetHandle(self, id, fmt, source_id)

    # ── VERSIONS ──────────────────────────────────────────────────────────────

    def list_versions(self, dataset_id: str) -> List[dict]:
        return self._get(f"/datasets/{dataset_id}/versions")

    def get_version_metadata(self, dataset_id: str, version: str) -> dict:
        return self._get(f"/datasets/{dataset_id}/versions/{version}/metadata")

    # ── LINEAGE ───────────────────────────────────────────────────────────────

    def get_lineage(self, dataset_id: str, version: str) -> dict:
        return self._get(f"/datasets/{dataset_id}/lineage/{version}")

    # ── EXPECTATIONS (DQ config) ───────────────────────────────────────────────

    def list_expectations(self, dataset_id: str) -> List[dict]:
        return self._get(f"/datasets/{dataset_id}/expectations")

    def add_expectation(self, dataset_id: str, rule_id: str,
                        inputs: dict = None, params: dict = None,
                        tolerance: float = 1.0, position: int = 0) -> dict:
        return self._post(f"/datasets/{dataset_id}/expectations", json={
            "rule_id":   rule_id,
            "inputs":    inputs or {},
            "params":    params or {},
            "tolerance": tolerance,
            "position":  position,
        })

    # ── CHARTS ────────────────────────────────────────────────────────────────

    def list_charts(self, dataset_id: str) -> List[dict]:
        return self._get(f"/datasets/{dataset_id}/charts")

    # ── DATA OPS ──────────────────────────────────────────────────────────────

    def read_dataset(self, dataset_id: str, version: str = None) -> "DatasetReader":
        """Return a DatasetReader for the latest (or a specific) committed version."""
        dataset  = self.get_dataset(dataset_id)
        versions = self.list_versions(dataset_id)
        if not versions:
            raise CatalogError(f"No committed version found for {dataset_id}")
        ver = (
            next((v for v in versions if v["version"] == version), versions[0])
            if version else versions[0]
        )
        source    = self.get_source(dataset["source_id"])
        connector = ConnectorFactory.get(source["type"], source["config"])
        return DatasetReader(
            dataset_id=dataset_id,
            version=ver["version"],
            location=ver["location"],
            fmt=DatasetFormat(dataset["format"]),
            connector=connector,
        )

    # ── TRANSPORT (private) ───────────────────────────────────────────────────

    def _unwrap(self, response: httpx.Response) -> Any:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            try:
                error_detail = e.response.json().get("detail", e.response.text)
            except Exception:
                error_detail = e.response.text
            raise CatalogError(f"HTTP {e.response.status_code}: {error_detail}")

        if response.status_code == 204:
            return None

        body       = response.json()
        diagnostic = body.get("diagnostic", {})
        result     = diagnostic.get("result", "OK")
        messages   = diagnostic.get("messages", [])
        data       = body.get("data")

        if result == "KO":
            raise CatalogError("; ".join(messages) if messages else "Unknown error")

        if result == "WARN" and messages:
            logger.warning("; ".join(messages))

        return data

    def _get(self, path: str, params: dict = None) -> Any:
        return self._unwrap(httpx.get(f"{self.url}{path}", params=params))

    def _post(self, path: str, json: dict = None, params: dict = None) -> Any:
        return self._unwrap(httpx.post(f"{self.url}{path}", json=json, params=params))

    def _patch(self, path: str, json: dict = None, params: dict = None) -> Any:
        return self._unwrap(httpx.patch(f"{self.url}{path}", json=json, params=params))

    def _delete(self, path: str, params: dict = None) -> Any:
        return self._unwrap(httpx.delete(f"{self.url}{path}", params=params))


# ── DatasetHandle ──────────────────────────────────────────────────────────────

class DatasetHandle:
    """Handle for a defined dataset. Use to set expectations, add charts, and produce versions."""

    def __init__(self, client: CatalogClient, id: str,
                 format: DatasetFormat, source_id: str):
        self._client   = client
        self.id        = id
        self.format    = format
        self.source_id = source_id

    def set_expectations(self, rules: List[dict]) -> List[dict]:
        """Replace all DQ expectations for this dataset.

        Expectations are evaluated automatically when a new version is committed.
        Call this before ``create_version()`` so DQ runs at commit time.

        Each rule: {"rule_id": str, "inputs": dict, "params": dict, "tolerance": float}
        """
        for exp in self._client.list_expectations(self.id):
            self._client._delete(f"/datasets/{self.id}/expectations/{exp['id']}")
        return [
            self._client.add_expectation(
                self.id,
                rule_id=r["rule_id"],
                inputs=r.get("inputs", {}),
                params=r.get("params", {}),
                tolerance=r.get("tolerance", 1.0),
                position=i,
            )
            for i, r in enumerate(rules)
        ]

    def set_chart(self, key: str, title: str, spec: dict,
                  position: int = 0) -> dict:
        """Create or update a chart definition for this dataset (upsert by key)."""
        existing = {c["key"]: c for c in self._client._get(f"/datasets/{self.id}/charts")}
        body = {"key": key, "title": title, "spec": spec, "position": position}
        if key in existing:
            return self._client._patch(
                f"/datasets/{self.id}/charts/{existing[key]['id']}", json=body
            )
        return self._client._post(f"/datasets/{self.id}/charts", json=body)

    def create_version(self, metadata: Dict[str, Any] = None,
                       inputs: List[dict] = None,
                       force: bool = False) -> "DatasetWriter":
        """Open a DatasetWriter to write a new dataset version (reserve → write → commit).

        DQ expectations defined via ``.set_expectations()`` are evaluated
        automatically at commit time. Use as a context manager::

            with handle.create_version(metadata={"date": "2026-01-01"}) as writer:
                writer.write(df)
        """
        metadata = metadata or {}
        inputs   = inputs or []
        result   = self._client._post(
            f"/datasets/{self.id}/_reserve",
            json={"metadata": metadata, "force": force},
        )
        source    = self._client.get_source(result["source_id"])
        connector = ConnectorFactory.get(source["type"], source["config"])
        return DatasetWriter(
            client=self._client,
            dataset_id=self.id,
            version=result["version"],
            location=result["location"],
            fmt=self.format,
            connector=connector,
            metadata=metadata,
            inputs=inputs,
            skipped=result["skipped"],
        )


# ── DatasetWriter ──────────────────────────────────────────────────────────────

class DatasetWriter:

    def __init__(
        self,
        client: CatalogClient,
        dataset_id: str,
        version: str,
        location: str,
        fmt: DatasetFormat = None,
        connector: BaseConnector = None,
        metadata: Dict[str, Any] = {},
        inputs: List[dict] = [],
        skipped: bool = False,
    ):
        self._client    = client
        self._connector = connector
        self._location  = location
        self._format    = fmt
        self.metadata: Dict[str, Any] = metadata
        self.dataset_id = dataset_id
        self.version    = version
        self.inputs     = inputs or []
        self.skipped    = skipped

    def write(self, data: Tabular) -> int:
        """Write tabular data to the reserved location. Returns row count."""
        if self.skipped:
            return 0
        if self._connector is None:
            raise CatalogError("Connector not initialized")
        return self._connector.write(self._location, self._format, data)

    def __enter__(self) -> DatasetWriter:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.skipped:
            return False
        if exc_type is not None:
            self._fail()
            return False
        self._commit()
        return False

    def _commit(self):
        self._client._post(
            f"/datasets/{self.dataset_id}/_commit/{self.version}",
            json={
                "inputs":   self.inputs,
                "metadata": self.metadata,
                "task_id":  self._client._task_id,
                "job_id":   self._client._job_id,
            },
        )

    def _fail(self):
        try:
            self._client._post(
                f"/datasets/{self.dataset_id}/_fail/{self.version}",
                json={},
            )
        except Exception:
            pass


# ── DatasetReader ──────────────────────────────────────────────────────────────

class DatasetReader:

    def __init__(
        self,
        dataset_id: str,
        version: str,
        location: str,
        fmt: DatasetFormat,
        connector: BaseConnector,
    ):
        self.dataset_id = dataset_id
        self.version    = version
        self.location   = location
        self.format     = fmt
        self._connector = connector

    def read(self, limit: int = None, offset: int = 0) -> Any:
        """Read and return the dataset contents."""
        return self._connector.read(self.location, self.format,
                                    limit=limit, offset=offset)


catalog = CatalogClient()
