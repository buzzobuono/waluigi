from __future__ import annotations
import os
import httpx
import logging
from typing import Any, Dict, Iterator, List, Union
import pandas as pd
import pyarrow as pa

from waluigi.core.utils import _model_dump
from waluigi.catalog.models import *
from waluigi.sdk.connectors import ConnectorFactory
from waluigi.sdk.connectors.base import BaseConnector

logger = logging.getLogger("waluigi")

Tabular = Union[
    list[dict],
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

    def __init__(self, url: str = None):
        self.url = (
            url
            or os.environ.get("WALUIGI_CATALOG_URL", "http://localhost:9000")
        ).rstrip("/")
        self._task_id = os.environ.get("WALUIGI_TASK_ID", "unknown")
        self._job_id  = os.environ.get("WALUIGI_JOB_ID",  "unknown")
   
    # Folders
    
    def folders(self, prefix: str = "") -> dict:
        return self._get(f"/folders/{prefix}/")
        
    # Sources
    
    def list_sources(self) -> List[dict]:
        return self._get("/sources")
        
    def create_source(self, request: SourceCreateRequest) -> dict:
        return self._post("/sources", json=_model_dump(request))

    def update_source(self, id: str, request: SourceUpdateRequest) -> dict:
        return self._patch(f"/sources/{id}", json=_model_dump(request))
        
    def get_source(self, id: str) -> dict:
        return self._get(f"/sources/{id}")
    
    def delete_source(self, id: str) -> dict:
        return self._delete(f"/sources/{id}")
     
    # Datasets
    
    def find_datasets(self, status: DatasetStatus, description: str) -> List[dict]:
        params = {
            "status": status.value,
            "description": description
        }
        return self._get("/datasets", params=params)
        
    def create_dataset(self, request: DatasetCreateRequest) -> dict:
        return self._post("/datasets", json=_model_dump(request))
    
    def update_dataset(self, id: str, request: DatasetUpdateRequest) -> dict:
        return self._patch(f"/datasets/{id}", json=_model_dump(request))
     
    def get_dataset(self, id: str) -> dict:
        return self._get(f"/datasets/{id}")
    
    def delete_dataset(self, id: str) -> dict:
        return self._delete(f"/datasets/{id}")
        
    def resolve(self, dataset_id: str) -> "DatasetReader":
        dataset  = self.get_dataset(dataset_id)
        versions = self._get(f"/datasets/{dataset_id}/versions")
        if not versions:
            raise CatalogError(f"No committed version found for {dataset_id}")
        latest   = versions[0]
        source   = self.get_source(dataset["source_id"])
        connector = ConnectorFactory.get(source["type"], source["config"])
        return DatasetReader(
            dataset_id=dataset_id,
            version=latest["version"],
            location=latest["location"],
            fmt=DatasetFormat(dataset["format"]),
            connector=connector,
        )

    def produce(self, dataset: DatasetCreateRequest, metadata: Dict[str, Any] = {}, inputs: List[dict] = []) -> DatasetWriter:
        self.create_dataset(dataset)
        result = self._post(f"/datasets/{dataset.id}/_reserve", json = { "metadata": metadata})
        source = self.get_source(result["source_id"])
        connector = ConnectorFactory.get(source["type"], source["config"])
        return DatasetWriter(
            client=self,
            dataset_id=dataset.id,
            version=result["version"],
            location=result["location"],
            fmt=dataset.format,
            connector=connector,
            metadata=metadata,
            inputs=inputs,
            skipped=result["skipped"]
        )
        
    # Commons
    
    def _unwrap(self, response: httpx.Response) -> Any:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            try:
                error_detail = e.response.json().get("detail", e.response.text)
            except:
                error_detail = e.response.text
            raise CatalogError(f"HTTP {e.response.status_code}: {error_detail}")
    
        if response.status_code == 204:
            return None
    
        body = response.json()
        diagnostic = body.get("diagnostic", {})
        result     = diagnostic.get("result", "OK")
        messages   = diagnostic.get("messages", [])
        data       = body.get("data")
        
        if result == "KO":
            raise CatalogError("; ".join(messages) if messages else "Unknown error")
        
        if result == "WARN" and messages:
            logger.warning(f"{'; '.join(messages)}")
        
        return data
    
        
    def _get(self, path: str, params: dict = None) -> Any:
        return self._unwrap(httpx.get(f"{self.url}{path}", params=params))
        
    def _post(self, path: str, json: dict = None, params: dict = None) -> Any:
        resp = httpx.post(f"{self.url}{path}", json=json, params=params)
        return self._unwrap(resp)
    
    def _patch(self, path: str, json: dict = None, params: dict = None) -> Any:
        return self._unwrap(httpx.patch(f"{self.url}{path}", json=json, params=params))

    def _delete(self, path: str, params: dict = None) -> Any:
        return self._unwrap(httpx.delete(f"{self.url}{path}", params=params))
        
        
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
        skipped: bool = False    
    ):
        self._client        = client
        self._connector     = connector
        self._location      = location
        self._format        = fmt
        self.metadata: Dict[str, Any] = metadata
        self.dataset_id     = dataset_id
        self.version        = version
        self.inputs         = inputs or []
        self.skipped        = skipped
        
    def write(self, data: Tabular) -> int:
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

    def _fail(self):
        try:
            self._client._post(
                f"/datasets/{self.dataset_id}/_fail/{self.version}",
                json={},
            )
        except Exception:
            pass

        self._cleanup()

    def _commit(self):
        result = self._client._post(
            f"/datasets/{self.dataset_id}/_commit/{self.version}",
            json={
                "inputs": self.inputs,
                "metadata": self.metadata
            },
        )
        
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
        return self._connector.read(self.location, self.format,
                                    limit=limit, offset=offset)


catalog = CatalogClient()
