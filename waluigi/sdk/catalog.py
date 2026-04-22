import os
import warnings
from typing import Any, Dict, List, Optional
import httpx

from waluigi.core.utils import _model_dump
from waluigi.catalog.models import *

class CatalogError(Exception):
    """Raised when the catalog returns result=KO."""
    
class CatalogWarning(UserWarning):
    """Raised (as a warning) when the catalog returns result=WARN."""

class DatasetWriter:

    def __init__(self, client: "CatalogClient",
                 dataset_id: str, version: str, path: str,
                 inputs: List[dict] = None):
        self._client     = client
        self._dataset_id = dataset_id
        self._version    = version
        self._inputs     = inputs or []
        self.path               = path
        self.rows: Optional[int]     = None
        self.columns: Optional[dict] = None
        self.meta: Dict[str, str]    = {}
        self.skipped                 = False
        self.committed_version       = version

    def __enter__(self) -> "DatasetWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            try:
                self._client._post(
                    f"/datasets/{self._dataset_id}/_fail/{self._version}",
                    json={}, unwrap=False)
            except Exception:
                pass
            return False

        result = self._client._post(
            f"/datasets/{self._dataset_id}/_commit/{self._version}",
            json={
                "inputs": self._inputs,
                "metadata": self.meta,
            },
        )
        self.skipped           = result.get("skipped", False)
        self.committed_version = result.get("version", self._version)
        return False

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
    
    
    def produce(self, dataset: DatasetCreateRequest,
                inputs: List[dict] = None) -> DatasetWriter:
        self.create_dataset(dataset)
        r = self._post(
            f"/datasets/{dataset.id}/_reserve"
        )
        return DatasetWriter(self, dataset.id,
                             r["version"], r["location"],
                             inputs=inputs or [])

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
            warnings.warn(f"[Catalog WARN] {'; '.join(messages)}", CatalogWarning, stacklevel=3)
        
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
        
catalog = CatalogClient()
