import os
import warnings
from typing import Any, Dict, List, Optional
import requests

from waluigi.core.utils import _model_dump
from waluigi.catalog.models import *

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
   
    def folders(self, prefix: str = "") -> dict:
        return self._get(f"/folders/{prefix}/")
        
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
     
    def _unwrap(self, response: requests.Response) -> Any:
        if response.status_code == 204:
            return None
    
        if not response.text or response.text.strip() == "":
            return None
    
        try:
            body = response.json()
        except Exception as e:
            raise CatalogError(f"Server returned invalid JSON. Status: {response.status_code}. Content: {response.text[:100]}")
    
        diagnostic = body.get("diagnostic", {})
        result     = diagnostic.get("result", "OK")
        messages   = diagnostic.get("messages", [])
        data       = body.get("data")
        
        if result == "KO":
            raise CatalogError("; ".join(messages) if messages else "Unknown error")
        
        if result == "WARN" and messages:
            warnings.warn(f"[Catalog WARN] {'; '.join(messages)}", CatalogWarning, stacklevel=3)
        
        return data
        
    def _get(self, path: str) -> Any:
        return self._unwrap(requests.get(f"{self.url}{path}"))
        
    def _post(self, path: str, json: dict = None) -> Any:
        resp = requests.post(f"{self.url}{path}", json=json)
        return self._unwrap(resp)
    
    def _patch(self, path: str, json: dict = None) -> Any:
        return self._unwrap(requests.patch(f"{self.url}{path}", json=json))

    def _delete(self, path: str) -> Any:
        return self._unwrap(requests.delete(f"{self.url}{path}"))
        
catalog = CatalogClient()
