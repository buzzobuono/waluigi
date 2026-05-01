from abc import ABC, abstractmethod
from typing import Any, Dict, List
from waluigi.catalog.models import DatasetFormat


class BaseConnector(ABC):

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    def exists(self, location: str) -> bool:
        """Verify whether dataset version actually exists in the location provided"""

    @abstractmethod
    def checksum(self, location: str) -> str:
        """Calculate dataset version checksum"""

    @abstractmethod
    def resolve_location(self, dataset_id: str, version: str, format: str, data_path: str) -> str:
        """Resolve location of the dataset version """
        
    @abstractmethod
    def write(self, location: str, format: DatasetFormat, data: Any) -> None:
        """Scrive data in location nel formato specificato."""

    @abstractmethod
    def delete(self, location: str) -> None:
        """Rimuove il dato scritto in location (rollback)."""

    @abstractmethod
    def read(self, location: str, format: DatasetFormat,
             limit: int = None, offset: int = 0) -> Any:
        """Legge e restituisce i dati da location.

        limit/offset abilitano la paginazione: None = leggi tutto.
        """