from abc import ABC, abstractmethod
from typing import Any, Dict
from waluigi.catalog.api.schemas import DatasetFormat


class BaseConnector(ABC):

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    def exists(self, location: str) -> bool:
        """Return True if the dataset version exists at location."""

    @abstractmethod
    def checksum(self, location: str) -> str:
        """Return a content hash for the data at location."""

    @abstractmethod
    def resolve_location(self, dataset_id: str, version: str, format: str, data_path: str) -> str:
        """Compute the write location for a new version."""

    @abstractmethod
    def write(self, location: str, format: DatasetFormat, data: Any) -> int:
        """Write data to location.

        data may be a DataFrame, list[dict], dict[str,list], pa.Table,
        or any Iterator/Generator of the above (streaming path).
        Returns the number of rows written.
        """

    @abstractmethod
    def delete(self, location: str) -> None:
        """Remove the data at location (used on rollback)."""

    @abstractmethod
    def read(self, location: str, format: DatasetFormat,
             limit: int = None, offset: int = 0) -> Any:
        """Read and return data from location.

        limit/offset enable pagination; None = read all.
        """

    @abstractmethod
    def infer_schema(self, location: str) -> list[dict]:
        """Infer column schema from the data at location.

        Returns list[dict] with keys: name, physical_type, logical_type.
        """
