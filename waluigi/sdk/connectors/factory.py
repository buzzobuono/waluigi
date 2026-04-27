from typing import Any, Dict
from waluigi.catalog.models import SourceType
from .base import BaseConnector
from .local import LocalConnector
from .s3 import S3Connector
from .sql import SQLConnector
#from .sftp import SFTPConnector


class ConnectorFactory:

    _registry = {
        SourceType.LOCAL: LocalConnector,
        SourceType.S3:    S3Connector,
        SourceType.SQL:   SQLConnector,
        #SourceType.SFTP:  SFTPConnector,
    }

    @classmethod
    def get(cls, source_type: SourceType, config: Dict[str, Any]) -> BaseConnector:
        klass = cls._registry.get(source_type)
        if klass is None:
            raise NotImplementedError(f"No connector for source type: {source_type}")
        return klass(config)

    @classmethod
    def register(cls, source_type: SourceType, klass: type) -> None:
        """Permette di registrare connector custom a runtime."""
        cls._registry[source_type] = klass