import os
import re
from typing import Any, Dict
from waluigi.catalog.api.schemas import SourceType
from .base import BaseConnector
from .local import LocalConnector
from .s3 import S3Connector
from .sql import SQLConnector
from .sharepoint import SharePointConnector
#from .sftp import SFTPConnector


def _expand(config: Dict[str, Any]) -> Dict[str, Any]:
    """Expand ${VAR} placeholders in string config values against os.environ."""
    def _expand_val(v):
        if isinstance(v, str):
            return re.sub(r"\$\{([^}]+)\}", lambda m: os.environ.get(m.group(1), m.group(0)), v)
        if isinstance(v, dict):
            return {k: _expand_val(w) for k, w in v.items()}
        if isinstance(v, list):
            return [_expand_val(i) for i in v]
        return v
    return {k: _expand_val(v) for k, v in config.items()}


class ConnectorFactory:

    _registry = {
        SourceType.LOCAL:       LocalConnector,
        SourceType.S3:          S3Connector,
        SourceType.SQL:         SQLConnector,
        SourceType.SHAREPOINT:  SharePointConnector,
        #SourceType.SFTP:       SFTPConnector,
    }

    @classmethod
    def get(cls, source_type: SourceType, config: Dict[str, Any]) -> BaseConnector:
        klass = cls._registry.get(source_type)
        if klass is None:
            raise NotImplementedError(f"No connector for source type: {source_type}")
        return klass(_expand(config))

    @classmethod
    def register(cls, source_type: SourceType, klass: type) -> None:
        """Permette di registrare connector custom a runtime."""
        cls._registry[source_type] = klass