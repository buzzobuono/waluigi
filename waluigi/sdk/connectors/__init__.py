from .base import BaseConnector
from .factory import ConnectorFactory
from .local import LocalConnector
from .s3 import S3Connector
from .sql import SQLConnector
#from .sftp import SFTPConnector

__all__ = [
    "BaseConnector",
    "ConnectorFactory",
    "LocalConnector",
    "S3Connector",
    "SQLConnector",
    #"SFTPConnector",
]