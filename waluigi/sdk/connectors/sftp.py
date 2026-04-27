import io
import json
import pickle
import pandas as pd
import paramiko
from typing import Any, Dict
from waluigi.catalog.models import DatasetFormat
from .base import BaseConnector


class SFTPConnector(BaseConnector):
    """
    config atteso:
        host:     str
        port:     int  (default 22)
        username: str
        password: str  (opzionale)
        key_path: str  (opzionale, path chiave privata)
    """

    def _connect(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        kwargs = dict(
            hostname=self.config["host"],
            port=self.config.get("port", 22),
            username=self.config["username"],
        )
        if "key_path" in self.config:
            kwargs["key_filename"] = self.config["key_path"]
        else:
            kwargs["password"] = self.config["password"]
        ssh.connect(**kwargs)
        return ssh

    def _serialize(self, data: Any, format: DatasetFormat) -> bytes:
        buf = io.BytesIO()
        if format == DatasetFormat.CSV:
            df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
            df.to_csv(buf, index=False)
        elif format == DatasetFormat.PARQUET:
            df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
            df.to_parquet(buf, index=False)
        elif format == DatasetFormat.JSON:
            buf.write(json.dumps(data).encode())
        elif format in (DatasetFormat.PKL, DatasetFormat.PICKLE):
            pickle.dump(data, buf)
        else:
            raise NotImplementedError(f"Format {format} not supported by SFTPConnector")
        buf.seek(0)
        return buf.read()

    def write(self, location: str, format: DatasetFormat, data: Any) -> None:
        ssh = self._connect()
        try:
            sftp = ssh.open_sftp()
            buf = io.BytesIO(self._serialize(data, format))
            sftp.putfo(buf, location)
        finally:
            ssh.close()

    def delete(self, location: str) -> None:
        ssh = self._connect()
        try:
            sftp = ssh.open_sftp()
            sftp.remove(location)
        finally:
            ssh.close()

    def read(self, location: str, format: DatasetFormat) -> Any:
        ssh = self._connect()
        try:
            sftp = ssh.open_sftp()
            buf = io.BytesIO()
            sftp.getfo(location, buf)
            buf.seek(0)
            if format == DatasetFormat.CSV:
                return pd.read_csv(buf)
            elif format == DatasetFormat.PARQUET:
                return pd.read_parquet(buf)
            elif format == DatasetFormat.JSON:
                return json.load(buf)
            elif format in (DatasetFormat.PKL, DatasetFormat.PICKLE):
                return pickle.load(buf)
            else:
                raise NotImplementedError(f"Format {format} not supported by SFTPConnector")
        finally:
            ssh.close()