import io
import csv
import json
import pickle
import pandas as pd
import boto3
from typing import Any, Dict
from urllib.parse import urlparse
from waluigi.catalog.models import DatasetFormat
from .base import BaseConnector


class S3Connector(BaseConnector):
    """
    config atteso:
        bucket:            str  (opzionale se incluso nel location URI)
        aws_access_key_id: str
        aws_secret_access_key: str
        region_name:       str
        endpoint_url:      str  (opzionale, per S3-compatible come MinIO)
    """

    def __init__(self, config: Dict):
        super().__init__(config)
        self._s3 = boto3.client(
            "s3",
            aws_access_key_id=config.get("aws_access_key_id"),
            aws_secret_access_key=config.get("aws_secret_access_key"),
            region_name=config.get("region_name"),
            endpoint_url=config.get("endpoint_url"),
        )

    def _parse(self, location: str):
        """Restituisce (bucket, key) da s3://bucket/key o /bucket/key."""
        parsed = urlparse(location)
        if parsed.scheme == "s3":
            return parsed.netloc, parsed.path.lstrip("/")
        raise ValueError(f"Invalid S3 location: {location}")

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
        elif format in (DatasetFormat.XLS, DatasetFormat.XLSX):
            df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
            df.to_excel(buf, index=False)
        else:
            raise NotImplementedError(f"Format {format} not supported by S3Connector")
        buf.seek(0)
        return buf.read()

    def write(self, location: str, format: DatasetFormat, data: Any) -> None:
        bucket, key = self._parse(location)
        body = self._serialize(data, format)
        self._s3.put_object(Bucket=bucket, Key=key, Body=body)

    def delete(self, location: str) -> None:
        bucket, key = self._parse(location)
        self._s3.delete_object(Bucket=bucket, Key=key)

    def read(self, location: str, format: DatasetFormat) -> Any:
        bucket, key = self._parse(location)
        obj = self._s3.get_object(Bucket=bucket, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        if format == DatasetFormat.CSV:
            return pd.read_csv(buf)
        elif format == DatasetFormat.PARQUET:
            return pd.read_parquet(buf)
        elif format == DatasetFormat.JSON:
            return json.load(buf)
        elif format in (DatasetFormat.PKL, DatasetFormat.PICKLE):
            return pickle.load(buf)
        elif format in (DatasetFormat.XLS, DatasetFormat.XLSX):
            return pd.read_excel(buf)
        else:
            raise NotImplementedError(f"Format {format} not supported by S3Connector")