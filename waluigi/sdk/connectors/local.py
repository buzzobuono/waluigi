import os
import csv
import json
import pickle
import pandas as pd
from typing import Any, Dict
from waluigi.catalog.models import DatasetFormat
from .base import BaseConnector
import hashlib

class LocalConnector(BaseConnector):
    
    def exists(self, location: str) -> bool:
        return os.path.exists(location)

    def checksum(self, location: str) -> str:
        h = hashlib.sha256()
        with open(location, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
        
    def resolve_location(self, dataset_id: str, version: str, format: str, data_path: str) -> str:
        safe_ver = version.replace(":", "-")
        ext = f".{format}" if format else ""
        d = os.path.join(data_path, dataset_id)
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, f"{safe_ver}{ext}")
 
    def write(self, location: str, format: DatasetFormat, data: Any) -> int:  # ← int
        os.makedirs(os.path.dirname(location), exist_ok=True)
    
        if format == DatasetFormat.CSV:
            if isinstance(data, pd.DataFrame):
                data.to_csv(location, index=False)
                return len(data)                          # ← aggiunto
            else:
                with open(location, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
                return len(data)                          # ← aggiunto
    
        elif format == DatasetFormat.PARQUET:
            df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
            df.to_parquet(location, index=False)
            return len(df)                                # ← aggiunto
    
        elif format == DatasetFormat.JSON:
            with open(location, "w") as f:
                json.dump(data, f)
            return len(data) if hasattr(data, "__len__") else 0   # ← aggiunto
    
        elif format in (DatasetFormat.PKL, DatasetFormat.PICKLE):
            with open(location, "wb") as f:
                pickle.dump(data, f)
            return len(data) if hasattr(data, "__len__") else 0   # ← aggiunto
    
        elif format in (DatasetFormat.XLS, DatasetFormat.XLSX):
            df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
            df.to_excel(location, index=False)
            return len(df)                                # ← aggiunto
    
        elif format == DatasetFormat.FEATHER:
            df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
            df.to_feather(location)
            return len(df)                                # ← aggiunto
    
        elif format == DatasetFormat.ORC:
            df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
            df.to_orc(location)
            return len(df)                                # ← aggiunto
    
        else:
            raise NotImplementedError(f"Format {format} not supported by LocalConnector")
            
    def delete(self, location: str) -> None:
        if os.path.exists(location):
            os.remove(location)

    def read(self, location: str, format: DatasetFormat,
             limit: int = None, offset: int = 0) -> Any:
        if format == DatasetFormat.CSV:
            if limit is not None:
                skip = range(1, offset + 1) if offset else None
                return pd.read_csv(location, skiprows=skip, nrows=limit)
            return pd.read_csv(location)

        elif format == DatasetFormat.PARQUET:
            df = pd.read_parquet(location)
            if limit is not None:
                return df.iloc[offset: offset + limit]
            return df

        elif format == DatasetFormat.JSON:
            with open(location) as f:
                data = json.load(f)
            if limit is not None and isinstance(data, list):
                return data[offset: offset + limit]
            return data

        elif format in (DatasetFormat.PKL, DatasetFormat.PICKLE):
            with open(location, "rb") as f:
                data = pickle.load(f)
            if limit is not None and isinstance(data, list):
                return data[offset: offset + limit]
            return data

        elif format in (DatasetFormat.XLS, DatasetFormat.XLSX):
            df = pd.read_excel(location)
            if limit is not None:
                return df.iloc[offset: offset + limit]
            return df

        elif format == DatasetFormat.FEATHER:
            df = pd.read_feather(location)
            if limit is not None:
                return df.iloc[offset: offset + limit]
            return df

        elif format == DatasetFormat.ORC:
            df = pd.read_orc(location)
            if limit is not None:
                return df.iloc[offset: offset + limit]
            return df

        else:
            raise NotImplementedError(f"Format {format} not supported by LocalConnector")