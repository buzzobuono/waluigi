from datetime import datetime, timezone
from sqlalchemy import inspect
import os
import hashlib
import numpy as np
import pandas as pd

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _version_id() -> str:
    return _now()

def _infer_schema(path: str, fmt: str) -> list[dict]:
    try:
        if fmt in ("csv", "tsv"):
            df = pd.read_csv(path, sep="\t" if fmt == "tsv" else ",", nrows=1000)
        elif fmt == "parquet":
            df = pd.read_parquet(path)
        elif fmt in ("xls", "xlsx"):
            df = pd.read_excel(path, nrows=1000)
        else:
            return []

        type_map = {
            "int64": "integer", 
                "int32": "integer",
            "float64": "decimal", 
                "float32": "decimal",
            "bool": "boolean", 
                "datetime64[ns]": "datetime",
            "object": "string",
        }
        return [
            {
                "name": col,
                "physical_type": str(df[col].dtype),
                "logical_type":  type_map.get(str(df[col].dtype), "string")
            }
            for col in df.columns
        ]
    except Exception:
        return []


def _safe_json_value(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (float, int, np.number)):
        if not np.isfinite(float(v)):
            return None
        return v
    return v

