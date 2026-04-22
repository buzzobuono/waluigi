from datetime import datetime, timezone

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _version_id() -> str:
    return _now()
    
def _local_path(dataset_id: str, version: str, fmt: str, data_path: str) -> str:
    safe_ver = version.replace(":", "-")
    ext = f".{fmt}" if fmt else ""
    d = os.path.join(data_path, dataset_id)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f"{safe_ver}{ext}")
    
def _compute_hash(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


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
            "int64": "integer", "int32": "integer",
            "float64": "decimal", "float32": "decimal",
            "bool": "boolean", "datetime64[ns]": "datetime",
            "object": "string",
        }
        return [
            {"name": col,
             "physical_type": str(df[col].dtype),
             "logical_type":  type_map.get(str(df[col].dtype), "string")}
            for col in df.columns
        ]
    except Exception:
        return []


def _safe_json_value(v):
    import numpy as np
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

