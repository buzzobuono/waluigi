import hashlib
from datetime import datetime, timezone

class CatalogHelper:

    @staticmethod
    def now_iso():
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")

    @staticmethod
    def compute_hash(path):
        sha = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha.update(chunk)
        return sha.hexdigest()

    @staticmethod
    def infer_schema(path, fmt):
        try:
            if fmt == "parquet":
                import pyarrow.parquet as pq
                schema = pq.read_schema(path)
                return {name: str(schema.field(name).type) for name in schema.names}
            elif fmt in ("csv", "tsv"):
                import csv
                sep = "\t" if fmt == "tsv" else ","
                with open(path, newline="") as f:
                    headers = next(csv.reader(f, delimiter=sep), None)
                return {h: "string" for h in headers} if headers else None
        except Exception:
            pass
        return None

    