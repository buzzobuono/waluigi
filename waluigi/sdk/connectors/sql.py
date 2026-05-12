import hashlib
from typing import Any, Dict, Iterator
import pandas as pd
from sqlalchemy import create_engine, text
from waluigi.catalog.api.schemas import DatasetFormat
from waluigi.catalog.utils import _infer_schema_from_df
from .base import BaseConnector


def _is_stream(data) -> bool:
    return not isinstance(data, (pd.DataFrame, list, dict))


class SQLConnector(BaseConnector):
    """
    config:
        url: str  — SQLAlchemy DSN (e.g. postgresql+psycopg2://user:pw@host/db)

    location is a table name, optionally schema-qualified: "schema.table"
    For virtual datasets, location is a raw SELECT query.
    """

    def __init__(self, config: Dict):
        super().__init__(config)
        self._engine = create_engine(config["url"])

    def exists(self, location: str) -> bool:
        from sqlalchemy import inspect as sa_inspect
        if self._is_query(location):
            return True
        schema, table = self._split(location)
        return sa_inspect(self._engine).has_table(table, schema=schema)

    def checksum(self, location: str) -> str:
        df = pd.read_sql_table(location, con=self._engine).sort_index(axis=1)
        return hashlib.sha256(
            pd.util.hash_pandas_object(df, index=False).values.tobytes()
        ).hexdigest()

    def resolve_location(self, dataset_id: str, version: str, format: str, data_path: str) -> str:
        table_base = dataset_id.rstrip("/").split("/")[-1]
        safe_ver = (version
                    .replace(":", "")
                    .replace("-", "")
                    .replace("+", "")
                    .split(".")[0].lower())
        return f"{table_base}__{safe_ver}"

    def infer_schema(self, location: str) -> list[dict]:
        try:
            with self._engine.connect() as conn:
                if self._is_query(location):
                    sql = f"SELECT * FROM ({location}) AS _sub LIMIT 1000"
                else:
                    schema, table = self._split(location)
                    qualified = f"{schema}.{table}" if schema else table
                    sql = f"SELECT * FROM {qualified} LIMIT 1000"
                df = pd.read_sql(text(sql), conn)
            return _infer_schema_from_df(df)
        except Exception:
            return []

    # ------------------------------------------------------------------
    # write
    # ------------------------------------------------------------------

    def write(self, location: str, format: DatasetFormat, data: Any) -> int:
        schema, table = self._split(location)
        if _is_stream(data):
            return self._write_stream(schema, table, data)
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        df.to_sql(table, self._engine, schema=schema, if_exists="fail", index=False)
        return len(df)

    def _write_stream(self, schema, table, stream: Iterator) -> int:
        count, first = 0, True
        for chunk in stream:
            df = chunk if isinstance(chunk, pd.DataFrame) else pd.DataFrame(chunk)
            df.to_sql(table, self._engine, schema=schema,
                      if_exists="replace" if first else "append",
                      index=False)
            count += len(df)
            first = False
        return count

    # ------------------------------------------------------------------
    # delete / read
    # ------------------------------------------------------------------

    def delete(self, location: str) -> None:
        with self._engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {location}"))
            conn.commit()

    def read(self, location: str, format: DatasetFormat,
             limit: int = None, offset: int = 0) -> Any:
        with self._engine.connect() as conn:
            if self._is_query(location):
                if limit is not None:
                    sql = (f"SELECT * FROM ({location}) AS _sub"
                           f" LIMIT {int(limit)} OFFSET {int(offset)}")
                else:
                    sql = location
                return pd.read_sql(text(sql), conn)
            schema, table = self._split(location)
            if limit is not None:
                qualified = f"{schema}.{table}" if schema else table
                sql = (f"SELECT * FROM {qualified}"
                       f" LIMIT {int(limit)} OFFSET {int(offset)}")
                return pd.read_sql(text(sql), conn)
            return pd.read_sql_table(table, con=self._engine, schema=schema)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _is_query(self, location: str) -> bool:
        return location.strip().upper().startswith("SELECT")

    def _split(self, location: str):
        parts = location.split(".", 1)
        return (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])
