from typing import Any, Dict
import pandas as pd
import hashlib
from sqlalchemy import create_engine, text
from waluigi.catalog.models import DatasetFormat
from .base import BaseConnector

class SQLConnector(BaseConnector):
    """
    config atteso:
        url: str  (SQLAlchemy DSN, es. postgresql+psycopg2://user:pw@host/db)
    
    location è il nome della tabella, es. "schema.table_name"
    """

    def __init__(self, config: Dict):
        super().__init__(config)
        self._engine = create_engine(config["url"])
        
    def exists(self, location: str) -> bool:
        from sqlalchemy import inspect
        return inspect(self._engine).has_table(location)

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

    def write(self, location: str, format: DatasetFormat, data: Any) -> int:
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        schema, table = self._split(location)
        df.to_sql(table, self._engine, schema=schema,
                  if_exists="fail",
                  index=False)
        return len(df)          
    
    def delete(self, location: str) -> None:
        with self._engine.connect() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS {location}'))
            conn.commit()

    def read(self, location: str, format: DatasetFormat) -> Any:
        schema, table = self._split(location)
        return pd.read_sql_table(table, con=self._engine, schema=schema)

    def _split(self, location: str):
        """Restituisce (schema, table) da schema.table o (None, table)."""
        parts = location.split(".", 1)
        return (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])