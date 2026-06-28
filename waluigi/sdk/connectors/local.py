import os
import warnings
import hashlib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Any, Dict, Iterator
from waluigi.catalog.api.schemas import DatasetFormat
from waluigi.catalog.utils import _infer_schema_from_df
from .base import BaseConnector


def _to_df(chunk) -> pd.DataFrame:
    if isinstance(chunk, pd.DataFrame):
        return chunk
    if isinstance(chunk, pa.Table):
        return chunk.to_pandas()
    return pd.DataFrame(chunk)


def _is_stream(data) -> bool:
    return not isinstance(data, (pd.DataFrame, pa.Table, list, dict))


def _coerce_mixed_types(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce object columns with mixed types to string so PyArrow can write Parquet."""
    mixed = [c for c in df.select_dtypes(include="object").columns
             if df[c].apply(type).nunique() > 1]
    if not mixed:
        return df
    df = df.copy()
    for col in mixed:
        warnings.warn(
            f"Column '{col}' has mixed types — coercing to string for Parquet write",
            stacklevel=4,
        )
        df[col] = df[col].astype(str)
    return df


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

    def infer_schema(self, location: str) -> list[dict]:
        ext = os.path.splitext(location)[1].lstrip(".").lower()
        try:
            if ext in ("csv", "tsv"):
                df = pd.read_csv(location, sep="\t" if ext == "tsv" else ",", nrows=1000)
            elif ext == "parquet":
                df = pd.read_parquet(location)
            elif ext == "json":
                df = pd.read_json(location)
            else:
                return []
            return _infer_schema_from_df(df)
        except Exception:
            return []

    # ------------------------------------------------------------------
    # write
    # ------------------------------------------------------------------

    def write(self, location: str, format: DatasetFormat, data: Any) -> int:
        os.makedirs(os.path.dirname(location), exist_ok=True)
        if _is_stream(data):
            return self._write_stream(location, format, data)
        return self._write_batch(location, format, _to_df(data))

    def _write_batch(self, location: str, format: DatasetFormat, df: pd.DataFrame) -> int:
        if format == DatasetFormat.CSV:
            df.to_csv(location, index=False)
        elif format == DatasetFormat.TSV:
            df.to_csv(location, index=False, sep="\t")
        elif format == DatasetFormat.PARQUET:
            df = _coerce_mixed_types(df)
            df.to_parquet(location, index=False)
        elif format == DatasetFormat.JSON:
            df.to_json(location, orient="records", lines=True)
        else:
            raise NotImplementedError(f"Format {format} not supported by LocalConnector")
        return len(df)

    def _write_stream(self, location: str, format: DatasetFormat,
                      stream: Iterator) -> int:
        if format in (DatasetFormat.CSV, DatasetFormat.TSV):
            return self._stream_csv(location, format, stream)
        if format == DatasetFormat.PARQUET:
            return self._stream_parquet(location, stream)
        if format == DatasetFormat.JSON:
            return self._stream_jsonl(location, stream)
        raise NotImplementedError(
            f"Streaming not supported for format {format}")

    def _stream_csv(self, location, format, stream) -> int:
        count, first = 0, True
        sep = "\t" if format == DatasetFormat.TSV else ","
        for chunk in stream:
            df = _to_df(chunk)
            df.to_csv(location, mode="a", header=first, index=False, sep=sep)
            count += len(df)
            first = False
        return count

    def _stream_parquet(self, location, stream) -> int:
        writer, count = None, 0
        try:
            for chunk in stream:
                df = _coerce_mixed_types(_to_df(chunk))
                table = pa.Table.from_pandas(df, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(location, table.schema)
                writer.write_table(table)
                count += len(df)
        finally:
            if writer:
                writer.close()
        return count

    def _stream_jsonl(self, location, stream) -> int:
        count = 0
        with open(location, "a", encoding="utf-8") as f:
            for chunk in stream:
                df = _to_df(chunk)
                f.write(df.to_json(orient="records", lines=True))
                count += len(df)
        return count

    # ------------------------------------------------------------------
    # delete / read
    # ------------------------------------------------------------------

    def delete(self, location: str) -> None:
        if os.path.exists(location):
            os.remove(location)

    def read(self, location: str, format: DatasetFormat,
             limit: int = None, offset: int = 0) -> Any:
        if format in (DatasetFormat.CSV, DatasetFormat.TSV):
            sep = "\t" if format == DatasetFormat.TSV else ","
            if limit is not None:
                skip = range(1, offset + 1) if offset else None
                return pd.read_csv(location, sep=sep, skiprows=skip, nrows=limit)
            return pd.read_csv(location, sep=sep)

        if format == DatasetFormat.PARQUET:
            df = pd.read_parquet(location)
            return df.iloc[offset: offset + limit] if limit is not None else df

        if format == DatasetFormat.JSON:
            df = pd.read_json(location, lines=True)
            return df.iloc[offset: offset + limit] if limit is not None else df

        raise NotImplementedError(f"Format {format} not supported by LocalConnector")
