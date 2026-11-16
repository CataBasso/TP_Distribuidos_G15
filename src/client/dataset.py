from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

COLUMNS = ["url", "name", "in_language", "date_modified", "abstract", "sections", "image"]
DEFAULT_BATCH_ROWS = 200
DEFAULT_BATCH_BYTES = 1 << 20 # 1MiB
import os

# filas que se convierten a Python de una vez (no cambia el row group que Arrow lee del disco)
READ_CHUNK = int(os.environ.get("PARQUET_READ_CHUNK", DEFAULT_BATCH_ROWS))


def parquet_files(data_dir) -> list[Path]:
    return sorted(Path(data_dir).rglob("*.parquet"))


def _format_date(value):
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def project(record: dict) -> dict:
    language = record.get("in_language") or {}
    image = record.get("image") or {}
    return {
        "url": record.get("url"),
        "name": record.get("name"),
        "lang": language.get("identifier"),
        "date_modified": _format_date(record.get("date_modified")),
        "abstract": record.get("abstract"),
        "sections": record.get("sections"),
        "image_url": image.get("content_url"),
    }


def iter_rows(data_dir) -> Iterator[dict]:
    files = parquet_files(data_dir)
    if not files:
        raise FileNotFoundError(f"no hay archivos .parquet bajo {data_dir}")
    for path in files:
        reader = pq.ParquetFile(path)
        try:
            for chunk in reader.iter_batches(batch_size=READ_CHUNK, columns=COLUMNS):
                for record in chunk.to_pylist():
                    yield project(record)
        finally:
            reader.close()
            pa.default_memory_pool().release_unused()      # devuelve al SO la memoria Arrow del archivo


def iter_batches(data_dir, max_rows: int = DEFAULT_BATCH_ROWS,
                 max_bytes: int = DEFAULT_BATCH_BYTES) -> Iterator[list[dict]]:
    batch, size = [], 0
    for row in iter_rows(data_dir):
        batch.append(row)
        size += len(row["abstract"] or "") + len(row["sections"] or "") + 200
        if len(batch) >= max_rows or size >= max_bytes:
            yield batch
            batch, size = [], 0
    if batch:
        yield batch
