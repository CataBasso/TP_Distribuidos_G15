import re
from datetime import datetime
from pathlib import Path

import pytest

from client.dataset import iter_batches, iter_rows, project, parquet_files

SAMPLE = Path("data/sample/small_dataset")
pytestmark = pytest.mark.skipif(not SAMPLE.exists(), reason="falta el dataset de ejemplo")


def test_project_flattens_nested_columns():
    row = project({
        "url": "u", "name": "n", "in_language": {"identifier": "fr"},
        "date_modified": datetime(2019, 4, 29, 7, 54, 22),
        "abstract": None, "sections": "[]", "image": {"content_url": "http://img"},
    })
    assert row == {"url": "u", "name": "n", "lang": "fr", "date_modified": "2019-04-29 07:54:22",
                   "abstract": None, "sections": "[]", "image_url": "http://img"}


def test_project_without_image_or_language():
    row = project({"url": "u", "name": "n", "in_language": None, "date_modified": None,
                   "abstract": "a", "sections": None, "image": None})
    assert row["lang"] is None and row["image_url"] is None and row["date_modified"] is None
    assert row["sections"] is None


def test_null_abstracts_are_kept_as_none():
    # el dataset de ejemplo tiene 12 abstracts nulos: no deben llegar como texto vacío
    assert sum(1 for r in iter_rows(SAMPLE) if r["abstract"] is None) == 12


def test_reads_both_languages_in_stable_order():
    files = [p.name for p in parquet_files(SAMPLE)]
    assert files == sorted(files) and len(files) == 2
    langs = {r["lang"] for r in iter_rows(SAMPLE)}
    assert langs == {"en", "fr"}


def test_batches_cover_every_row_once_and_respect_max_rows():
    total = sum(1 for _ in iter_rows(SAMPLE))
    batches = list(iter_batches(SAMPLE, max_rows=300))
    assert sum(len(b) for b in batches) == total == 2246
    assert all(0 < len(b) <= 300 for b in batches)


def test_batches_respect_max_bytes():
    batches = list(iter_batches(SAMPLE, max_rows=10_000, max_bytes=100_000))
    assert len(batches) > 5 and all(len(b) >= 1 for b in batches)


def test_dates_use_expected_text_format():
    pattern = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
    assert all(pattern.match(r["date_modified"]) for r in iter_rows(SAMPLE))


def test_missing_dataset_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        list(iter_batches(tmp_path))
