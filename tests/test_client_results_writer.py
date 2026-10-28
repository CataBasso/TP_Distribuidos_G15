import base64
import csv
import hashlib

from client.results_writer import ResultWriter


def read(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.reader(f))


def test_creates_all_files_with_headers_even_without_results(tmp_path):
    ResultWriter(tmp_path).close()
    assert read(tmp_path / "output_query_1.csv") == [["date_modified", "url"]]
    assert read(tmp_path / "output_query_2.csv") == [["name", "url", "image_sha256"]]
    assert read(tmp_path / "output_query_3.csv") == [["word"]]
    assert read(tmp_path / "output_query_4.csv") == [["word", "count"]]
    assert read(tmp_path / "output_query_5.csv") == [["url", "abstract_word_count"]]
    assert (tmp_path / "images").is_dir()


def test_chunks_are_appended_in_order_and_flushed_immediately(tmp_path):
    w = ResultWriter(tmp_path)
    w.handle({"query": 4, "rows": [{"word": "a", "count": 5}, {"word": "b", "count": 4}]})
    # sin cerrar: ya está en disco (streaming)
    assert read(tmp_path / "output_query_4.csv")[1:] == [["a", "5"], ["b", "4"]]
    w.handle({"query": 4, "rows": [{"word": "c", "count": 3}]})
    w.close()
    assert [r[0] for r in read(tmp_path / "output_query_4.csv")[1:]] == ["a", "b", "c"]
    assert w.rows_written[4] == 3


def test_queries_1_3_5_columns(tmp_path):
    w = ResultWriter(tmp_path)
    w.handle({"query": 1, "rows": [{"date_modified": "2019-04-29 07:54:22", "url": "u1"}]})
    w.handle({"query": 3, "rows": [{"word": "ada"}]})
    w.handle({"query": 5, "rows": [{"url": "https://x/1,3-Dichlorobenzene", "abstract_word_count": 38}]})
    w.close()
    assert read(tmp_path / "output_query_1.csv")[1] == ["2019-04-29 07:54:22", "u1"]
    assert read(tmp_path / "output_query_3.csv")[1] == ["ada"]
    assert read(tmp_path / "output_query_5.csv")[1] == ["https://x/1,3-Dichlorobenzene", "38"]  # coma entre comillas


def test_query2_saves_image_and_writes_its_sha256(tmp_path):
    content = b"\x89PNG fake image bytes"
    digest = hashlib.sha256(content).hexdigest()
    w = ResultWriter(tmp_path)
    w.handle({"query": 2, "rows": [
        {"name": "Ada", "url": "u", "image": base64.b64encode(content).decode()},
        {"name": "Sin imagen", "url": "v", "image": None},
    ]})
    w.close()
    rows = read(tmp_path / "output_query_2.csv")
    assert rows[1] == ["Ada", "u", digest] and rows[2] == ["Sin imagen", "v", ""]
    assert (tmp_path / "images" / digest).read_bytes() == content


def test_query2_accepts_image_as_raw_bytes(tmp_path):
    content = b"\x89PNG raw bytes"
    digest = hashlib.sha256(content).hexdigest()
    w = ResultWriter(tmp_path)
    w.handle({"query": 2, "rows": [{"name": "Ada", "url": "u", "image": content}]})
    w.close()
    assert read(tmp_path / "output_query_2.csv")[1] == ["Ada", "u", digest]
    assert (tmp_path / "images" / digest).read_bytes() == content


def test_unknown_query_is_rejected(tmp_path):
    import pytest
    w = ResultWriter(tmp_path)
    with pytest.raises(ValueError):
        w.handle({"query": 9, "rows": []})
    w.close()
