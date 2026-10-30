import csv
import hashlib
import threading
from collections import Counter
from pathlib import Path

import pytest

from client.client import Client
from fakes import InMemoryBroker
from gateway.gateway import Gateway

SAMPLE = Path("data/sample/small_dataset")
EXPECTED = Path("data/expected")
pytestmark = pytest.mark.skipif(not (SAMPLE.exists() and EXPECTED.exists()),
                                reason="faltan los datos de ejemplo")
CHUNK = 100


def read_expected(name):
    with (EXPECTED / name).open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def fake_pipeline(broker, seen):
    """Reemplaza a YearFilter...Joiner: guarda lo que recibe y contesta al EOF."""
    q2_rows = read_expected("query2.csv")
    while True:
        msg = broker.get_year(timeout=20)
        seen.append(msg)
        if msg["type"] != "eof":
            continue
        cid = msg["client_id"]
        def chunks(query, rows):
            for i in range(0, max(len(rows), 1), CHUNK):
                broker.publish_result({"type": "result", "client_id": cid, "query": query,
                                       "rows": rows[i:i + CHUNK]})
        chunks(1, read_expected("query1.csv"))
        chunks(2, [{"name": r["name"], "url": r["url"],
                    "image": r["url"].encode()} for r in q2_rows])   # bytes: el protocolo los soporta
        chunks(3, read_expected("query3.csv"))
        chunks(4, [{"word": r["word"], "count": int(r["count"])} for r in read_expected("query4.csv")])
        chunks(5, [{"url": r["url"], "abstract_word_count": int(r["abstract_word_count"])}
                   for r in read_expected("query5.csv")])
        broker.publish_result({"type": "done", "client_id": cid})
        return


def rows(path):
    with Path(path).open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def test_full_session_matches_expected_results(tmp_path):
    broker = InMemoryBroker()
    gateway = Gateway(1, broker, "127.0.0.1", 0)
    port = gateway.start()
    seen = []
    t = threading.Thread(target=fake_pipeline, args=(broker, seen), daemon=True); t.start()
    try:
        Client("127.0.0.1", port, SAMPLE, tmp_path, batch_rows=300).run()
    finally:
        t.join(10); gateway.stop()

    # --- lo que recibió el "sistema" ---
    batches = [m for m in seen if m["type"] == "batch"]
    ids = [m["batch_id"] for m in batches]
    assert ids == sorted(ids) and set(ids) == set(range(max(ids) + 1))   # batch_id 0,1,2... sin huecos
    assert {m["client_id"] for m in seen} == {(1, 1)}
    assert sum(len(m["rows"]) for m in batches) == 2246   # todas las filas del dataset
    assert seen[-1]["type"] == "eof" and seen[-1]["total"] == len(batches)
    assert set(batches[0]["rows"][0]) == {"url", "name", "lang", "date_modified", "abstract",
                                          "sections", "image_url"}

    # --- lo que escribió el cliente ---
    for n, expected in ((1, "query1.csv"), (3, "query3.csv"), (5, "query5.csv")):
        got = rows(tmp_path / f"output_query_{n}.csv")
        want = rows(EXPECTED / expected)
        assert Counter(map(lambda r: tuple(sorted(r.items())), got)) == \
               Counter(map(lambda r: tuple(sorted(r.items())), want)), f"query {n}"
    assert rows(tmp_path / "output_query_4.csv") == rows(EXPECTED / "query4.csv")   # con orden

    q2 = rows(tmp_path / "output_query_2.csv")
    want_q2 = read_expected("query2.csv")
    assert [(r["name"], r["url"]) for r in q2] == [(r["name"], r["url"]) for r in want_q2]
    first = q2[0]
    assert first["image_sha256"] == hashlib.sha256(first["url"].encode()).hexdigest()
    assert (tmp_path / "images" / first["image_sha256"]).read_bytes() == first["url"].encode()


def test_two_clients_at_the_same_time_each_get_complete_results(tmp_path):
    broker = InMemoryBroker()
    gateway = Gateway(1, broker, "127.0.0.1", 0)
    port = gateway.start()
    seen = []
    # el pipeline falso atiende a los dos clientes, uno tras otro, al llegar sus EOF
    def two():
        for _ in range(2):
            fake_pipeline(broker, seen)
    t = threading.Thread(target=two, daemon=True); t.start()
    errors = []
    def run(i):
        try:
            Client("127.0.0.1", port, SAMPLE, tmp_path / f"c{i}", batch_rows=300).run()
        except Exception as e:      # pragma: no cover
            errors.append(e)
    clients = [threading.Thread(target=run, args=(i,)) for i in (1, 2)]
    for c in clients: c.start()
    for c in clients: c.join(30)
    t.join(10); gateway.stop()
    assert not errors
    assert {m["client_id"] for m in seen} == {(1, 1), (1, 2)}
    for i in (1, 2):
        assert len(rows(tmp_path / f"c{i}" / "output_query_4.csv")) == len(read_expected("query4.csv"))
