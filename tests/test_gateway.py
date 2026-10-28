import queue
import socket
import time

import pytest

import struct

from common.protocol import send_frame, recv_frame
from fakes import InMemoryBroker
from gateway.gateway import Gateway


@pytest.fixture
def gw():
    broker = InMemoryBroker()
    gateway = Gateway(gw_id=3, broker=broker, host="127.0.0.1", port=0)
    gateway.start()
    yield gateway, broker
    gateway.stop()


def connect(gateway):
    s = socket.create_connection(("127.0.0.1", gateway.port), timeout=5)
    welcome = recv_frame(s)
    assert welcome["type"] == "welcome"
    return s, tuple(welcome["client_id"])


def test_assigns_client_ids_with_gateway_id_and_increasing_n(gw):
    gateway, _ = gw
    _, a = connect(gateway)
    _, b = connect(gateway)
    assert a[0] == b[0] == 3 and b[1] == a[1] + 1


def test_each_row_is_published_as_an_article_with_header_and_sequential_batch_ids(gw):
    gateway, broker = gw
    s, cid = connect(gateway)
    send_frame(s, {"type": "batch", "batch_id": 0, "rows": [{"url": "u1"}]})
    send_frame(s, {"type": "batch", "batch_id": 1, "rows": [{"url": "u2"}, {"url": "u3"}]})
    send_frame(s, {"type": "eof"})
    a1, a2, a3, eof = [broker.get_year() for _ in range(4)]
    assert a1 == {"type": "article", "client_id": cid, "batch_id": 0, "origin": 3,
                  "article": {"url": "u1"}}
    assert [(a["batch_id"], a["article"]["url"]) for a in (a2, a3)] == [(1, "u2"), (1, "u3")]
    assert eof == {"type": "eof", "client_id": cid, "origin": 3}
    assert broker.aborts.empty()                 # terminó bien: nada que abortar


def test_results_go_only_to_the_owner_client_and_done_closes_session(gw):
    gateway, broker = gw
    s1, c1 = connect(gateway)
    s2, c2 = connect(gateway)
    broker.publish_result({"type": "result", "client_id": c2, "query": 3,
                           "rows": [{"word": "ada"}]})
    broker.publish_result({"type": "result", "client_id": c1, "query": 1,
                           "rows": [{"date_modified": "2019-01-01 00:00:00", "url": "u"}]})
    r2, r1 = recv_frame(s2), recv_frame(s1)
    assert (r2["type"], r2["query"], r2["rows"]) == ("result", 3, [{"word": "ada"}])
    assert r1["query"] == 1
    broker.publish_result({"type": "done", "client_id": c1})
    assert recv_frame(s1) == {"type": "done", "client_id": c1}
    assert recv_frame(s1) is None                # el Gateway cerró la sesión
    time.sleep(0.2)
    assert broker.aborts.empty()                 # recibió 'done': no hay abort


def test_disconnect_before_done_publishes_abort(gw):
    gateway, broker = gw
    s, cid = connect(gateway)
    send_frame(s, {"type": "batch", "batch_id": 0, "rows": []})
    s.close()
    assert broker.get_abort() == {"type": "abort", "client_id": cid}


def test_disconnect_after_eof_but_before_done_also_aborts(gw):
    gateway, broker = gw
    s, cid = connect(gateway)
    send_frame(s, {"type": "eof"})
    broker.get_year()
    s.close()
    assert broker.get_abort()["client_id"] == cid


def test_results_for_a_gone_client_or_other_gateway_are_acked_and_dropped(gw):
    gateway, broker = gw
    broker.publish_result({"type": "result", "client_id": (3, 999), "query": 1, "rows": []})
    broker.publish_result({"type": "result", "client_id": (9, 1), "query": 1, "rows": []})
    broker.out_q.put(b"basura")
    deadline = time.time() + 3
    while broker.acked < 3 and time.time() < deadline:
        time.sleep(0.02)
    assert broker.acked == 3                     # ninguno queda trabado en la cola


def test_unexpected_message_type_gets_error_and_connection_is_closed(gw):
    gateway, broker = gw
    s, cid = connect(gateway)
    send_frame(s, {"type": "result", "query": 1, "rows": []})   # válido, pero no de un cliente
    err = recv_frame(s)
    assert err["type"] == "error" and err["code"] == "invalid_message"
    assert recv_frame(s) is None
    assert broker.get_abort()["client_id"] == cid


def test_protocol_violation_gets_error_and_abort(gw):
    gateway, broker = gw
    s, cid = connect(gateway)
    s.sendall(struct.pack(">I", 5) + struct.pack(">I", 999) + b"\x00")   # tipo 999 desconocido
    err = recv_frame(s)
    assert err["type"] == "error" and err["code"] == "invalid_message"
    assert recv_frame(s) is None
    assert broker.get_abort()["client_id"] == cid


def test_binary_values_in_rows_and_results_survive_the_round_trip(gw):
    gateway, broker = gw
    s, cid = connect(gateway)
    row = {"url": "u", "abstract": None, "name": "Ñandú", "image": None}
    send_frame(s, {"type": "batch", "batch_id": 0, "rows": [row]})
    assert broker.get_year()["article"] == row
    broker.publish_result({"type": "result", "client_id": cid, "query": 2,
                           "rows": [{"name": "n", "url": "u", "image": b"\x00\xff"}]})
    assert recv_frame(s)["rows"][0]["image"] == b"\x00\xff"


def test_stop_aborts_clients_in_flight_and_closes_connections():
    broker = InMemoryBroker()
    gateway = Gateway(1, broker, "127.0.0.1", 0)
    gateway.start()
    s, cid = connect(gateway)
    gateway.stop()
    assert broker.get_abort()["client_id"] == cid
    assert recv_frame(s) is None


def test_many_concurrent_clients_get_their_own_results(gw):
    gateway, broker = gw
    clients = [connect(gateway) for _ in range(8)]
    for _, cid in clients:
        broker.publish_result({"type": "result", "client_id": cid, "query": 3,
                               "rows": [{"word": f"w{cid[1]}"}]})
    for s, cid in clients:
        assert recv_frame(s)["rows"] == [{"word": f"w{cid[1]}"}]
