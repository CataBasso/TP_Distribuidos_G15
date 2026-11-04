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


def test_each_batch_is_published_as_a_batch_with_header_and_eof_carries_the_total(gw):
    gateway, broker = gw
    s, cid = connect(gateway)
    send_frame(s, {"type": "batch", "batch_id": 0, "rows": [{"url": "u1"}]})
    send_frame(s, {"type": "batch", "batch_id": 1, "rows": [{"url": "u2"}, {"url": "u3"}]})
    send_frame(s, {"type": "eof"})
    b0, b1, eof = [broker.get_year() for _ in range(3)]
    assert b0 == {"type": "batch", "client_id": cid, "batch_id": 0, "origin": 3,
                  "rows": [{"url": "u1"}]}
    assert (b1["type"], b1["batch_id"], b1["rows"]) == ("batch", 1, [{"url": "u2"}, {"url": "u3"}])
    assert eof == {"type": "eof", "client_id": cid, "origin": 3, "total": 2}   # total = batches
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
    broker.finish(c1)                            # los EOF de las cinco consultas
    assert recv_frame(s1) == {"type": "done", "client_id": c1}
    assert recv_frame(s1) is None                # el Gateway cerró la sesión
    time.sleep(0.2)
    assert broker.aborts.empty()                 # se completó: no hay abort


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
    broker.results_q.put(b"basura")
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
    assert broker.get_year()["rows"] == [row]
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


# --- conteo de EOF por consulta (lo que antes hacía el Joiner) ---------------------------------
P, K = 3, 2
EXPECTED = {1: 1, 2: 1, 3: P, 4: 1, 5: K}


@pytest.fixture
def gw_pk():
    broker = InMemoryBroker()
    gateway = Gateway(gw_id=3, broker=broker, host="127.0.0.1", port=0, expected_eofs=EXPECTED)
    gateway.start()
    yield gateway, broker
    gateway.stop()


def frames_until_closed(s, limit=20):
    out = []
    while len(out) < limit:
        msg = recv_frame(s)
        if msg is None:
            break
        out.append(msg)
    return out


def test_done_only_when_every_query_has_all_its_eofs(gw_pk):
    gateway, broker = gw_pk
    s, cid = connect(gateway)
    broker.finish(cid, EXPECTED, skip={(3, 2)})          # falta el EOF de la partición 2 de Q3
    time.sleep(0.3)
    s.settimeout(0.3)
    with pytest.raises(socket.timeout):
        recv_frame(s)                                    # todavía no hay 'done'
    assert gateway.tracker.pending(cid)[3] == (2, P)
    s.settimeout(5)
    broker.publish_result({"type": "eof", "client_id": cid, "origin": 2, "query": 3})
    assert recv_frame(s) == {"type": "done", "client_id": cid}


def test_missing_q5_eof_means_no_done(gw_pk):
    gateway, broker = gw_pk
    s, cid = connect(gateway)
    broker.finish(cid, EXPECTED, skip={(5, 1)})
    time.sleep(0.3)
    assert gateway.tracker.pending(cid)[5] == (1, K)
    assert not gateway._sessions[cid[1]].done


def test_results_are_forwarded_in_order_and_eofs_are_not(gw_pk):
    gateway, broker = gw_pk
    s, cid = connect(gateway)
    broker.publish_result({"type": "result", "client_id": cid, "query": 1,
                           "rows": [{"date_modified": "2019-01-01 00:00:00", "url": "u"}]})
    broker.publish_result({"type": "result", "client_id": cid, "query": 4,
                           "rows": [{"word": "a", "count": 3}]})
    broker.finish(cid, EXPECTED)
    got = frames_until_closed(s)
    assert [m["type"] for m in got] == ["result", "result", "done"]   # ningún 'eof' llega al cliente
    assert [m.get("query") for m in got[:2]] == [1, 4]


def test_concurrent_clients_are_counted_separately(gw_pk):
    gateway, broker = gw_pk
    s1, c1 = connect(gateway)
    s2, c2 = connect(gateway)
    broker.finish(c1, EXPECTED)
    broker.finish(c2, EXPECTED, skip={(2, 0)})
    assert recv_frame(s1)["type"] == "done"
    time.sleep(0.3)
    assert not gateway._sessions[c2[1]].done              # al segundo le falta Q2
    broker.publish_result({"type": "eof", "client_id": c2, "origin": 0, "query": 2})
    assert recv_frame(s2)["type"] == "done"


def test_duplicated_eof_from_the_same_origin_counts_once(gw_pk):
    gateway, broker = gw_pk
    s, cid = connect(gateway)
    broker.finish(cid, EXPECTED, skip={(3, 1), (3, 2)})
    for _ in range(2):
        broker.publish_result({"type": "eof", "client_id": cid, "origin": 0, "query": 3})
    time.sleep(0.3)
    assert gateway.tracker.pending(cid)[3] == (1, P)


def test_eof_with_unknown_query_is_ignored(gw_pk):
    gateway, broker = gw_pk
    s, cid = connect(gateway)
    broker.publish_result({"type": "eof", "client_id": cid, "origin": 0, "query": 9})
    broker.publish_result({"type": "eof", "client_id": cid, "origin": 0})      # sin consulta
    broker.finish(cid, EXPECTED)
    assert recv_frame(s)["type"] == "done"                # el resto del flujo sigue andando


def test_disconnect_discards_the_eof_state(gw_pk):
    gateway, broker = gw_pk
    s, cid = connect(gateway)
    broker.finish(cid, EXPECTED, skip={(5, 0)})
    time.sleep(0.3)
    assert gateway.tracker.pending(cid)[1] == (1, 1)
    s.close()
    broker.get_abort()
    time.sleep(0.2)
    assert gateway.tracker.pending(cid)[1] == (0, 1)      # se olvidó todo de ese cliente
