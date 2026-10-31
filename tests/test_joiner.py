"""Joiner por Gateway: conteo de EOF por consulta (1 + 1 + P + 1 + K) y entrega a out_q."""
from common.bus import InMemoryBus
from common.protocol import decode
from common.protocol.messages import abort_message, encode, result_message
from common.topology import ABORT_EX, RESULTS_EX
from results.joiner import Joiner

from fakes import out_messages, query_eof

P, K = 3, 2
C = (1, 1)


def setup(gw=1):
    bus = InMemoryBus()
    joiner = Joiner(bus, gw, P, K)
    joiner.start()
    return bus, joiner


def put(bus, body, gw=1):
    bus.send(RESULTS_EX, body, key=str(gw))


def finish_all(bus, client=C, skip=None):
    """Manda los EOF de todas las consultas (menos los de 'skip': {(query, origin)})."""
    skip = skip or set()
    plan = [(1, 0), (2, 0), (4, 0)] + [(3, p) for p in range(P)] + [(5, k) for k in range(K)]
    for q, o in plan:
        if (q, o) not in skip:
            put(bus, query_eof(client, q, origin=o), gw=client[0])


def kinds(bus, gw=1):
    return [m["type"] for m in out_messages(bus, gw)]


def test_done_solo_con_todos_los_eof():
    bus, j = setup()
    finish_all(bus)
    bus.drain()
    assert kinds(bus) == ["done"]


def test_falta_un_eof_de_q3_no_hay_done():
    bus, j = setup()
    finish_all(bus, skip={(3, 2)})
    bus.drain()
    assert kinds(bus) == []
    assert j.pending(C)[3] == (2, P)
    put(bus, query_eof(C, 3, origin=2))
    bus.drain()
    assert kinds(bus) == ["done"]


def test_falta_un_eof_de_q5_no_hay_done():
    bus, j = setup()
    finish_all(bus, skip={(5, 1)})
    bus.drain()
    assert kinds(bus) == []
    assert j.pending(C)[5] == (1, K)


def test_eof_repetido_del_mismo_origen_no_cuenta_dos_veces():
    bus, j = setup()
    finish_all(bus, skip={(3, 1), (3, 2)})
    put(bus, query_eof(C, 3, origin=0))
    put(bus, query_eof(C, 3, origin=0))
    bus.drain()
    assert kinds(bus) == []


def test_resultados_se_reenvian_tal_cual_y_done_va_ultimo():
    bus, _ = setup()
    r1 = result_message(C, 1, [{"date_modified": "2020-01-01", "url": "u"}])
    r4 = result_message(C, 4, [{"word": "a", "count": 3}])
    put(bus, encode(r1))
    put(bus, encode(r4))
    finish_all(bus)
    bus.drain()
    out = out_messages(bus, 1)
    assert [m["type"] for m in out] == ["result", "result", "done"]
    assert out[0]["rows"] == r1["rows"] and out[1]["query"] == 4
    assert tuple(out[2]["client_id"]) == C


def test_no_se_reenvian_los_eof():
    bus, _ = setup()
    finish_all(bus)
    bus.drain()
    assert "eof" not in kinds(bus)


def test_clientes_concurrentes_se_cuentan_por_separado():
    bus, _ = setup()
    c2 = (1, 2)
    finish_all(bus, C)
    finish_all(bus, c2, skip={(2, 0)})
    bus.drain()
    assert [tuple(m["client_id"]) for m in out_messages(bus, 1)] == [C]
    put(bus, query_eof(c2, 2))
    bus.drain()
    assert [tuple(m["client_id"]) for m in out_messages(bus, 1)] == [C, c2]


def test_mensaje_de_cliente_de_otro_gateway_se_descarta():
    bus, _ = setup(gw=1)
    finish_all(bus, client=(2, 1))                 # llega por la clave de gw 1 a propósito
    for q, o in [(1, 0), (2, 0), (4, 0)]:
        put(bus, query_eof((2, 1), q, o), gw=1)
    bus.drain()
    assert kinds(bus) == []


def test_done_no_se_duplica_por_eof_tardio():
    bus, _ = setup()
    finish_all(bus)
    bus.drain()
    put(bus, query_eof(C, 1))
    bus.drain()
    assert kinds(bus) == ["done"]


def test_abort_descarta_el_estado():
    bus, j = setup()
    finish_all(bus, skip={(5, 0)})
    bus.drain()
    bus.send(ABORT_EX, encode(abort_message(C)))
    bus.drain()
    put(bus, query_eof(C, 5, origin=0))
    bus.drain()
    assert kinds(bus) == []


def test_cada_joiner_atiende_solo_su_gateway():
    bus = InMemoryBus()
    j1, j2 = Joiner(bus, 1, P, K), Joiner(bus, 2, P, K)
    j1.start(); j2.start()
    finish_all(bus, client=(1, 1))
    finish_all(bus, client=(2, 7))
    bus.drain()
    assert [tuple(m["client_id"]) for m in out_messages(bus, 1)] == [(1, 1)]
    assert [tuple(m["client_id"]) for m in out_messages(bus, 2)] == [(2, 7)]


def test_eof_con_total_como_los_manda_q1_e_imagegetter():
    """Q1Filter e ImageGetter mandan eof(query, origin=replica, total=batches)."""
    from common.protocol.messages import eof_message
    bus, _ = setup()
    for q, o in [(1, 5), (2, 3)] + [(3, p) for p in range(P)] + [(4, 0)] + [(5, k) for k in range(K)]:
        m = eof_message(C, origin=o, query=q, total=17)
        put(bus, encode(m))
    bus.drain()
    assert kinds(bus) == ["done"]            # el origin de Q1/Q2 es cualquier réplica: cuenta 1
