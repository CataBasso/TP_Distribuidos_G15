"""LengthMapper -> AvgReducer -> Q5Filter con un InMemoryBus (sin RabbitMQ).

EOF por cobertura: el AbstractCleaner manda batches numerados 0..n-1 y un stage_eof con el total;
las réplicas terminan un cliente cuando la unión de lo que procesó cada una cubre 0..total-1."""
from common.bus import InMemoryBus
from common.protocol import decode
from common.protocol.messages import (
    abort_message, average_ready_message, eof_message, encode, q5_articles_batch_message,
    stage_eof_message, tokens_batch_message,
)
from common.topology import (
    ABORT_EX, AVG_EX, AVG_PEX, PERIOD_FROM_2021, PERIOD_UP_TO_2020, Q5_PEX, RESULTS_EX, TOK_EX,
    client_key, partition,
)
from filters.q5_filter.q5_filter import Q5Filter
from text_processing.avg_reducer.avg_reducer import AvgReducer
from text_processing.length_mapper.length_mapper import LengthMapper

from fakes import Feeder

C = (1, 1)
OLD, NEW = PERIOD_UP_TO_2020, PERIOD_FROM_2021


def build(tmp_path, lm=2, A=2, K=2):
    bus = InMemoryBus()
    stages = [LengthMapper(bus, r, 1, A, K) for r in range(lm)]
    stages += [AvgReducer(bus, a, lm, K) for a in range(A)]
    q5 = [Q5Filter(bus, k, lm, tmp_path / f"q5_{k}") for k in range(K)]
    stages += q5
    for s in stages:
        s.start()
    return bus, stages, q5, Feeder(bus)


def results(bus, client=C, query=5):
    rows, eofs = [], []
    for b in bus.bodies(RESULTS_EX):
        m = decode(b)
        if tuple(m["client_id"]) != client:
            continue
        if m["type"] == "result" and m["query"] == query:
            rows += m["rows"]
        elif m["type"] == "eof":
            eofs.append((m["query"], m["origin"]))
    return rows, eofs


def test_q5_end_to_end_strictly_greater_than_average(tmp_path):
    bus, _, _, f = build(tmp_path)
    for n in (2, 4, 6):                                  # <=2020 -> promedio 4.0
        f.article(C, f"old{n}", n, OLD)
    for n in (3, 4, 5, 9):                               # >=2021: 3 no, 4 no (estricto), 5 y 9 sí
        f.article(C, f"new{n}", n, NEW)
    f.eof(C)
    bus.drain()
    rows, eofs = results(bus)
    assert sorted((r["url"], r["abstract_word_count"]) for r in rows) == [("new5", 5), ("new9", 9)]
    assert sorted(eofs) == [(5, 0), (5, 1)]              # un EOF por partición K


def test_abstract_nulo_no_cuenta_en_el_promedio(tmp_path):
    bus, _, _, f = build(tmp_path)
    f.article(C, "a", 10, OLD)
    f.article(C, "b", None, OLD)
    f.article(C, "n1", 9, NEW)
    f.article(C, "n2", 11, NEW)
    f.eof(C)
    bus.drain()
    assert [r["url"] for r in results(bus)[0]] == ["n2"]          # promedio 10 (no 5)


def test_sin_articulos_hasta_2020_no_hay_filas_pero_termina(tmp_path):
    bus, _, _, f = build(tmp_path)
    f.article(C, "n1", 9, NEW)
    f.eof(C)
    bus.drain()
    rows, eofs = results(bus)
    assert rows == [] and len(eofs) == 2
    assert decode(bus.bodies(AVG_EX)[0])["average"] is None


def test_clientes_independientes(tmp_path):
    bus, _, _, f = build(tmp_path)
    c1, c2 = (1, 1), (1, 2)
    f.article(c1, "o1", 2, OLD)
    f.article(c2, "o2", 20, OLD)
    f.article(c1, "x1", 5, NEW)
    f.article(c2, "x2", 5, NEW)
    f.article(c2, "y2", 30, NEW)
    f.eof(c1)
    f.eof(c2)
    bus.drain()
    assert [r["url"] for r in results(bus, c1)[0]] == ["x1"]
    assert [r["url"] for r in results(bus, c2)[0]] == ["y2"]


def test_la_particion_de_promedio_depende_solo_del_cliente(tmp_path):
    bus, _, _, f = build(tmp_path, A=3)
    for i in range(5):
        f.article(C, f"u{i}", 2, OLD)
    f.eof(C)
    bus.drain()
    keys = {k for ex, k, b in bus.sent if ex == AVG_PEX}
    assert keys == {str(partition(client_key(C), 3))}


def test_lm_no_termina_hasta_cubrir_todos_los_batches(tmp_path):
    bus, _, _, f = build(tmp_path)
    f.article(C, "o", 2, OLD)
    f.article(C, "n", 5, NEW)
    f.eof(C, total=3)                                    # falta el batch 2
    bus.drain()
    assert results(bus)[1] == []
    f.article(C, "n2", 9, NEW)                           # llega el batch faltante
    bus.drain()
    assert len(results(bus)[1]) == 2


def test_eof_duplicado_y_reentrega_no_adelantan_la_terminacion(tmp_path):
    bus, _, _, f = build(tmp_path)
    f.article(C, "o", 2, OLD)
    f.eof(C, total=2)
    f.eof(C, total=2)                                    # duplicado
    bus.send(TOK_EX, encode(tokens_batch_message([["o", "en", OLD, 2, ["a", "b"]]], C, 0, origin=0)))
    bus.drain()                                          # el batch 0 reentregado no cubre al 1
    assert results(bus)[1] == []


def test_batch_reentregado_no_cambia_el_resultado(tmp_path):
    bus, _, _, f = build(tmp_path, lm=1, A=1, K=1)
    f.article(C, "o", 2, OLD)
    f.article(C, "n", 9, NEW)
    bus.send(TOK_EX, encode(tokens_batch_message([["n", "en", NEW, 9, ["w"] * 9]], C, 1, origin=0)))
    f.eof(C)
    bus.drain()
    assert [r["url"] for r in results(bus)[0]] == ["n"]  # una sola fila (el duplicado se descarta)


def test_chunks_de_resultado(tmp_path):
    bus = InMemoryBus()
    stages = [LengthMapper(bus, 0, 1, 1, 1), AvgReducer(bus, 0, 1, 1),
              Q5Filter(bus, 0, 1, tmp_path / "s", chunk_rows=2)]
    for s in stages:
        s.start()
    f = Feeder(bus)
    f.article(C, "o", 1, OLD)
    f.batch(C, [[f"n{i}", "en", NEW, 5, ["w"] * 5] for i in range(5)])
    f.eof(C)
    bus.drain()
    rows, _ = results(bus)
    assert sorted(r["url"] for r in rows) == [f"n{i}" for i in range(5)]


def test_abort_libera_estado_y_spool(tmp_path):
    bus, _, q5, f = build(tmp_path, K=1, A=1)
    f.article(C, "n1", 8, NEW)
    bus.drain()
    assert q5[0].spool.exists(C)
    bus.send(ABORT_EX, encode(abort_message(C)))
    bus.drain()
    assert not q5[0].spool.exists(C)
    f.article(C, "n2", 9, NEW)                           # tardío: se ignora
    f.eof(C)
    bus.drain()
    assert results(bus)[1] == []


def test_mensaje_invalido_no_tira_la_etapa(tmp_path):
    bus, _, _, f = build(tmp_path)
    bus.send(TOK_EX, b"\x00\x00")
    f.article(C, "o", 2, OLD)
    bus.drain()


def test_batch_con_articulo_nulo_o_periodo_invalido_no_rompe(tmp_path):
    bus, stages, _, f = build(tmp_path, lm=1, A=1, K=1)
    f.batch(C, [["a", "en", OLD, None, None], ["b", "en", "xx", 3, ["w"]],
                ["c", "en", NEW, 7, ["w"] * 7], ["d", "en", OLD, 1, ["w"]]])
    f.eof(C)
    bus.drain()
    lm = stages[0]
    assert lm.stats["skipped_null"] == 1 and lm.stats["unknown_period"] == 1
    assert [r["url"] for r in results(bus)[0]] == ["c"]


def test_lm_manda_un_mensaje_por_batch_y_particion(tmp_path):
    bus, _, _, f = build(tmp_path, lm=1, A=1, K=2)
    f.batch(C, [[f"u{i}", "en", NEW, 5, ["w"] * 5] for i in range(20)])
    bus.drain()
    msgs = [(k, decode(b)) for ex, k, b in bus.sent if ex == Q5_PEX and decode(b)["type"] != "eof"]
    assert len(msgs) <= 2
    assert sorted(u for _, m in msgs for u, _n in m["articles"]) == sorted(f"u{i}" for i in range(20))
    for k, m in msgs:
        assert all(k == str(partition(u, 2)) for u, _n in m["articles"])
        assert m["msg_id"]                                # id determinista para deduplicar


def test_los_msg_id_de_lm_son_deterministas(tmp_path):
    """Reprocesar el mismo batch (reentrega tras una caída) produce exactamente los mismos ids."""
    def ids():
        bus, _, _, f = build(tmp_path / f"r{len(list(tmp_path.iterdir()))}", lm=1, A=1, K=2)
        f.batch(C, [["o", "en", OLD, 3, ["w"] * 3], ["n", "en", NEW, 6, ["w"] * 6]])
        bus.drain()
        return sorted(decode(b)["msg_id"] for ex, k, b in bus.sent if ex in (AVG_PEX, Q5_PEX))
    assert ids() == ids()


# --- Q5Filter solo: spool persistente y reanudable -----------------------------------------------
def q5_alone(tmp_path, chunk_rows=500):
    bus = InMemoryBus()
    q5 = Q5Filter(bus, 0, 1, tmp_path / "s", chunk_rows=chunk_rows)
    q5.start()
    return bus, q5


def article(bus, url, n, batch_id=0, client=C):
    msg = q5_articles_batch_message(client, [[url, n]], batch_id, origin=0)
    msg["msg_id"] = f"{client[0]}.{client[1]}/lm5/{batch_id}/0"
    bus.send(Q5_PEX, encode(msg), key="0")


def average(bus, value, client=C):
    bus.send(AVG_EX, encode(average_ready_message(client, value)))


def q5_eof(bus, client=C):
    bus.send(Q5_PEX, encode(eof_message(client, origin=0)), key="0")


def test_articulos_despues_del_promedio_no_pasan_por_el_spool(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    average(bus, 4.0)
    bus.drain()
    article(bus, "mayor", 9, 0)
    article(bus, "igual", 4, 1)                          # estricto: no entra
    article(bus, "menor", 2, 2)
    bus.drain()
    assert not q5.spool.exists(C)
    q5_eof(bus)
    bus.drain()
    rows, eofs = results(bus)
    assert [r["url"] for r in rows] == ["mayor"] and eofs == [(5, 0)]


def test_el_spool_se_conserva_hasta_terminar_y_despues_se_borra(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    article(bus, "a", 8, 0)
    article(bus, "b", 3, 1)
    bus.drain()
    assert q5.spool.exists(C)
    average(bus, 5.0)
    bus.drain()
    assert q5.spool.exists(C)                            # se conserva: permite reemitir tras una caída
    assert [r["url"] for r in results(bus)[0]] == ["a"] and results(bus)[1] == []
    q5_eof(bus)
    bus.drain()
    assert results(bus)[1] == [(5, 0)] and not q5.spool.exists(C)


def test_antes_y_despues_del_promedio_da_lo_mismo_que_todo_antes(tmp_path):
    bus, _ = q5_alone(tmp_path)
    article(bus, "p1", 10, 0)
    article(bus, "p2", 1, 1)
    bus.drain()
    average(bus, 5.0)
    article(bus, "p3", 7, 2)
    article(bus, "p4", 5, 3)
    bus.drain()
    q5_eof(bus)
    bus.drain()
    rows, _ = results(bus)
    assert sorted((r["url"], r["abstract_word_count"]) for r in rows) == [("p1", 10), ("p3", 7)]


def test_promedio_none_no_emite_filas_pero_termina(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    article(bus, "a", 8, 0)
    average(bus, None)
    article(bus, "b", 9, 1)
    q5_eof(bus)
    bus.drain()
    rows, eofs = results(bus)
    assert rows == [] and eofs == [(5, 0)] and not q5.spool.exists(C)


def test_estado_limpio_al_terminar(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    average(bus, 3.0)
    article(bus, "a", 9, 0)
    q5_eof(bus)
    bus.drain()
    ck = client_key(C)
    assert ck not in q5.average and ck not in q5.emitted and ck in q5.done
    assert not q5.spool.exists(C) and not q5.eofs.complete(C)


def test_promedio_duplicado_no_vuelve_a_emitir_filas(tmp_path):
    bus, _ = q5_alone(tmp_path)
    article(bus, "a", 9, 0)
    average(bus, 3.0)
    average(bus, 3.0)
    q5_eof(bus)
    bus.drain()
    ids = [decode(b).get("msg_id") for b in bus.bodies(RESULTS_EX) if decode(b)["type"] == "result"]
    assert len(ids) == 1 and results(bus)[0] == [{"url": "a", "abstract_word_count": 9}]


def test_q5_batch_antes_del_promedio_va_al_spool_y_se_filtra_despues(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    msg = q5_articles_batch_message(C, [["a", 9], ["b", 2], ["c", 5]], 0, origin=0)
    msg["msg_id"] = "x"
    bus.send(Q5_PEX, encode(msg), key="0")
    bus.drain()
    assert q5.spool.exists(C)
    average(bus, 4.0)
    q5_eof(bus)
    bus.drain()
    assert sorted(r["url"] for r in results(bus)[0]) == ["a", "c"]
    assert not q5.spool.exists(C)


def test_reentrega_de_un_bloque_no_duplica_el_spool(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    article(bus, "a", 9, 0)
    article(bus, "a", 9, 0)                              # reentrega con el mismo msg_id
    bus.drain()
    assert len(q5.spool.read(C)) == 1
