"""LengthMapper -> AvgReducer -> Q5Filter con un InMemoryBus (sin RabbitMQ)."""
import pytest

from common.bus import InMemoryBus
from common.protocol import decode
from common.protocol.messages import abort_message, encode
from common.topology import (
    ABORT_EX, AVG_EX, AVG_PEX, PERIOD_FROM_2021, PERIOD_UP_TO_2020, Q5_PEX, RESULTS_EX,
    TOK_EX, client_key, partition,
)
from filters.q5_filter.q5_filter import Q5Filter
from text_processing.avg_reducer.avg_reducer import AvgReducer
from text_processing.length_mapper.length_mapper import LengthMapper

from fakes import cleaner_eof, tokens

C = (1, 1)
OLD, NEW = PERIOD_UP_TO_2020, PERIOD_FROM_2021


def words(n):
    return [f"w{i}" for i in range(n)]


def build(tmp_path, lm=2, cleaners=2, A=2, K=2):
    bus = InMemoryBus()
    stages = [LengthMapper(bus, r, cleaners, A, K, flush_every=3) for r in range(lm)]
    stages += [AvgReducer(bus, a, lm, K) for a in range(A)]
    q5 = [Q5Filter(bus, k, lm, tmp_path / f"q5_{k}") for k in range(K)]
    stages += q5
    for s in stages:
        s.start()
    return bus, stages, q5


def feed(bus, bodies):
    for b in bodies:
        bus.send(TOK_EX, b)


def eof_all(bus, client, cleaners=2):
    for o in range(cleaners):
        bus.send(TOK_EX, cleaner_eof(client, origin=o))


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
    bus, _, _ = build(tmp_path)
    # <=2020: n = 2, 4, 6 -> promedio 4.0
    feed(bus, [tokens(C, f"old{n}", words(n), OLD, batch_id=n) for n in (2, 4, 6)])
    # >=2021: 3 (no), 4 (igual al promedio: no, estricto), 5 (si), 9 (si)
    feed(bus, [tokens(C, f"new{n}", words(n), NEW, batch_id=10 + n) for n in (3, 4, 5, 9)])
    eof_all(bus, C)
    bus.drain()
    rows, eofs = results(bus)
    assert sorted((r["url"], r["abstract_word_count"]) for r in rows) == [("new5", 5), ("new9", 9)]
    assert sorted(eofs) == [(5, 0), (5, 1)]          # un EOF por partición K


def test_abstract_nulo_no_cuenta_en_el_promedio(tmp_path):
    bus, _, _ = build(tmp_path)
    feed(bus, [tokens(C, "a", words(10), OLD), tokens(C, "b", None, OLD),
               tokens(C, "n1", words(9), NEW), tokens(C, "n2", words(11), NEW)])
    eof_all(bus, C)
    bus.drain()
    rows, _ = results(bus)
    assert [r["url"] for r in rows] == ["n2"]        # promedio 10 (no 5)


def test_sin_articulos_hasta_2020_no_hay_filas_pero_termina(tmp_path):
    bus, _, _ = build(tmp_path)
    feed(bus, [tokens(C, "n1", words(9), NEW)])
    eof_all(bus, C)
    bus.drain()
    rows, eofs = results(bus)
    assert rows == [] and len(eofs) == 2
    ready = [decode(b) for b in bus.bodies(AVG_EX)]
    assert ready[0]["average"] is None


def test_average_ready_llega_despues_de_los_articulos_y_el_spool_se_borra(tmp_path):
    bus, _, q5 = build(tmp_path, K=1, A=1)
    feed(bus, [tokens(C, "n1", words(8), NEW)])
    bus.drain()
    spool = q5[0].spool
    assert spool.exists(C)                           # el artículo espera en disco
    feed(bus, [tokens(C, "o", words(4), OLD)])
    eof_all(bus, C)
    bus.drain()
    assert not spool.exists(C)
    assert [r["url"] for r in results(bus)[0]] == ["n1"]


def test_clientes_independientes(tmp_path):
    bus, _, _ = build(tmp_path)
    c1, c2 = (1, 1), (1, 2)
    feed(bus, [tokens(c1, "o1", words(2), OLD), tokens(c2, "o2", words(20), OLD),
               tokens(c1, "x1", words(5), NEW), tokens(c2, "x2", words(5), NEW),
               tokens(c2, "y2", words(30), NEW)])
    eof_all(bus, c1)
    eof_all(bus, c2)
    bus.drain()
    assert [r["url"] for r in results(bus, c1)[0]] == ["x1"]
    assert [r["url"] for r in results(bus, c2)[0]] == ["y2"]


def test_longitud_mapper_agrupa_por_batch(tmp_path):
    bus, _, _ = build(tmp_path, lm=1, cleaners=1, A=1, K=1)
    feed(bus, [tokens(C, f"u{i}", words(i + 1), OLD, batch_id=0) for i in range(4)]
         + [tokens(C, "v", words(10), OLD, batch_id=1)])
    bus.send(TOK_EX, cleaner_eof(C))
    bus.drain()
    data = [decode(b) for b in bus.bodies(AVG_PEX) if decode(b)["type"] == "average_data"]
    # flush_every=3 corta el batch 0 en (3 artículos) + (1); luego el batch 1
    assert [(d["batch_id"], d["sum"], d["count"]) for d in data] == [(0, 6, 3), (0, 4, 1), (1, 10, 1)]


def test_la_particion_de_promedio_depende_solo_del_cliente(tmp_path):
    bus, _, _ = build(tmp_path, A=3)
    feed(bus, [tokens(C, f"u{i}", words(2), OLD, batch_id=i) for i in range(5)])
    eof_all(bus, C)
    bus.drain()
    keys = {k for ex, k, b in bus.sent if ex == AVG_PEX}
    assert keys == {str(partition(client_key(C), 3))}


def test_q5_particiona_por_url(tmp_path):
    bus, _, _ = build(tmp_path, K=3)
    feed(bus, [tokens(C, f"url{i}", words(3), NEW) for i in range(20)])
    bus.drain()
    for ex, k, b in bus.sent:
        m = decode(b)
        if ex == Q5_PEX and m["type"] == "q5_article":
            assert k == str(partition(m["url"], 3))
        if ex == Q5_PEX and m["type"] == "q5_articles_batch":
            assert all(k == str(partition(url, 3)) for url, _n in m["articles"])


def test_lm_no_termina_hasta_tener_los_eof_de_todos_los_cleaners(tmp_path):
    bus, _, _ = build(tmp_path)
    feed(bus, [tokens(C, "o", words(2), OLD), tokens(C, "n", words(5), NEW)])
    bus.send(TOK_EX, cleaner_eof(C, origin=0))       # falta el EOF del cleaner 1
    bus.drain()
    assert results(bus)[1] == []
    bus.send(TOK_EX, cleaner_eof(C, origin=1))
    bus.drain()
    assert len(results(bus)[1]) == 2


def test_eof_duplicado_no_adelanta_la_terminacion(tmp_path):
    bus, _, _ = build(tmp_path)
    feed(bus, [tokens(C, "o", words(2), OLD)])
    bus.send(TOK_EX, cleaner_eof(C, origin=0))
    bus.send(TOK_EX, cleaner_eof(C, origin=0))       # mismo origen, dos veces
    bus.drain()
    assert results(bus)[1] == []


def test_chunks_de_resultado(tmp_path):
    bus = InMemoryBus()
    stages = [LengthMapper(bus, 0, 1, 1, 1), AvgReducer(bus, 0, 1, 1),
              Q5Filter(bus, 0, 1, tmp_path / "s", chunk_rows=2)]
    for s in stages:
        s.start()
    feed(bus, [tokens(C, "o", words(1), OLD)]
         + [tokens(C, f"n{i}", words(5), NEW) for i in range(5)])
    bus.send(TOK_EX, cleaner_eof(C))
    bus.drain()
    sizes = [len(decode(b)["rows"]) for b in bus.bodies(RESULTS_EX) if decode(b)["type"] == "result"]
    assert sizes == [2, 2, 1]


def test_abort_libera_estado_y_spool(tmp_path):
    bus, stages, q5 = build(tmp_path, K=1, A=1)
    feed(bus, [tokens(C, "n1", words(8), NEW), tokens(C, "o", words(2), OLD)])
    bus.drain()
    assert q5[0].spool.exists(C)
    bus.send(ABORT_EX, encode(abort_message(C)))
    bus.drain()
    assert not q5[0].spool.exists(C)
    feed(bus, [tokens(C, "n2", words(9), NEW)])      # tardío: se ignora
    eof_all(bus, C)
    bus.drain()
    assert results(bus)[1] == []


def test_mensaje_invalido_no_tira_la_etapa(tmp_path):
    bus, _, _ = build(tmp_path)
    bus.send(TOK_EX, b"\x00\x00")
    feed(bus, [tokens(C, "o", words(2), OLD)])
    bus.drain()


def test_length_mapper_acepta_stage_eof_con_total_como_manda_el_resto_del_sistema(tmp_path):
    """El YearFilter y los demás mandan stage_eof (con total) entre etapas, no eof."""
    from common.protocol.messages import stage_eof_message
    bus, _, _ = build(tmp_path, cleaners=1)
    feed(bus, [tokens(C, "o", words(4), OLD), tokens(C, "n", words(9), NEW)])
    bus.send(TOK_EX, encode(stage_eof_message(C, "abstract_cleaner", 0, total=1)))
    bus.drain()
    assert [r["url"] for r in results(bus)[0]] == ["n"]
    assert len(results(bus)[1]) == 2


# --- camino rápido: con el promedio ya conocido no se escribe en disco ---------------------------
from common.protocol.messages import average_ready_message, eof_message, q5_article_message  # noqa: E402


def q5_alone(tmp_path, chunk_rows=500):
    bus = InMemoryBus()
    q5 = Q5Filter(bus, 0, 1, tmp_path / "s", chunk_rows=chunk_rows)
    q5.start()
    return bus, q5


def article(bus, url, n, client=C):
    bus.send(Q5_PEX, encode(q5_article_message(client, url, n, 0, origin=0)), key="0")


def average(bus, value, client=C):
    bus.send(AVG_EX, encode(average_ready_message(client, value)))


def q5_eof(bus, client=C):
    bus.send(Q5_PEX, encode(eof_message(client, origin=0)), key="0")


def test_articulos_despues_del_promedio_no_pasan_por_el_spool(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    average(bus, 4.0)
    bus.drain()
    article(bus, "mayor", 9)
    article(bus, "igual", 4)             # estricto: no entra
    article(bus, "menor", 2)
    bus.drain()
    assert not q5.spool.exists(C)        # nunca se creó el archivo
    q5_eof(bus)
    bus.drain()
    rows, eofs = results(bus)
    assert [r["url"] for r in rows] == ["mayor"] and eofs == [(5, 0)]


def test_el_spool_se_vacia_y_se_borra_apenas_llega_el_promedio(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    article(bus, "a", 8)
    article(bus, "b", 3)
    bus.drain()
    assert q5.spool.exists(C)
    average(bus, 5.0)                    # todavía faltan los EOF: el cliente no terminó
    bus.drain()
    assert not q5.spool.exists(C)        # pero el archivo ya se borró
    assert results(bus)[1] == []         # sin EOF de Q5 todavía
    q5_eof(bus)
    bus.drain()
    rows, eofs = results(bus)
    assert [r["url"] for r in rows] == ["a"] and eofs == [(5, 0)]


def test_antes_y_despues_del_promedio_da_lo_mismo_que_todo_antes(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    article(bus, "p1", 10)
    article(bus, "p2", 1)
    bus.drain()
    average(bus, 5.0)
    article(bus, "p3", 7)                # llegan después del promedio
    article(bus, "p4", 5)
    bus.drain()
    q5_eof(bus)
    bus.drain()
    rows, _ = results(bus)
    assert sorted((r["url"], r["abstract_word_count"]) for r in rows) == [("p1", 10), ("p3", 7)]


def test_promedio_none_no_emite_filas_pero_termina(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    article(bus, "a", 8)
    average(bus, None)
    article(bus, "b", 9)                 # después del promedio None: tampoco entra
    q5_eof(bus)
    bus.drain()
    rows, eofs = results(bus)
    assert rows == [] and eofs == [(5, 0)] and not q5.spool.exists(C)


def test_chunks_con_camino_rapido(tmp_path):
    bus, _ = q5_alone(tmp_path, chunk_rows=2)
    average(bus, 1.0)
    for i in range(5):
        article(bus, f"n{i}", 5)
    q5_eof(bus)
    bus.drain()
    sizes = [len(decode(b)["rows"]) for b in bus.bodies(RESULTS_EX) if decode(b)["type"] == "result"]
    assert sizes == [2, 2, 1]


def test_estado_limpio_al_terminar(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    average(bus, 3.0)
    article(bus, "a", 9)
    q5_eof(bus)
    bus.drain()
    assert C not in q5._average and C not in q5._rows and C not in q5._sent
    assert not q5.spool.exists(C) and not q5.eofs.complete(C)


def test_promedio_duplicado_antes_de_terminar_no_vuelve_a_emitir(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    article(bus, "a", 9)
    average(bus, 3.0)
    average(bus, 3.0)                    # duplicado: el spool ya se vació, no se emite dos veces
    q5_eof(bus)
    bus.drain()
    assert [r["url"] for r in results(bus)[0]] == ["a"]


# --- batching: LengthMapper -> Q5Filter manda un mensaje por batch y por partición ----------------
from common.protocol.messages import q5_articles_batch_message, tokens_batch_message  # noqa: E402


def batch_body(articles, client=C, batch_id=0):
    """articles: [(url, period, n_words)] -> tokens_batch como lo manda el AbstractCleaner."""
    rows = [[url, "en", period, n, [f"w{i}" for i in range(n)]] for url, period, n in articles]
    return encode(tokens_batch_message(rows, client, batch_id, origin=0))


def test_lm_manda_un_mensaje_por_batch_y_particion_no_uno_por_articulo(tmp_path):
    bus, _, _ = build(tmp_path, lm=1, cleaners=1, A=1, K=2)
    bus.send(TOK_EX, batch_body([(f"u{i}", NEW, 5) for i in range(20)]))
    bus.drain()
    msgs = [(k, decode(b)) for ex, k, b in bus.sent if ex == Q5_PEX and decode(b)["type"] != "eof"]
    assert all(m["type"] == "q5_articles_batch" for _, m in msgs)
    assert len(msgs) <= 2                                         # una por partición, no 20
    assert sorted(url for _, m in msgs for url, _n in m["articles"]) == sorted(f"u{i}" for i in range(20))
    for k, m in msgs:
        assert all(k == str(partition(url, 2)) for url, _n in m["articles"])


def test_batch_mixto_acumula_promedio_y_manda_q5(tmp_path):
    bus, _, _ = build(tmp_path, lm=1, cleaners=1, A=1, K=1)
    bus.send(TOK_EX, batch_body([("o1", OLD, 2), ("o2", OLD, 4), ("n1", NEW, 3), ("n2", NEW, 9)]))
    bus.send(TOK_EX, cleaner_eof(C, origin=0))
    bus.drain()
    assert [r["url"] for r in results(bus)[0]] == ["n2"]          # promedio = 3, estrictamente mayor
    assert results(bus)[1] != []


def test_batch_con_articulo_nulo_o_periodo_invalido_no_rompe(tmp_path):
    bus, stages, _ = build(tmp_path, lm=1, cleaners=1, A=1, K=1)
    rows = [["a", "en", OLD, None, None], ["b", "en", "xx", 3, ["w"]], ["c", "en", NEW, 7, ["w"] * 7],
            ["d", "en", OLD, 1, ["w"]]]
    bus.send(TOK_EX, encode(tokens_batch_message(rows, C, 0, origin=0)))
    bus.send(TOK_EX, cleaner_eof(C, origin=0))
    bus.drain()
    lm = stages[0]
    assert lm.stats["skipped_null"] == 1 and lm.stats["unknown_period"] == 1
    assert [r["url"] for r in results(bus)[0]] == ["c"]


def q5_batch(bus, articles, client=C):
    bus.send(Q5_PEX, encode(q5_articles_batch_message(client, [list(a) for a in articles], 0, origin=0)), key="0")


def test_q5_batch_antes_del_promedio_va_al_spool_y_se_filtra_despues(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    q5_batch(bus, [("a", 9), ("b", 2), ("c", 5)])
    bus.drain()
    assert q5.spool.exists(C)
    average(bus, 4.0)
    q5_eof(bus)
    bus.drain()
    assert sorted(r["url"] for r in results(bus)[0]) == ["a", "c"]
    assert not q5.spool.exists(C)


def test_q5_batch_despues_del_promedio_no_pasa_por_el_spool(tmp_path):
    bus, q5 = q5_alone(tmp_path)
    average(bus, 4.0)
    q5_batch(bus, [("a", 9), ("b", 2), ("c", 5)])
    bus.drain()
    assert not q5.spool.exists(C)
    q5_eof(bus)
    bus.drain()
    assert sorted(r["url"] for r in results(bus)[0]) == ["a", "c"]
