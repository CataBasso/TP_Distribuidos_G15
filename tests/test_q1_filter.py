import pytest

from common.bus import InMemoryBus
from common.ft import failpoints
from common.protocol.messages import batch_message, decode, encode, eof_message
from common.topology import RESULTS_EX
from filters.q1_filter.query1 import Q1Filter, rows_in_english

C = (1, 7)


def row(lang, url="u"):
    return {"lang": lang, "url": url, "date_modified": "2020-01-01", "extra": 1}


def feed(bus, *messages):
    for m in messages:
        bus.send("", encode(m), key="q1_in")


def results(bus):
    return [decode(b) for ex, _k, b in bus.sent if ex == RESULTS_EX]


def new_stage(tmp_path=None, bus=None, rid=0):
    bus = bus or InMemoryBus()
    stage = Q1Filter(bus, rid, "q1_in", str(tmp_path / f"r{rid}") if tmp_path else None)
    stage.start()
    return stage, bus


def test_rows_in_english_conserva_solo_ingles_y_dos_columnas():
    assert rows_in_english([row("en", "a"), row("es", "b")]) == [{"date_modified": "2020-01-01", "url": "a"}]


def test_publica_un_resultado_por_batch_incluso_vacio_y_un_solo_eof():
    stage, bus = new_stage()
    feed(bus, batch_message(C, 0, [row("en")]), batch_message(C, 1, [row("es")]),
         eof_message(C, 0, total=2))
    bus.drain()
    out = results(bus)
    assert [m["type"] for m in out] == ["result", "result", "eof"]
    assert out[0]["rows"] == [{"date_modified": "2020-01-01", "url": "u"}] and out[1]["rows"] == []
    assert out[2]["query"] == 1
    assert len({m["msg_id"] for m in out}) == 3


def test_msg_id_es_determinista_entre_replicas():
    ids = []
    for rid in (0, 1):
        stage, bus = new_stage(rid=rid)
        feed(bus, batch_message(C, 5, [row("en")]))
        bus.drain()
        ids.append(results(bus)[0]["msg_id"])
    assert ids[0] == ids[1]


def test_eof_antes_que_los_batches_espera_a_la_cobertura():
    stage, bus = new_stage()
    feed(bus, eof_message(C, 0, total=2), batch_message(C, 0, [row("en")]))
    bus.drain()
    assert [m["type"] for m in results(bus)] == ["result"]
    feed(bus, batch_message(C, 1, [row("en")]))
    bus.drain()
    assert [m["type"] for m in results(bus)] == ["result", "result", "eof"]


def test_reentrega_de_un_batch_no_adelanta_el_eof():
    stage, bus = new_stage()
    feed(bus, batch_message(C, 0, [row("en")]), batch_message(C, 0, [row("en")]), eof_message(C, 0, total=2))
    bus.drain()
    assert "eof" not in [m["type"] for m in results(bus)]


@pytest.mark.parametrize("point", ["before_persist", "after_persist", "after_publish", "before_ack"])
def test_caida_y_reinicio_no_pierde_ni_cierra_de_menos(tmp_path, point):
    msgs = [batch_message(C, 0, [row("en")]), batch_message(C, 1, [row("en")]),
            eof_message(C, 0, total=2)]
    stage, bus = new_stage(tmp_path)
    failpoints.set_handler(lambda: (_ for _ in ()).throw(failpoints.SimulatedCrash()))
    try:
        failpoints.configure(f"{point}:2")
        feed(bus, *msgs)
        with pytest.raises(failpoints.SimulatedCrash):
            bus.drain()
    finally:
        failpoints.set_handler(None)
        failpoints.configure("")
    published = results(bus)
    stage2, bus2 = new_stage(tmp_path)              
    feed(bus2, *msgs[1:])                            
    bus2.drain()
    final = published + results(bus2)
    by_id = {m["msg_id"]: m for m in final}
    assert sorted(m["type"] for m in by_id.values()) == ["eof", "result", "result"]
