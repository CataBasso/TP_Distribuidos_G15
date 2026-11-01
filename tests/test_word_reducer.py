"""WordMapper -> WordReducer con un InMemoryBus (sin RabbitMQ)."""
from common.bus import InMemoryBus
from common.protocol import decode
from common.protocol.messages import encode, eof_message
from common.topology import RESULTS_EX, TOK_EX, TOP_IN
from text_processing.word_mapper.word_mapper import WordMapper
from text_processing.word_reducer.word_reducer import WordReducer

from fakes import cleaner_eof, tokens

C = (1, 1)
P = 2


def build(mappers=2, cleaners=1, chunk_rows=500, top_n=20):
    bus = InMemoryBus()
    stages = [WordMapper(bus, r, cleaners, P) for r in range(mappers)]
    stages += [WordReducer(bus, p, mappers, chunk_rows, top_n) for p in range(P)]
    for s in stages:
        s.start()
    return bus, stages


def article(bus, url, words, lang):
    m = decode(tokens(C, url, words, "<=2020"))
    m["lang"] = lang
    bus.send(TOK_EX, encode(m))


def finish(bus):
    bus.send(TOK_EX, cleaner_eof(C, 0))
    bus.drain()


def q3(bus):
    rows, eofs = [], []
    for b in bus.bodies(RESULTS_EX):
        m = decode(b)
        if m["type"] == "result" and m["query"] == 3:
            rows += [r["word"] for r in m["rows"]]
        elif m["type"] == "eof" and m["query"] == 3:
            eofs.append(m["origin"])
    return sorted(rows), sorted(eofs)


def tops(bus):
    out, eofs = {}, []
    for b in bus.bodies("", TOP_IN):
        m = decode(b)
        if m["type"] == "partial_top":
            out.update(m["counts"])
        else:
            eofs.append(m["origin"])
    return out, sorted(eofs)


def test_q3_only_words_seen_in_english_and_french():
    bus, _ = build()
    article(bus, "u1", ["cat", "dog", "shared"], "en")
    article(bus, "u2", ["chat", "shared"], "fr")
    finish(bus)
    rows, eofs = q3(bus)
    assert rows == ["shared"]
    assert eofs == list(range(P))        # un EOF por partición


def test_q4_local_tops_merge_into_the_global_top():
    bus, _ = build(top_n=2)
    article(bus, "u1", ["a", "b", "c"], "en")
    article(bus, "u2", ["a", "b"], "en")
    article(bus, "u3", ["a"], "fr")
    finish(bus)
    top, eofs = tops(bus)
    assert eofs == list(range(P))
    assert sorted(top.items(), key=lambda t: (-t[1], t[0]))[:2] == [("a", 3), ("b", 2)]


def test_waits_for_every_mapper_replica_before_emitting():
    bus = InMemoryBus()
    r = WordReducer(bus, 0, 2)
    r.start()
    r._on_input({"type": "word_counts", "counts": {"x": [1, 1]}, "client_id": C, "origin": 0})
    r._on_input(eof_message(C, origin=0))
    assert not bus.bodies(RESULTS_EX)
    r._on_input(eof_message(C, origin=1))
    assert q3(bus) == (["x"], [0])


def test_q3_rows_are_chunked_and_abort_drops_state():
    bus, stages = build(chunk_rows=1)
    article(bus, "u1", ["a", "b", "c"], "en")
    article(bus, "u2", ["a", "b", "c"], "fr")
    finish(bus)
    chunks = [decode(b) for b in bus.bodies(RESULTS_EX) if decode(b)["type"] == "result"]
    assert all(len(m["rows"]) == 1 for m in chunks) and len(chunks) == 3
    for s in stages:
        s.on_abort(C)
    assert not any(C in getattr(s, "_words", {}) for s in stages)


def test_top_merger_end_to_end_global_top_and_eof():
    from text_processing.top_merger.top_merger import TopMerger
    bus, stages = build(top_n=2)
    tm = TopMerger(bus, P, top_n=2)
    tm.start()
    article(bus, "u1", ["a", "b", "c"], "en")
    article(bus, "u2", ["a", "b"], "en")
    article(bus, "u3", ["a"], "fr")
    finish(bus)
    msgs = [decode(b) for b in bus.bodies(RESULTS_EX)]
    rows = [r for m in msgs if m["type"] == "result" and m["query"] == 4 for r in m["rows"]]
    assert rows == [{"word": "a", "count": 3}, {"word": "b", "count": 2}]
    assert [m for m in msgs if m["type"] == "eof" and m["query"] == 4]


def test_top_merger_waits_for_every_partition_and_abort_drops_state():
    from common.protocol.messages import partial_top_message
    from text_processing.top_merger.top_merger import TopMerger
    bus = InMemoryBus()
    tm = TopMerger(bus, 2)
    tm.start()
    tm._on_input(partial_top_message({"a": 1}, C, origin=0))
    tm._on_input(eof_message(C, origin=0))
    assert not bus.bodies(RESULTS_EX)
    tm.on_abort(C)
    assert C not in tm._tops
