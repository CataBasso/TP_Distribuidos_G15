"""WordMapper con un InMemoryBus (sin RabbitMQ)."""
from common.bus import InMemoryBus
from common.protocol import decode
from common.topology import WORDS_EX, partition
from text_processing.word_mapper.word_mapper import WordMapper

from fakes import Feeder

C = (1, 1)
P = 3
OLD = "<=2020"


def build(replicas=2):
    bus = InMemoryBus()
    stages = [WordMapper(bus, r, 1, P) for r in range(replicas)]
    for s in stages:
        s.start()
    return bus, stages, Feeder(bus)


def sent(bus):
    return [(k, decode(b)) for ex, k, b in bus.sent if ex == WORDS_EX]


def test_partials_go_to_the_partition_of_each_word_and_eof_from_every_replica():
    bus, _, f = build()
    f.batch(C, [["u1", "en", OLD, 2, ["cat", "dog"]]])
    f.eof(C)
    bus.drain()
    msgs = sent(bus)
    counts = {}
    for key, m in msgs:
        if m["type"] == "word_counts":
            for w, pair in m["counts"].items():
                assert key == str(partition(w, P))
                counts[w] = pair
    assert counts == {"cat": [1, 0], "dog": [1, 0]}
    eofs = [(k, m["origin"]) for k, m in msgs if m["type"] == "eof"]
    assert sorted(eofs) == sorted((str(p), r) for p in range(P) for r in range(2))


def test_no_eof_until_every_batch_is_covered():
    bus, _, f = build()
    f.batch(C, [["u1", "en", OLD, 1, ["cat"]]])
    f.eof(C, total=2)
    bus.drain()
    assert not [m for _, m in sent(bus) if m["type"] == "eof"]


def test_counts_words_once_per_abstract_and_split_by_language():
    bus, _, f = build(replicas=1)
    f.batch(C, [["u1", "en", OLD, 2, ["cat", "cat"]], ["u2", "fr", OLD, 1, ["cat"]]])
    f.eof(C)
    bus.drain()
    cat = [m["counts"]["cat"] for _, m in sent(bus) if m["type"] == "word_counts" and "cat" in m["counts"]]
    assert cat == [[1, 1]]


def test_null_abstracts_are_skipped_and_abort_drops_state():
    from common.protocol.messages import abort_message, encode
    from common.topology import ABORT_EX
    bus, stages, f = build()
    f.batch(C, [["u1", "en", OLD, None, None], ["u2", "en", OLD, 1, ["cat"]]])
    bus.drain()
    assert sum(s.stats["skipped_null"] for s in stages) == 1
    bus.send(ABORT_EX, encode(abort_message(C)))
    bus.drain()
    assert all(s.eof.is_closed(C) for s in stages)


def test_los_msg_id_son_deterministas_entre_reprocesos():
    def ids():
        bus, _, f = build(replicas=1)
        f.batch(C, [["u1", "en", OLD, 2, ["cat", "dog"]]])
        bus.drain()
        return sorted(m["msg_id"] for _, m in sent(bus) if m["type"] == "word_counts")
    assert ids() == ids() and ids()
