"""WordMapper con un InMemoryBus (sin RabbitMQ)."""
from common.bus import InMemoryBus
from common.protocol import decode
from common.protocol.messages import encode
from common.topology import TOK_EX, WORDS_EX, partition
from text_processing.word_mapper.word_mapper import WordMapper

from fakes import cleaner_eof, tokens

C = (1, 1)
P = 3


def build(replicas=2, cleaners=2, flush_every=1000):
    bus = InMemoryBus()
    stages = [WordMapper(bus, r, cleaners, P, flush_every) for r in range(replicas)]
    for s in stages:
        s.start()
    return bus, stages


def sent(bus):
    return [(k, decode(b)) for ex, k, b in bus.sent if ex == WORDS_EX]


def test_partials_go_to_the_partition_of_each_word_and_eof_to_all():
    bus, _ = build()
    bus.send(TOK_EX, tokens(C, "u1", ["cat", "dog"], "<=2020"))
    bus.send(TOK_EX, cleaner_eof(C, 0))
    bus.send(TOK_EX, cleaner_eof(C, 1))
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


def test_no_eof_until_every_cleaner_replica_finished():
    bus, _ = build()
    bus.send(TOK_EX, tokens(C, "u1", ["cat"], "<=2020"))
    bus.send(TOK_EX, cleaner_eof(C, 0))
    bus.drain()
    assert not [m for _, m in sent(bus) if m["type"] in ("eof", "word_counts")]


def test_counts_words_once_per_abstract_and_split_by_language():
    bus, _ = build(replicas=1)
    bus.send(TOK_EX, tokens(C, "u1", ["cat", "cat"], "<=2020"))
    for url, words in (("u2", ["cat"]),):
        m = decode(tokens(C, url, words, "<=2020"))
        m["lang"] = "fr"
        bus.send(TOK_EX, encode(m))
    bus.send(TOK_EX, cleaner_eof(C, 0))
    bus.send(TOK_EX, cleaner_eof(C, 1))
    bus.drain()
    cat = [m["counts"]["cat"] for _, m in sent(bus)
           if m["type"] == "word_counts" and "cat" in m["counts"]]
    assert cat == [[1, 1]]


def test_null_abstracts_are_skipped_and_abort_drops_state():
    bus, stages = build()
    bus.send(TOK_EX, tokens(C, "u1", None, "<=2020"))
    bus.send(TOK_EX, tokens(C, "u2", ["cat"], "<=2020"))
    bus.drain()
    assert sum(s.stats["skipped_null"] for s in stages) == 1
    for s in stages:
        s.on_abort(C)
        assert C not in s._acc
