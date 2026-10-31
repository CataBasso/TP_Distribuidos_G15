"""RabbitBus con un middleware falso: verifica qué declara y publica, sin broker."""
import common.middleware as mw
from common.bus import InMemoryBus, RabbitBus

calls = []


class FakeConn:
    def __init__(self, host, prefetch=1, **kw):
        calls.append(("conn", host, prefetch))

    def run(self): calls.append(("run",))
    def stop(self): calls.append(("stop",))
    def close(self): calls.append(("close",))


class FakeExchange:
    def __init__(self, host, exchange, keys, exchange_type="direct", queue_name=None,
                 durable=True, connection=None):
        calls.append(("exchange", exchange, tuple(keys), exchange_type, queue_name, durable))

    def send(self, body, key=None): calls.append(("xsend", key, body))
    def consume(self, cb): calls.append(("xconsume",))


class FakeQueue:
    def __init__(self, host, name, connection=None, durable=True):
        self.name = name
        calls.append(("queue", name, durable))

    def send(self, body): calls.append(("qsend", self.name, body))
    def consume(self, cb): calls.append(("qconsume", self.name))


def make(monkeypatch):
    calls.clear()
    monkeypatch.setattr(mw, "RabbitConnection", FakeConn)
    monkeypatch.setattr(mw, "MessageMiddlewareExchangeRabbitMQ", FakeExchange)
    monkeypatch.setattr(mw, "MessageMiddlewareQueueRabbitMQ", FakeQueue)
    return RabbitBus("h")


def test_fanout_exchange_se_declara_fanout(monkeypatch):
    bus = make(monkeypatch)
    bus.listen(None, lambda *a: None, "abort_ex", kind="fanout")
    bus.send("tok_ex", b"x")
    assert ("exchange", "abort_ex", (), "fanout", None, True) in calls
    assert ("exchange", "tok_ex", (), "fanout", None, True) in calls
    assert ("xsend", "", b"x") in calls or ("xsend", None, b"x") in calls


def test_send_a_cola_por_defecto_y_cache_de_endpoints(monkeypatch):
    bus = make(monkeypatch)
    bus.send("", b"a", key="out_q.1")
    bus.send("", b"b", key="out_q.1")
    assert [c for c in calls if c[0] == "queue"] == [("queue", "out_q.1", True)]
    assert [c for c in calls if c[0] == "qsend"] == [("qsend", "out_q.1", b"a"), ("qsend", "out_q.1", b"b")]


def test_listen_con_particion_liga_con_la_clave(monkeypatch):
    bus = make(monkeypatch)
    bus.listen("q5_in.1", lambda *a: None, "q5_pex", ["1"])
    assert ("exchange", "q5_pex", ("1",), "direct", "q5_in.1", True) in calls


def test_inmemory_round_robin_entre_consumidores_de_la_misma_cola():
    bus = InMemoryBus()
    a, b = [], []
    bus.listen("q", lambda body, ack, nack: (a.append(body), ack()))
    bus.listen("q", lambda body, ack, nack: (b.append(body), ack()))
    for i in range(4):
        bus.send("", bytes([i]), key="q")
    bus.drain()
    assert len(a) == len(b) == 2


def test_inmemory_fanout_da_una_copia_a_cada_cola():
    bus = InMemoryBus()
    got = []
    bus.listen("c1", lambda body, ack, nack: got.append(("c1", body)), "fx", kind="fanout")
    bus.listen(None, lambda body, ack, nack: got.append(("anon", body)), "fx", kind="fanout")
    bus.send("fx", b"m")
    bus.drain()
    assert sorted(got) == [("anon", b"m"), ("c1", b"m")]
