"""Broker en memoria con la misma interfaz que gateway.broker.RabbitGatewayBroker."""
import queue
import threading

from common.protocol import decode, encode


class InMemoryBroker:
    def __init__(self):
        self.year_in = queue.Queue()        
        self.aborts = queue.Queue()         
        self.results_q = queue.Queue()      
        self.acked = 0
        self._stop = threading.Event()

    def open_publisher(self):
        return _Publisher(self)

    def run_results(self, on_message):
        def ack():
            self.acked += 1
        while not self._stop.is_set():
            try:
                body = self.results_q.get(timeout=0.05)
            except queue.Empty:
                continue
            on_message(body, ack, lambda requeue=True: None)

    def defer(self, fn):
        fn()

    def stop_results(self):
        self._stop.set()

    def publish_result(self, message: dict):
        self.results_q.put(encode(message))

    def finish(self, client_id, expected=None, skip=()):
        expected = expected or {1: 1, 2: 1, 3: 1, 4: 1, 5: 1}
        for query, count in expected.items():
            for origin in range(count):
                if (query, origin) not in skip:
                    self.publish_result({"type": "eof", "client_id": client_id,
                                         "origin": origin, "query": query})

    def get_year(self, timeout=5) -> dict:
        return decode(self.year_in.get(timeout=timeout))

    def get_abort(self, timeout=5) -> dict:
        return decode(self.aborts.get(timeout=timeout))


class _Publisher:
    def __init__(self, broker):
        self.broker = broker
        self.closed = False

    def send_year(self, body):
        self.broker.year_in.put(body)

    def send_abort(self, body):
        self.broker.aborts.put(body)

    def close(self):
        self.closed = True


from common.bus import InMemoryBus  
from common.protocol.messages import ( 
    abort_message, eof_message, result_message, tokens_message,
)
from common.topology import RESULTS_EX, TOK_EX, queue_of 


def tokens(client_id, url, words, period, batch_id=0, origin=0):
    m = tokens_message(url, "en", words, period, client_id, origin=origin)
    m["batch_id"] = batch_id
    return encode(m)


def cleaner_eof(client_id, origin=0):
    return encode(eof_message(client_id, origin=origin))


def query_eof(client_id, query, origin=0):
    m = eof_message(client_id, origin=origin)
    m["query"] = query
    return encode(m)


class Feeder:
    """Arma el flujo del AbstractCleaner: batches numerados 0..n-1 por cliente y un EOF con el total."""

    def __init__(self, bus):
        self.bus, self.n = bus, {}

    def batch(self, client, rows):
        """rows: [[url, lang, period, n_words, words]] -> un tokens_batch con el próximo batch_id."""
        from common.protocol.messages import tokens_batch_message
        b = self.n.get(client, 0)
        self.n[client] = b + 1
        self.bus.send(TOK_EX, encode(tokens_batch_message(rows, client, b, origin=0)))
        return b

    def article(self, client, url, n_words, period, lang="en"):
        words = None if n_words is None else [f"w{i}" for i in range(n_words)]
        return self.batch(client, [[url, lang, period, n_words, words]])

    def eof(self, client, total=None):
        from common.protocol.messages import stage_eof_message
        total = self.n.get(client, 0) if total is None else total
        self.bus.send(TOK_EX, encode(stage_eof_message(client, "abstract_cleaner", 0, total=total)))
