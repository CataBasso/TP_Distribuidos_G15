"""Broker en memoria con la misma interfaz que gateway.broker.RabbitGatewayBroker."""
import queue
import threading

from common.protocol import decode, encode


class InMemoryBroker:
    def __init__(self):
        self.year_in = queue.Queue()        # cuerpos publicados en year_in
        self.aborts = queue.Queue()         # cuerpos publicados en abort_ex
        self.out_q = queue.Queue()          # cuerpos que el Joiner "publica" en out_q.<g>
        self.acked = 0
        self._stop = threading.Event()

    # --- lado del Gateway ---
    def open_publisher(self):
        return _Publisher(self)

    def run_results(self, on_message):
        def ack():
            self.acked += 1
        while not self._stop.is_set():
            try:
                body = self.out_q.get(timeout=0.05)
            except queue.Empty:
                continue
            on_message(body, ack, lambda requeue=True: None)

    def stop_results(self):
        self._stop.set()

    # --- lado de las etapas falsas ---
    def publish_result(self, message: dict):
        self.out_q.put(encode(message))

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
