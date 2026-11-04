import threading

from common.middleware import (
    RabbitConnection, MessageMiddlewareQueueRabbitMQ, MessageMiddlewareExchangeRabbitMQ,
)

YEAR_QUEUE = "year_in"
ABORT_EXCHANGE = "abort_ex"
RESULTS_EXCHANGE = "results_ex"      


def results_queue_name(gw_id: int) -> str:
    return f"results_q.{gw_id}"


class _RabbitPublisher:
    def __init__(self, host, year_queue, abort_exchange, durable):
        self._rc = RabbitConnection(host)
        self._year = MessageMiddlewareQueueRabbitMQ(host, year_queue, connection=self._rc,
                                                    durable=durable)
        self._abort = MessageMiddlewareExchangeRabbitMQ(host, abort_exchange, [],
                                                        exchange_type="fanout", durable=durable,
                                                        connection=self._rc)

    def send_year(self, body: bytes) -> None:
        self._year.send(body)

    def send_abort(self, body: bytes) -> None:
        self._abort.send(body)

    def close(self) -> None:
        self._rc.close()


class RabbitGatewayBroker:
    def __init__(self, host, gw_id, year_queue=YEAR_QUEUE, abort_exchange=ABORT_EXCHANGE,
                 durable=True, results_exchange=RESULTS_EXCHANGE, results_queue=None):
        self.host, self.gw_id = host, gw_id
        self.year_queue, self.abort_exchange, self.durable = year_queue, abort_exchange, durable
        self.results_exchange = results_exchange
        self.results_queue = results_queue or results_queue_name(gw_id)
        self._results_rc = None
        self._stopped = False
        self.results_ready = threading.Event()

    def open_publisher(self):
        return _RabbitPublisher(self.host, self.year_queue, self.abort_exchange, self.durable)

    def run_results(self, on_message) -> None:
        if self._stopped:
            return
        self._results_rc = RabbitConnection(self.host)
        results = MessageMiddlewareExchangeRabbitMQ(
            self.host, self.results_exchange, [str(self.gw_id)], exchange_type="direct",
            queue_name=self.results_queue, durable=self.durable, connection=self._results_rc)
        try:
            results.consume(on_message)
            self.results_ready.set()
            self._results_rc.run()
        finally:
            self._results_rc.close()

    def stop_results(self) -> None:
        self._stopped = True
        if self._results_rc is not None:
            self._results_rc.stop()
