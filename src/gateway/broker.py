from common.middleware import (
    RabbitConnection, MessageMiddlewareQueueRabbitMQ, MessageMiddlewareExchangeRabbitMQ,
)

YEAR_QUEUE = "year_in"
ABORT_EXCHANGE = "abort_ex"


def out_queue_name(gw_id: int) -> str:
    return f"out_q.{gw_id}"


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
                 durable=True):
        self.host, self.gw_id = host, gw_id
        self.year_queue, self.abort_exchange, self.durable = year_queue, abort_exchange, durable
        self._results_rc = None
        self._stopped = False

    def open_publisher(self):
        return _RabbitPublisher(self.host, self.year_queue, self.abort_exchange, self.durable)

    def run_results(self, on_message) -> None:
        if self._stopped:
            return
        self._results_rc = RabbitConnection(self.host)
        out_q = MessageMiddlewareQueueRabbitMQ(self.host, out_queue_name(self.gw_id),
                                               connection=self._results_rc, durable=self.durable)
        try:
            out_q.consume(on_message)
            self._results_rc.run()
        finally:
            self._results_rc.close()

    def stop_results(self) -> None:
        self._stopped = True
        if self._results_rc is not None:
            self._results_rc.stop()
