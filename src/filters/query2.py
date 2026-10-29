import logging
import os
import signal

from common.middleware.middleware_rabbitmq import (
    RabbitConnection,
    MessageMiddlewareQueueRabbitMQ,
    MessageMiddlewareExchangeRabbitMQ,
)
from common.protocol import internal, messages

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q2_CONTROL_EXCHANGE = "q2_ctrl"
ABORT_EXCHANGE = "abort_ex"
STAGE = "q2"
BIOGRAPHY_SECTIONS = {"biography", "biographie"}  


def biography_rows(rows):
    """Filas de q2_in ({name, url, sections, image_url}) -> filas de img_in
    ({name, url, image_url}) solo de los artículos con una sección de biografía."""
    result = []
    for row in rows:
        try:
            if any(str(s).strip().lower() in BIOGRAPHY_SECTIONS for s in row["sections"]):
                result.append({"name": row["name"], "url": row["url"],
                               "image_url": row["image_url"]})
        except (KeyError, TypeError) as e:
            logging.warning("fila inválida (%s: %s), se descarta", type(e).__name__, e)
    return result


class Q2Filter:
    """Lee batches de q2_in, deja los artículos con sección de biografía y los publica en
    img_in. Las réplicas se coordinan por q2_ctrl para saber cuándo procesaron todos los
    batches del cliente."""

    def __init__(self):
        self.id = ID
        self.rc = RabbitConnection(MOM_HOST)
        self.input = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE, connection=self.rc)
        self.output = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE, connection=self.rc)
        self.control = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, Q2_CONTROL_EXCHANGE, [], exchange_type="fanout", connection=self.rc)
        self.abort = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, ABORT_EXCHANGE, [], exchange_type="fanout", connection=self.rc)

        self.count = {}         
        self.eof_total = {}    
        self.reported = {}      
        self.holds_eof = set()  
        self.done = set()
        self.aborted = set()

    def on_data(self, body, ack, nack):
        message = internal.deserialize(body)
        client_id = messages.client_id_of(message)
        if client_id not in self.aborted:
            if message["type"] == "batch":
                self._on_batch(client_id, message)
            elif message["type"] in ("eof", "stage_eof"):
                self._on_eof(client_id, message)
            else:
                logging.warning("tipo inesperado en q2_in: %s", message["type"])
        ack()

    def _on_batch(self, client_id, message):
        rows = biography_rows(message["rows"])
        self.output.send(internal.serialize(messages.batch_message(
            client_id, message["batch_id"], rows, origin=self.id)))
        self.count[client_id] = self.count.get(client_id, 0) + 1
        if client_id in self.eof_total:
            self._broadcast(client_id)

    def _on_eof(self, client_id, message):
        if "total" not in message:
            raise ValueError("el EOF de q2_in debe traer el total de batches")
        self.holds_eof.add(client_id)
        self._learn_total(client_id, message["total"])

    def _broadcast(self, client_id):
        self.control.send(internal.serialize(messages.control_message(
            client_id, STAGE, self.id, self.count.get(client_id, 0),
            total=self.eof_total.get(client_id))))

    def _learn_total(self, client_id, total):
        if client_id not in self.eof_total:
            self.eof_total[client_id] = total
            self._broadcast(client_id)

    def on_control(self, body, ack, nack):
        message = internal.deserialize(body)
        client_id = messages.client_id_of(message)
        if client_id not in self.done and client_id not in self.aborted:
            if message.get("total") is not None:
                self._learn_total(client_id, message["total"])
            reported = self.reported.setdefault(client_id, {})
            replica = message["replica"]
            reported[replica] = max(reported.get(replica, 0), message["count"])
            total = self.eof_total.get(client_id)
            if total is not None and sum(reported.values()) >= total:
                self._flush(client_id)
        ack()

    def _flush(self, client_id):
        total = self.eof_total.pop(client_id)
        self.count.pop(client_id, None)
        self.reported.pop(client_id, None)
        self.done.add(client_id)
        if client_id in self.holds_eof:  
            self.holds_eof.discard(client_id)
            self.output.send(internal.serialize(messages.stage_eof_message(
                client_id, STAGE, self.id, total=total)))

    def on_abort(self, body, ack, nack):
        client_id = messages.client_id_of(internal.deserialize(body))
        self.aborted.add(client_id)
        for state in (self.count, self.eof_total, self.reported):
            state.pop(client_id, None)
        self.holds_eof.discard(client_id)
        ack()

    def setup_consumers(self):
        self.control.consume(self.on_control)
        self.abort.consume(self.on_abort)
        self.input.consume(self.on_data)

    def start(self):
        signal.signal(signal.SIGTERM, lambda signum, frame: self.rc.stop())
        self.setup_consumers()
        try:
            self.rc.run()
        finally:
            self.close()

    def close(self):
        pass

def main():
    logging.basicConfig(level=logging.INFO)
    q2_filter = Q2Filter()
    q2_filter.start()
    return 0


if __name__ == "__main__":
    main()