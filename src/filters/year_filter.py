import os
import logging
import signal
from common.middleware.middleware_rabbitmq import (
    RabbitConnection, MessageMiddlewareQueueRabbitMQ, MessageMiddlewareExchangeRabbitMQ,
)
from common.protocol import internal, messages
from filters.year_logic import split_rows

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
Q1_QUEUE = os.environ["Q1_QUEUE"]
Q2_QUEUE = os.environ["Q2_QUEUE"]
NORM_QUEUE = os.environ["NORM_QUEUE"]
YEAR_CONTROL_EXCHANGE = "year_ctrl"
ABORT_EXCHANGE = "abort_ex"
STAGE = "year"

class YearFilter:
    def __init__(self):
        self.id = ID
        self.rc = RabbitConnection(MOM_HOST)
        self.input_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE, connection=self.rc)
        self.outputs = [
            MessageMiddlewareQueueRabbitMQ(MOM_HOST, q, connection=self.rc)
            for q in (Q1_QUEUE, Q2_QUEUE, NORM_QUEUE)
        ]
        self.control = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, YEAR_CONTROL_EXCHANGE, [], exchange_type="fanout", connection=self.rc)
        self.abort = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, ABORT_EXCHANGE, [], exchange_type="fanout", connection=self.rc)
        
        self.count = {} # client_id -> batches procesados por esta réplica
        self.eof_total = {}    # client_id -> total de batches (cuando se conoce)
        self.reported = {}     # client_id -> {replica: batches procesados}
        self.holds_eof = set() # clientes cuyo EOF consumió esta réplica
        self.done = set()
        self.aborted = set()

    def on_data(self, body, ack, nack):
        message = internal.deserialize(body)
        client_id = messages.client_id_of(message)

        if client_id not in self.aborted:
            if message["type"] == "batch":
                self._on_batch(client_id, message)
            elif message["type"] == "eof":
                self._on_eof(client_id, message)
            else:
                logging.warning("tipo inesperado en year_in: %s", message["type"])

        ack()

    def _on_batch(self, client_id, message):
        parts = split_rows(message["rows"])
        for out, rows in zip(self.outputs, parts):
            out.send(internal.serialize(messages.batch_message(client_id, message["batch_id"], rows, origin=self.id)))

        self.count[client_id] = self.count.get(client_id, 0) + 1
        if client_id in self.eof_total:
            self._broadcast(client_id)

    def _on_eof(self, client_id, message):
        self.holds_eof.add(client_id)
        self._learn_total(client_id, message["total"])

    def _broadcast(self, client_id):
        self.control.send(internal.serialize(messages.control_message(client_id, STAGE, self.id, self.count.get(client_id, 0), total=self.eof_total.get(client_id))))

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
            reported[replica] = max(reported.get(replica,0), message["count"])
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
            eof = internal.serialize(messages.stage_eof_message(
                client_id, STAGE, self.id, total=total))
            for out in self.outputs:
                out.send(eof)

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
        self.input_queue.consume(self.on_data)
 
    def start(self):
        signal.signal(signal.SIGTERM, lambda signum, frame: self.rc.stop())
        self.setup_consumers()
        try:
            self.rc.run()
        finally:
            self.close()

    def close(self):
        for name, endpoint in [("input", self.input_queue), ("control", self.control),
                               ("abort", self.abort)] + \
                              [(f"output_{i}", o) for i, o in enumerate(self.outputs)]:
            try:
                endpoint.close()
            except Exception as e:
                logging.error(f"Error al cerrar '{name}': {e}")
        try:
            self.rc.close()
        except Exception as e:
            logging.error(f"Error al cerrar la conexión: {e}")
 
 
def main():
    logging.basicConfig(level=logging.INFO)
    year_filter = YearFilter()
    year_filter.start()
    return 0
 

if __name__ == "__main__":
    main()