import logging
import os
import signal
 
from common.middleware.middleware_rabbitmq import (
    RabbitConnection, MessageMiddlewareQueueRabbitMQ, MessageMiddlewareExchangeRabbitMQ,
)
from common.protocol import internal, messages
 
ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
RESULTS_EXCHANGE = "results_ex"
Q1_CONTROL_EXCHANGE = "q1_ctrl"
ABORT_EXCHANGE = "abort_ex"
STAGE = "q1"
QUERY = 1
ENGLISH = "en"  

def rows_in_english(rows):
    result = []
    for row in rows:
        try:
            if row["lang"] == ENGLISH:
                result.append({"date_modified": row["date_modified"], "url": row["url"]})
        except (KeyError, TypeError) as e:
            logging.warning("fila inválida (%s: %s), se descarta", type(e).__name__, e)
    return result

class Q1Filter:
    def __init__(self):
        self.id = ID
        self.rc = RabbitConnection(MOM_HOST)
        self.input_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE, connection=self.rc)
        self.results = MessageMiddlewareExchangeRabbitMQ(MOM_HOST, RESULTS_EXCHANGE, [], exchange_type="direct", connection=self.rc)
        self.control = MessageMiddlewareExchangeRabbitMQ(MOM_HOST, Q1_CONTROL_EXCHANGE, [], exchange_type="fanout", connection=self.rc)
        self.abort = MessageMiddlewareExchangeRabbitMQ(MOM_HOST, ABORT_EXCHANGE, [], exchange_type="fanout", connection=self.rc)

        self.count = {} 
        self.eof_total = {}    
        self.reported = {}   
        self.holds_eof = set() 
        self.done = set()
        self.aborted = set()

    def on_data(self ,body ,ack ,nack):
        message = internal.deserialize(body)
        client_id = messages.client_id_of(message)

        if client_id not in self.aborted:
            if message["type"] == "batch":
                self._on_batch(client_id, message)
            elif message["type"] in ("eof", "stage_eof"):
                self._on_eof(client_id, message)
            else:
                logging.warning("tipo inesperado en q1_in: %s", message["type"])
        ack()

    def _on_batch(self ,client_id ,message):
        parts = rows_in_english(message["rows"])
        self._send(messages.result_message(client_id, QUERY, parts, origin=self.id), client_id)

        self.count[client_id] = self.count.get(client_id,0) + 1
        if client_id in self.eof_total:
            self._broadcast(client_id)

    def _on_eof(self ,client_id ,message):
        if "total" not in message:
            raise ValueError("el EOF de q1_in debe traer el total de batches")
        self.holds_eof.add(client_id)
        self._learn_total(client_id, message["total"])

    def _send(self ,message ,client_id):
        self.results.send(internal.serialize(message), routing_key=str(client_id[0]))

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
            self._send(messages.eof_message(client_id, origin=self.id, query=QUERY, total=total), client_id)

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
        pass


def main():
    logging.basicConfig(level=logging.INFO)
    q1_filter = Q1Filter()
    q1_filter.start()
    return 0
 
if __name__ == "__main__":
    main()