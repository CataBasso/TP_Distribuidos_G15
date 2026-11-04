import logging
import os
import re
import signal

from common.middleware.middleware_rabbitmq import (
    RabbitConnection, MessageMiddlewareQueueRabbitMQ, MessageMiddlewareExchangeRabbitMQ,
)
from common.protocol import internal, messages
from common.stopwords.stopwords import STOP_WORDS
from common.topology import STAGE_ABSTRACT_CLEANER

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
RESULTS_EXCHANGE = "tok_ex"
AC_CONTROL_EXCHANGE = "ac_ctrl"
ABORT_EXCHANGE = "abort_ex"
STAGE = STAGE_ABSTRACT_CLEANER

WORD = re.compile(r"\w+")
NUMBER = re.compile(r"\d+")

def clean_abstract(abstract):
    if not isinstance(abstract, str):
        return None

    return [w for w in WORD.findall(abstract.lower())
            if w not in STOP_WORDS and not NUMBER.fullmatch(w)]

class AbstractCleaner:
    def __init__(self):
        self.id = ID
        self.rc = RabbitConnection(MOM_HOST)
        self.input_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE, connection=self.rc)
        self.results = MessageMiddlewareExchangeRabbitMQ(MOM_HOST, RESULTS_EXCHANGE, [], exchange_type="fanout", connection=self.rc)
        self.control = MessageMiddlewareExchangeRabbitMQ(MOM_HOST, AC_CONTROL_EXCHANGE, [], exchange_type="fanout", connection=self.rc)
        self.abort = MessageMiddlewareExchangeRabbitMQ(MOM_HOST, ABORT_EXCHANGE, [], exchange_type="fanout", connection=self.rc)

        self.count = {}        
        self.eof_total = {}    
        self.reported = {}     
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
                logging.warning("tipo inesperado en norm_in: %s", message["type"])
        ack()

    def _on_batch(self, client_id, message):
        batch_id = message["batch_id"]
        articles = []
        for row in message["rows"]:
            try:
                url, lang, period = row["url"], row["lang"], row["period"]
                words = clean_abstract(row["abstract"])
            except (KeyError, TypeError) as e:
                logging.warning("fila inválida (%s: %s), se descarta", type(e).__name__, e)
                continue
            articles.append([url, lang, period, None if words is None else len(words), None if words is None else list(set(words))])
        if articles:
            self._send(messages.tokens_batch_message(articles, client_id, batch_id, origin=self.id))

        self.count[client_id] = self.count.get(client_id, 0) + 1
        if client_id in self.eof_total:
            self._broadcast(client_id)

    def _on_eof(self, client_id, message):
        if "total" not in message:
            raise ValueError("el EOF de norm_in tiene que tener el total de batches")
        self._learn_total(client_id, message["total"])

    def _send(self, message):
        self.results.send(internal.serialize(message))

    def _broadcast(self, client_id):
        self.control.send(internal.serialize(messages.control_message(client_id, STAGE, self.id, self.count.get(client_id, 0), total=self.eof_total.get(client_id))))

    def _learn_total(self, client_id, total):
        if client_id not in self.eof_total:
            self.eof_total[client_id] = total
            self._broadcast(client_id)

    def on_control(self, body, ack, nack):
        message = internal.deserialize(body)
        client_id = messages.client_id_of(message)
        if message["stage"] == STAGE and client_id not in self.done and client_id not in self.aborted:
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
        self._send(messages.stage_eof_message(client_id, STAGE, self.id, total=total))

    def on_abort(self, body, ack, nack):
        client_id = messages.client_id_of(internal.deserialize(body))
        self.aborted.add(client_id)
        for state in (self.count, self.eof_total, self.reported):
            state.pop(client_id, None)
        self.done.discard(client_id)
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
        for name, endpoint in [("input", self.input_queue), ("results", self.results),
                               ("control", self.control), ("abort", self.abort)]:
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
    AbstractCleaner().start()
    return 0

if __name__ == "__main__":
    main()
