import logging

from common.protocol.messages import (
    average_data_message, encode, eof_message, q5_article_message, stage_eof_message,
)
from common.stage import EofCounter, Stage
from common.topology import (
    AVG_IN, AVG_PEX, LM_CTRL, LM_IN, PERIOD_FROM_2021, PERIOD_UP_TO_2020, Q5_IN, Q5_PEX,
    STAGE_ABSTRACT_CLEANER, TOK_EX, client_key, partition, queue_of,
)
from common.bus import RabbitBus
from common.runner import broker_host, env_int, env_str, run_stage

log = logging.getLogger(__name__)


class LengthMapper(Stage):
    name = "length_mapper"

    def __init__(self, bus, replica_id: int, upstream_replicas: int, avg_partitions: int,
                 q5_partitions: int, flush_every: int = 1000):
        super().__init__(bus)
        self.replica_id = replica_id
        self.A, self.K = avg_partitions, q5_partitions
        self.flush_every = flush_every
        self.eofs = EofCounter(upstream_replicas)
        self._acc = {}
        self._sent = {}
        self.stats = {"articles": 0, "skipped_null": 0, "unknown_period": 0}

    def start(self) -> None:
        for a in range(self.A):
            self.bus.declare(queue_of(AVG_IN, a), AVG_PEX, [str(a)])
        for k in range(self.K):
            self.bus.declare(queue_of(Q5_IN, k), Q5_PEX, [str(k)])
        self.bus.listen(LM_IN, self.handler(self._on_input), TOK_EX, kind="fanout")
        self.bus.listen(None, self.handler(self._on_ctrl), LM_CTRL, kind="fanout")
        self.listen_abort()

    def _on_input(self, message: dict) -> None:
        if message["type"] == "tokens":
            self._on_tokens(message)
        elif message["type"] == "tokens_batch":
            client_id, batch_id = tuple(message["client_id"]), message.get("batch_id", 0)
            for url, _lang, period, n, _words in message["articles"]:
                self._on_article(client_id, url, period, n, batch_id)
        elif message["type"] in ("eof", "stage_eof"):
            ctrl = stage_eof_message(message["client_id"], STAGE_ABSTRACT_CLEANER,
                                     message.get("origin", 0))
            self.bus.send(LM_CTRL, encode(ctrl))
        else:
            log.warning("length_mapper: tipo inesperado %s", message["type"])

    def _on_tokens(self, message: dict) -> None:
        words = message["words"]
        self._on_article(tuple(message["client_id"]), message["url"], message["period"], None if words is None else len(words), message.get("batch_id", 0))

    def _on_article(self, client_id, url, period, n, batch_id) -> None:
        if n is None:
            self.stats["skipped_null"] += 1
            return
        if period == PERIOD_UP_TO_2020:
            self._accumulate(client_id, n, batch_id)
        elif period == PERIOD_FROM_2021:
            body = encode(q5_article_message(client_id, url, n, batch_id, origin=self.replica_id))
            self.bus.send(Q5_PEX, body, key=str(partition(url, self.K)))
        else:
            self.stats["unknown_period"] += 1
            log.warning("length_mapper: período desconocido %r", period)
            return
        self.stats["articles"] += 1

    def _accumulate(self, client_id, n: int, batch_id) -> None:
        acc = self._acc.get(client_id)
        if acc is not None and acc[2] != batch_id:
            self._flush(client_id)
            acc = None
        if acc is None:
            acc = self._acc[client_id] = [0, 0, batch_id]
        acc[0] += n
        acc[1] += 1
        if acc[1] >= self.flush_every:
            self._flush(client_id)

    def _flush(self, client_id) -> None:
        acc = self._acc.pop(client_id, None)
        if acc is None or acc[1] == 0:
            return
        total, count, batch_id = acc
        body = encode(average_data_message(client_id, batch_id, total, count,
                                           origin=self.replica_id))
        self._sent[client_id] = self._sent.get(client_id, 0) + 1
        self.bus.send(AVG_PEX, body, key=str(partition(client_key(client_id), self.A)))

    #Coordinacion de EOFs
    def _on_ctrl(self, message: dict) -> None:
        if message["type"] != "stage_eof" or message["stage"] != STAGE_ABSTRACT_CLEANER:
            return
        client_id = tuple(message["client_id"])
        if self.eofs.add(client_id, message["origin"]):
            self._finish(client_id)

    def _finish(self, client_id) -> None:
        self._flush(client_id)
        avg_eof = eof_message(client_id, origin=self.replica_id, total=self._sent.pop(client_id, 0))
        self.bus.send(AVG_PEX, encode(avg_eof), key=str(partition(client_key(client_id), self.A)))
        eof = encode(eof_message(client_id, origin=self.replica_id))
        for k in range(self.K):
            self.bus.send(Q5_PEX, eof, key=str(k))
        self.eofs.forget(client_id)
        log.info("length_mapper[%d]: cliente %s terminado", self.replica_id, client_id)

    def on_abort(self, client_id) -> None:
        self._acc.pop(client_id, None)
        self._sent.pop(client_id, None)
        self.eofs.forget(client_id)


def main() -> None:
    bus = RabbitBus(broker_host())
    run_stage(LengthMapper(bus, env_int("REPLICA_ID", alt="ID"), env_int("CLEANER_REPLICAS"),
                           env_int("AVG_PARTITIONS"), env_int("Q5_PARTITIONS"),
                           env_int("FLUSH_EVERY", 1000)))


if __name__ == "__main__":
    main()
