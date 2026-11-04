import logging

from common.protocol.messages import encode, eof_message, stage_eof_message, word_counts_message
from common.stage import EofCounter, Stage
from common.topology import (
    STAGE_ABSTRACT_CLEANER, TOK_EX, WM_CTRL, WM_IN, WORDS_EX, WORDS_Q, partition, queue_of,
)
from common.bus import RabbitBus
from common.runner import broker_host, env_int, run_stage

log = logging.getLogger(__name__)

EN, FR = "en", "fr"

class WordMapper(Stage):
    name = "word_mapper"

    def __init__(self, bus, replica_id: int, upstream_replicas: int, word_partitions: int, flush_every: int = 1000):
        super().__init__(bus)
        self.replica_id = replica_id
        self.P = word_partitions
        self.flush_every = flush_every
        self.eofs = EofCounter(upstream_replicas)
        self._acc = {}      
        self._pending = {} 
        self.stats = {"articles": 0, "skipped_null": 0, "unknown_lang": 0}

    def start(self) -> None:
        for p in range(self.P):
            self.bus.declare(queue_of(WORDS_Q, p), WORDS_EX, [str(p)])
        self.bus.listen(WM_IN, self.handler(self._on_input), TOK_EX, kind="fanout")
        self.bus.listen(None, self.handler(self._on_ctrl), WM_CTRL, kind="fanout")
        self.listen_abort()

    def _on_input(self, message: dict) -> None:
        if message["type"] == "tokens":
            self._on_article(tuple(message["client_id"]), message["lang"], message["words"])
        elif message["type"] == "tokens_batch":
            client_id = tuple(message["client_id"])
            for _url, lang, _period, _n, words in message["articles"]:
                self._on_article(client_id, lang, words)
        elif message["type"] in ("eof", "stage_eof"):
            ctrl = stage_eof_message(message["client_id"], STAGE_ABSTRACT_CLEANER,message.get("origin", 0))
            self.bus.send(WM_CTRL, encode(ctrl))
        else:
            log.warning("word_mapper: tipo inesperado %s", message["type"])

    def _on_article(self, client_id, lang, words) -> None:
        if words is None:
            self.stats["skipped_null"] += 1
            return
        if lang not in (EN, FR):
            self.stats["unknown_lang"] += 1
            log.warning("word_mapper: idioma desconocido %r", lang)
            return
        counts = self._acc.setdefault(client_id, {})
        col = 0 if lang == EN else 1
        for word in set(words):  
            counts.setdefault(word, [0, 0])[col] += 1
        self.stats["articles"] += 1
        self._pending[client_id] = self._pending.get(client_id, 0) + 1
        if self._pending[client_id] >= self.flush_every:
            self._flush(client_id)

    def _flush(self, client_id) -> None:
        counts = self._acc.pop(client_id, None)
        self._pending.pop(client_id, None)
        if not counts:
            return
        by_partition = {}
        for word, pair in counts.items():
            by_partition.setdefault(partition(word, self.P), {})[word] = pair
        for p, part in by_partition.items():
            body = encode(word_counts_message(part, client_id, origin=self.replica_id))
            self.bus.send(WORDS_EX, body, key=str(p))

    def _on_ctrl(self, message: dict) -> None:
        if message["type"] != "stage_eof" or message["stage"] != STAGE_ABSTRACT_CLEANER:
            return
        client_id = tuple(message["client_id"])
        if self.eofs.add(client_id, message["origin"]):
            self._finish(client_id)

    def _finish(self, client_id) -> None:
        self._flush(client_id)
        eof = encode(eof_message(client_id, origin=self.replica_id))
        for p in range(self.P):
            self.bus.send(WORDS_EX, eof, key=str(p))
        self.eofs.forget(client_id)
        log.info("word_mapper[%d]: cliente %s terminado", self.replica_id, client_id)

    def on_abort(self, client_id) -> None:
        self._acc.pop(client_id, None)
        self._pending.pop(client_id, None)
        self.eofs.forget(client_id)


def main() -> None:
    bus = RabbitBus(broker_host())
    run_stage(WordMapper(bus, env_int("REPLICA_ID", alt="ID"), env_int("CLEANER_REPLICAS"),env_int("WORD_PARTITIONS"), env_int("FLUSH_EVERY", 1000)))


if __name__ == "__main__":
    main()
