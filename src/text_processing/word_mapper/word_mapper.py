import logging

from common.batch_stage import BatchStage
from common.bus import RabbitBus
from common.ft.ids import msg_id
from common.protocol.messages import eof_message, word_counts_message, with_id
from common.runner import broker_host, env_int, run_stage, state_dir
from common.topology import TOK_EX, WM_CTRL, WM_IN, WORDS_EX, WORDS_Q, partition, queue_of

log = logging.getLogger(__name__)

EN, FR = "en", "fr"


class WordMapper(BatchStage):
    name = "word_mapper"
    ctrl_exchange = WM_CTRL
    input_queue = WM_IN
    batch_types = ("tokens_batch",)
    emit_all = True

    #Guarda P y stats, y llama a BatchStage.__init__
    def __init__(self, bus, replica_id: int, upstream_replicas: int, word_partitions: int,
                 flush_every: int = None, state_dir=None):
        self.P = word_partitions
        self.stats = {"articles": 0, "skipped_null": 0, "unknown_lang": 0}
        super().__init__(bus, replica_id, state_dir)

    #Declara una cola por partición con su clave
    def declare_outputs(self) -> None:
        for p in range(self.P):
            self.bus.declare(queue_of(WORDS_Q, p), WORDS_EX, [str(p)])

    #Escucha WM_IN sobre TOK_EX
    def bind_input(self) -> None:
        self.bus.listen(WM_IN, self.handler(self._on_data), TOK_EX, kind="fanout")

    #Cuenta palabras por idioma solo de ese batch, las agrupa por partición y publica
    def process_batch(self, client_id, message: dict) -> None:
        batch_id = message.get("batch_id", 0)
        counts = {}
        for _url, lang, _period, _n, words in message["articles"]:
            if words is None:
                self.stats["skipped_null"] += 1
                continue
            if lang not in (EN, FR):
                self.stats["unknown_lang"] += 1
                log.warning("word_mapper: idioma desconocido %r", lang)
                continue
            col = 0 if lang == EN else 1
            for word in set(words):
                counts.setdefault(word, [0, 0])[col] += 1
            self.stats["articles"] += 1
        by_partition = {}
        for word, pair in counts.items():
            by_partition.setdefault(partition(word, self.P), {})[word] = pair
        for p, part in by_partition.items():
            out = word_counts_message(part, client_id, origin=self.replica_id)
            self.publish(WORDS_EX, with_id(out, msg_id(client_id, "wm", batch_id, p)), key=str(p))

    #Por cada partición publica un EOF con origin = replica_id y msg_id
    def publish_eof(self, client_id, total: int) -> None:
        for p in range(self.P):
            eof = eof_message(client_id, origin=self.replica_id)
            self.publish(WORDS_EX, with_id(eof, msg_id(client_id, "wm-eof", self.replica_id, p)), key=str(p))
        log.info("word_mapper[%d]: cliente %s terminado", self.replica_id, client_id)


def main() -> None:
    rid = env_int("REPLICA_ID", alt="ID")
    run_stage(WordMapper(RabbitBus(broker_host()), rid, env_int("CLEANER_REPLICAS"),
                         env_int("WORD_PARTITIONS"), state_dir=state_dir(f"word_mapper_{rid}")))


if __name__ == "__main__":
    main()
