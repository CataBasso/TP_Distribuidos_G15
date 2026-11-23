import logging

from common.batch_stage import BatchStage
from common.bus import RabbitBus
from common.ft.ids import msg_id
from common.protocol.messages import (
    average_data_message, eof_message, q5_articles_batch_message, with_id,
)
from common.runner import broker_host, env_int, run_stage, state_dir
from common.topology import (
    AVG_IN, AVG_PEX, LM_CTRL, LM_IN, PERIOD_FROM_2021, PERIOD_UP_TO_2020, Q5_IN, Q5_PEX, TOK_EX,
    client_key, partition, queue_of,
)

log = logging.getLogger(__name__)


class LengthMapper(BatchStage):
    name = "length_mapper"
    ctrl_exchange = LM_CTRL
    input_queue = LM_IN
    batch_types = ("tokens_batch",)
    emit_all = True

    #Guarda A, K y stats, y llama a BatchStage.__init__
    def __init__(self, bus, replica_id: int, upstream_replicas: int, avg_partitions: int,
                 q5_partitions: int, flush_every: int = None, state_dir=None):
        self.A, self.K = avg_partitions, q5_partitions
        self.stats = {"articles": 0, "skipped_null": 0, "unknown_period": 0}
        super().__init__(bus, replica_id, state_dir)

    #Declara una cola por partición con su clave
    def declare_outputs(self) -> None:
        for a in range(self.A):
            self.bus.declare(queue_of(AVG_IN, a), AVG_PEX, [str(a)])
        for k in range(self.K):
            self.bus.declare(queue_of(Q5_IN, k), Q5_PEX, [str(k)])

    #Escucha LM_IN sobre TOK_EX
    def bind_input(self) -> None:
        self.bus.listen(LM_IN, self.handler(self._on_data), TOK_EX, kind="fanout")

    #Cuenta artículos por período y longitud, los agrupa por partición y publica
    def process_batch(self, client_id, message: dict) -> None:
        batch_id = message.get("batch_id", 0)
        total = count = 0
        by_partition = {}
        for url, _lang, period, n, _words in message["articles"]:
            if n is None:
                self.stats["skipped_null"] += 1
            elif period == PERIOD_UP_TO_2020:
                total += n
                count += 1
                self.stats["articles"] += 1
            elif period == PERIOD_FROM_2021:
                by_partition.setdefault(partition(url, self.K), []).append([url, n])
                self.stats["articles"] += 1
            else:
                self.stats["unknown_period"] += 1
                log.warning("length_mapper: período desconocido %r", period)
        if count:
            a = partition(client_key(client_id), self.A)
            out = average_data_message(client_id, batch_id, total, count, origin=self.replica_id)
            self.publish(AVG_PEX, with_id(out, msg_id(client_id, "lm", batch_id, a)), key=str(a))
        for k, rows in by_partition.items():
            out = q5_articles_batch_message(client_id, rows, batch_id, origin=self.replica_id)
            self.publish(Q5_PEX, with_id(out, msg_id(client_id, "lm5", batch_id, k)), key=str(k))

    #Manda un EOF a la partición de AvgReducer del cliente y otro a cada partición de Q5, con origin=replica_id
    def publish_eof(self, client_id, total: int) -> None:
        a = partition(client_key(client_id), self.A)
        eof = eof_message(client_id, origin=self.replica_id)
        self.publish(AVG_PEX, with_id(eof, msg_id(client_id, "lm-avg-eof", self.replica_id, a)), key=str(a))
        for k in range(self.K):
            eof = eof_message(client_id, origin=self.replica_id)
            self.publish(Q5_PEX, with_id(eof, msg_id(client_id, "lm-q5-eof", self.replica_id, k)), key=str(k))
        log.info("length_mapper[%d]: cliente %s terminado", self.replica_id, client_id)


def main() -> None:
    rid = env_int("REPLICA_ID", alt="ID")
    run_stage(LengthMapper(RabbitBus(broker_host()), rid, env_int("CLEANER_REPLICAS"),
                           env_int("AVG_PARTITIONS"), env_int("Q5_PARTITIONS"),
                           state_dir=state_dir(f"length_mapper_{rid}")))


if __name__ == "__main__":
    main()
