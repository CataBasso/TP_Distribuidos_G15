import logging

from common.accumulator import AccumulatorStage
from common.bus import RabbitBus
from common.ft.ids import msg_id
from common.protocol.messages import average_ready_message, with_id
from common.runner import broker_host, env_int, run_stage, state_dir
from common.topology import AVG_EX, AVG_IN, AVG_PEX, AVG_READY_Q, queue_of

log = logging.getLogger(__name__)


class AvgReducer(AccumulatorStage):
    name = "avg_reducer"
    data_type = "average_data"

    #Guarda partition_id, q5_partitions y llama a la base con state_dir
    def __init__(self, bus, partition_id: int, upstream_replicas: int, q5_partitions: int = 0,
                 state_dir=None):
        self.q5_partitions = q5_partitions
        self.partition_id = partition_id
        super().__init__(bus, upstream_replicas, state_dir)

    def new_state(self):
        return [0, 0]                    

    #Lo que se guarda en el WAL
    def payload(self, message):
        return [message["sum"], message["count"]]

    #Suma al estado
    def merge(self, state, payload):
        state[0] += payload[0]
        state[1] += payload[1]

    #Declara las colas avg_ready de cada partición Q5, escucha AVG_IN y termina con recover_pending()
    def start(self) -> None:
        for k in range(self.q5_partitions):
            self.bus.declare(queue_of(AVG_READY_Q, k), AVG_EX, kind="fanout")
        self.bus.listen(queue_of(AVG_IN, self.partition_id), self.handler(self.on_message),
                        AVG_PEX, [str(self.partition_id)])
        self.listen_abort()
        self.recover_pending()

    #Calcula suma/cantidad y publica average_ready con msg_id(client, "avg", 0, partición)
    def finish(self, client_id, state) -> None:
        total, count = state or [0, 0]
        average = total / count if count else None
        self.publish(AVG_EX, with_id(average_ready_message(client_id, average),
                                     msg_id(client_id, "avg", 0, self.partition_id)))
        log.info("avg_reducer[%d]: cliente %s promedio=%s (%d artículos)",
                 self.partition_id, client_id, average, count)


def main() -> None:
    pid = env_int("PARTITION_ID", alt="ID")
    run_stage(AvgReducer(RabbitBus(broker_host()), pid, env_int("LM_REPLICAS"),
                         env_int("Q5_PARTITIONS"), state_dir(f"avg_reducer_{pid}")))


if __name__ == "__main__":
    main()
