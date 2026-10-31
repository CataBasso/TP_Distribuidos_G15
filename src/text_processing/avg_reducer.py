import logging

from common.protocol.messages import average_ready_message, encode
from common.stage import EofCounter, Stage
from common.topology import AVG_EX, AVG_IN, AVG_PEX, AVG_READY_Q, queue_of

log = logging.getLogger(__name__)


class AvgReducer(Stage):
    name = "avg_reducer"

    def __init__(self, bus, partition_id: int, upstream_replicas: int, q5_partitions: int = 0):
        super().__init__(bus)
        self.q5_partitions = q5_partitions
        self.partition_id = partition_id
        self.eofs = EofCounter(upstream_replicas)
        self._state = {}           

    def start(self) -> None:
        for k in range(self.q5_partitions):
            self.bus.declare(queue_of(AVG_READY_Q, k), AVG_EX, kind="fanout")
        self.bus.listen(queue_of(AVG_IN, self.partition_id), self.handler(self._on_input),
                        AVG_PEX, [str(self.partition_id)])
        self.listen_abort()

    def _on_input(self, message: dict) -> None:
        client_id = tuple(message["client_id"])
        if message["type"] == "average_data":
            state = self._state.setdefault(client_id, [0, 0])
            state[0] += message["sum"]
            state[1] += message["count"]
        elif message["type"] == "eof":
            if self.eofs.add(client_id, message.get("origin", 0)):
                self._publish(client_id)
        else:
            log.warning("avg_reducer: tipo inesperado %s", message["type"])

    def _publish(self, client_id) -> None:
        total, count = self._state.pop(client_id, [0, 0])
        average = total / count if count else None
        self.bus.send(AVG_EX, encode(average_ready_message(client_id, average)))
        self.eofs.forget(client_id)
        log.info("avg_reducer[%d]: cliente %s promedio=%s (%d artículos)",
                 self.partition_id, client_id, average, count)

    def on_abort(self, client_id) -> None:
        self._state.pop(client_id, None)
        self.eofs.forget(client_id)


def main() -> None:
    from common.bus import RabbitBus
    from common.runner import broker_host, env_int, env_str, run_stage
    bus = RabbitBus(broker_host())
    run_stage(AvgReducer(bus, env_int("PARTITION_ID", alt="ID"), env_int("LM_REPLICAS"),
                         env_int("Q5_PARTITIONS")))


if __name__ == "__main__":
    main()
