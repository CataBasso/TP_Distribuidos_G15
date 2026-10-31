import logging

from common.protocol.messages import RESULT_COLUMNS, done_message, encode
from common.stage import Stage
from common.topology import (
    OUT_Q, RESULTS_EX, RESULTS_Q, expected_result_eofs, queue_of,
)

log = logging.getLogger(__name__)


class Joiner(Stage):
    name = "joiner"

    def __init__(self, bus, gw_id: int, word_partitions: int, q5_partitions: int):
        super().__init__(bus)
        self.gw_id = gw_id
        self.expected = expected_result_eofs(word_partitions, q5_partitions)
        self._eofs = {}
        self._done = set()
        self.out_queue = queue_of(OUT_Q, gw_id)

    def start(self) -> None:
        self.bus.declare(self.out_queue)
        self.bus.listen(queue_of(RESULTS_Q, self.gw_id), self._callback, RESULTS_EX,
                        [str(self.gw_id)])
        self.listen_abort()

    def _callback(self, body, ack, nack):
        self.handler(lambda message: self._on_message(message, body))(body, ack, nack)

    def _on_message(self, message: dict, body: bytes) -> None:
        client_id = tuple(message.get("client_id", ()))
        if len(client_id) != 2 or client_id[0] != self.gw_id:
            log.warning("joiner[%d]: mensaje de un cliente ajeno %s, se descarta",
                        self.gw_id, client_id)
            return
        if client_id in self._done:
            return
        kind = message["type"]
        if kind == "result":
            self.bus.send("", body, key=self.out_queue)
        elif kind == "eof":
            self._on_eof(client_id, message)
        else:
            log.warning("joiner[%d]: tipo inesperado %s", self.gw_id, kind)

    def _on_eof(self, client_id, message: dict) -> None:
        query = message.get("query")
        if query not in RESULT_COLUMNS:
            log.warning("joiner[%d]: eof sin consulta válida (%r)", self.gw_id, query)
            return
        per_query = self._eofs.setdefault(client_id, {})
        per_query.setdefault(query, set()).add(message.get("origin", 0))
        if all(len(per_query.get(q, ())) >= n for q, n in self.expected.items()):
            self.bus.send("", encode(done_message(client_id)), key=self.out_queue)
            self._done.add(client_id)
            del self._eofs[client_id]
            log.info("joiner[%d]: cliente %s completo -> done", self.gw_id, client_id)

    def pending(self, client_id):
        """Qué EOF faltan de un cliente: {query: (recibidos, esperados)}. Sirve para depurar."""
        per_query = self._eofs.get(tuple(client_id), {})
        return {q: (len(per_query.get(q, ())), n) for q, n in self.expected.items()}

    def on_abort(self, client_id) -> None:
        self._eofs.pop(client_id, None)
        self._done.discard(client_id)


def main() -> None:
    """Variables: RABBIT_HOST, GW_ID, WORD_PARTITIONS (P), Q5_PARTITIONS (K)."""
    from common.bus import RabbitBus
    from common.runner import broker_host, env_int, env_str, run_stage
    bus = RabbitBus(broker_host())
    run_stage(Joiner(bus, env_int("GW_ID", alt="ID"), env_int("WORD_PARTITIONS"), env_int("Q5_PARTITIONS")))


if __name__ == "__main__":
    main()
