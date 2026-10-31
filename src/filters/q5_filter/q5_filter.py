import logging

from common.protocol.messages import encode, eof_message, result_message
from common.stage import EofCounter, Stage
from common.topology import (
    AVG_EX, AVG_READY_Q, Q5_IN, Q5_PEX, RESULT_CHUNK_ROWS, RESULTS_EX, queue_of, results_key,
)
from common.utils.spool import Spool

log = logging.getLogger(__name__)

_NO_AVERAGE_YET = object()


class Q5Filter(Stage):
    name = "q5_filter"

    def __init__(self, bus, partition_id: int, upstream_replicas: int, spool_dir,
                 chunk_rows: int = RESULT_CHUNK_ROWS, spool=None):
        super().__init__(bus)
        self.partition_id = partition_id
        self.chunk_rows = chunk_rows
        self.spool = spool if spool is not None else Spool(spool_dir)
        self.eofs = EofCounter(upstream_replicas)
        self._average = {}  

    def start(self) -> None:
        k = self.partition_id
        self.bus.listen(queue_of(Q5_IN, k), self.handler(self._on_article), Q5_PEX, [str(k)])
        self.bus.listen(queue_of(AVG_READY_Q, k), self.handler(self._on_average),
                        AVG_EX, kind="fanout")
        self.listen_abort()

    def _on_article(self, message: dict) -> None:
        client_id = tuple(message["client_id"])
        if message["type"] == "q5_article":
            self.spool.append(client_id, [message["url"], message["word_count"]])
        elif message["type"] == "eof":
            self.eofs.add(client_id, message.get("origin", 0))
            self._try_emit(client_id)
        else:
            log.warning("q5_filter: tipo inesperado %s", message["type"])

    def _on_average(self, message: dict) -> None:
        if message["type"] != "average_ready":
            return
        client_id = tuple(message["client_id"])
        self._average[client_id] = message["average"]
        self._try_emit(client_id)

    def _try_emit(self, client_id) -> None:
        if not self.eofs.complete(client_id) or client_id not in self._average:
            return
        average = self._average.pop(client_id)
        gw_key = results_key(client_id)
        rows, sent = [], 0
        if average is not None:
            for url, n in self.spool.read(client_id):
                if n > average:
                    rows.append({"url": url, "abstract_word_count": n})
                    if len(rows) >= self.chunk_rows:
                        self._send_rows(client_id, rows, gw_key)
                        sent += len(rows)
                        rows = []
        if rows:
            self._send_rows(client_id, rows, gw_key)
            sent += len(rows)
        eof = eof_message(client_id, origin=self.partition_id)
        eof["query"] = 5
        self.bus.send(RESULTS_EX, encode(eof), key=gw_key)
        self.spool.delete(client_id)
        self.eofs.forget(client_id)
        log.info("q5_filter[%d]: cliente %s promedio=%s -> %d filas",
                 self.partition_id, client_id, average, sent)

    def _send_rows(self, client_id, rows, gw_key) -> None:
        body = encode(result_message(client_id, 5, rows, origin=self.partition_id))
        self.bus.send(RESULTS_EX, body, key=gw_key)

    def on_abort(self, client_id) -> None:
        self.spool.delete(client_id)
        self._average.pop(client_id, None)
        self.eofs.forget(client_id)


def main() -> None:
    """Variables: RABBIT_HOST, PARTITION_ID (k), LM_REPLICAS, SPOOL_DIR (default spool/q5_<k>)."""
    from common.bus import RabbitBus
    from common.runner import broker_host, env_int, env_str, run_stage
    k = env_int("PARTITION_ID", alt="ID")
    bus = RabbitBus(broker_host())
    run_stage(Q5Filter(bus, k, env_int("LM_REPLICAS"), env_str("SPOOL_DIR", f"spool/q5_{k}")))


if __name__ == "__main__":
    main()
