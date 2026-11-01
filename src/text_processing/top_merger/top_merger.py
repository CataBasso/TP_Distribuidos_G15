import logging

from common.protocol.messages import encode, eof_message, result_message
from common.stage import EofCounter, Stage
from common.topology import RESULTS_EX, TOP_IN, TOP_N, results_key
from common.bus import RabbitBus
from common.runner import broker_host, env_int, run_stage

log = logging.getLogger(__name__)


class TopMerger(Stage):
    name = "top_merger"

    def __init__(self, bus, word_partitions: int, top_n: int = TOP_N):
        super().__init__(bus)
        self.top_n = top_n
        self.eofs = EofCounter(word_partitions)
        self._tops = {}  

    def start(self) -> None:
        self.bus.listen(TOP_IN, self.handler(self._on_input))
        self.listen_abort()

    def _on_input(self, message: dict) -> None:
        client_id = tuple(message["client_id"])
        if message["type"] == "partial_top":
            tops = self._tops.setdefault(client_id, {})
            for word, n in message["counts"].items():
                tops[word] = tops.get(word, 0) + n
        elif message["type"] == "eof":
            if self.eofs.add(client_id, message.get("origin", 0)):
                self._publish(client_id)
        else:
            log.warning("top_merger: tipo inesperado %s", message["type"])

    def _publish(self, client_id) -> None:
        counts = self._tops.pop(client_id, {})
        ranked = sorted(counts.items(), key=lambda t: (-t[1], t[0]))[:self.top_n]
        gw_key = results_key(client_id)
        rows = [{"word": w, "count": n} for w, n in ranked]
        self.bus.send(RESULTS_EX, encode(result_message(client_id, 4, rows)), key=gw_key)
        eof = eof_message(client_id, query=4)
        self.bus.send(RESULTS_EX, encode(eof), key=gw_key)
        self.eofs.forget(client_id)
        log.info("top_merger: cliente %s -> top %d", client_id, len(rows))

    def on_abort(self, client_id) -> None:
        self._tops.pop(client_id, None)
        self.eofs.forget(client_id)

def main() -> None:
    bus = RabbitBus(broker_host())
    run_stage(TopMerger(bus, env_int("WORD_PARTITIONS")))


if __name__ == "__main__":
    main()
