import logging
import collections

from common.protocol.messages import encode, eof_message, partial_top_message, result_message
from common.stage import EofCounter, Stage
from common.topology import (
    RESULT_CHUNK_ROWS, RESULTS_EX, TOP_IN, TOP_N, WORDS_EX, WORDS_Q, queue_of, results_key,
)
from common.bus import RabbitBus
from common.runner import broker_host, env_int, run_stage

log = logging.getLogger(__name__)

class WordReducer(Stage):
    name = "word_reducer"

    def __init__(self, bus, partition_id: int, upstream_replicas: int,
                 chunk_rows: int = RESULT_CHUNK_ROWS, top_n: int = TOP_N):
        super().__init__(bus)
        self.partition_id = partition_id
        self.chunk_rows = chunk_rows
        self.top_n = top_n
        self.eofs = EofCounter(upstream_replicas)
        self._words = {}  

    def start(self) -> None:
        self.bus.declare(TOP_IN)
        self.bus.listen(queue_of(WORDS_Q, self.partition_id), self.handler(self._on_input),
                        WORDS_EX, [str(self.partition_id)])
        self.listen_abort()

    def _on_input(self, message: dict) -> None:
        client_id = tuple(message["client_id"])
        if message["type"] == "word_counts":
            words = self._words.setdefault(client_id, {})
            for word, (en, fr) in message["counts"].items():
                pair = words.setdefault(word, [0, 0])
                pair[0] += en
                pair[1] += fr
        elif message["type"] == "eof":
            if self.eofs.add(client_id, message.get("origin", 0)):
                self._finish(client_id)
        else:
            log.warning("word_reducer: tipo inesperado %s", message["type"])

    def _finish(self, client_id) -> None:
        words = self._words.pop(client_id, {})
        shared = self._emit_shared(client_id, words)
        self._emit_local_top(client_id, words)
        self.eofs.forget(client_id)
        log.info("word_reducer[%d]: cliente %s -> %d palabras compartidas",
                 self.partition_id, client_id, shared)

    def _emit_shared(self, client_id, words) -> int:
        gw_key = results_key(client_id)
        rows, sent = [], 0
        for word in sorted(w for w, (en, fr) in words.items() if en > 0 and fr > 0):
            rows.append({"word": word})
            if len(rows) >= self.chunk_rows:
                self._send_rows(client_id, rows, gw_key)
                sent += len(rows)
                rows = []
        if rows:
            self._send_rows(client_id, rows, gw_key)
            sent += len(rows)
        eof = eof_message(client_id, origin=self.partition_id, query=3)
        self.bus.send(RESULTS_EX, encode(eof), key=gw_key)
        return sent

    def _send_rows(self, client_id, rows, gw_key) -> None:
        body = encode(result_message(client_id, 3, rows, origin=self.partition_id))
        self.bus.send(RESULTS_EX, body, key=gw_key)

    def _emit_local_top(self, client_id, words) -> None:
        words_counter = collections.Counter(
            {w: en + fr for w, (en, fr) in sorted(words.items())})
        top = dict(words_counter.most_common(self.top_n))
        self.bus.send("", encode(partial_top_message(top, client_id, origin=self.partition_id)),
                      key=TOP_IN)
        self.bus.send("", encode(eof_message(client_id, origin=self.partition_id)), key=TOP_IN)

    def on_abort(self, client_id) -> None:
        self._words.pop(client_id, None)
        self.eofs.forget(client_id)

def main() -> None:
    bus = RabbitBus(broker_host())
    run_stage(WordReducer(bus, env_int("PARTITION_ID", alt="ID"), env_int("WM_REPLICAS")))


if __name__ == "__main__":
    main()

