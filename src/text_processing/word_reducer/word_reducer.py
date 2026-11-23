import heapq
import logging

from common.accumulator import AccumulatorStage
from common.bus import RabbitBus
from common.ft.ids import msg_id
from common.memory import release_memory
from common.protocol.messages import (
    eof_message, partial_top_message, result_message, with_id,
)
from common.runner import broker_host, env_int, run_stage, state_dir
from common.topology import (
    RESULT_CHUNK_ROWS, RESULTS_EX, TOP_IN, TOP_N, WORDS_EX, WORDS_Q, queue_of, results_key,
)

log = logging.getLogger(__name__)


class WordReducer(AccumulatorStage):
    name = "word_reducer"
    data_type = "word_counts"

    #Guarda partition_id, chunk_rows, top_n; llama a la base con state_dir
    def __init__(self, bus, partition_id: int, upstream_replicas: int,
                 chunk_rows: int = RESULT_CHUNK_ROWS, top_n: int = TOP_N, state_dir=None):
        self.partition_id = partition_id
        self.chunk_rows = chunk_rows
        self.top_n = top_n
        super().__init__(bus, upstream_replicas, state_dir)

    #Estado vacio de un cliente
    def new_state(self):
        return {}                         

    #Lo que se guarda en el WAL
    def payload(self, message):
        return message["counts"]

    #Suma en y fr a cada palabra
    def merge(self, words, counts):
        for word, (en, fr) in counts.items():
            pair = words.setdefault(word, [0, 0])
            pair[0] += en
            pair[1] += fr

    #Declara la cola, escucha y recupera el estado de los clientes pendientes
    def start(self) -> None:
        self.bus.declare(TOP_IN)
        self.bus.listen(queue_of(WORDS_Q, self.partition_id), self.handler(self.on_message),
                        WORDS_EX, [str(self.partition_id)])
        self.listen_abort()
        self.recover_pending()

    #Emite Q3 y el top local
    def finish(self, client_id, words) -> None:
        words = words or {}
        shared = self._emit_shared(client_id, words)
        self._emit_local_top(client_id, words)
        log.info("word_reducer[%d]: cliente %s -> %d palabras compartidas",
                 self.partition_id, client_id, shared)

    #Libera memoria cuando el cliente terminó o se abortó
    def after_release(self) -> None:
        release_memory()                  

    #Emite Q3, cada bloque lleva msg_id(client, "q3", chunk, partición) y el eof "q3-eof"
    def _emit_shared(self, client_id, words) -> int:
        gw_key = results_key(client_id)
        rows, sent, chunk = [], 0, 0
        for word in sorted(w for w, (en, fr) in words.items() if en > 0 and fr > 0):
            rows.append({"word": word})
            if len(rows) >= self.chunk_rows:
                self._send_rows(client_id, rows, gw_key, chunk)
                sent += len(rows)
                chunk += 1
                rows = []
        if rows:
            self._send_rows(client_id, rows, gw_key, chunk)
            sent += len(rows)
        eof = eof_message(client_id, origin=self.partition_id, query=3)
        self.publish(RESULTS_EX, with_id(eof, msg_id(client_id, "q3-eof", 0, self.partition_id)), key=gw_key)
        return sent

    #Recibe el número de chunk y pone el msg_id
    def _send_rows(self, client_id, rows, gw_key, chunk) -> None:
        body = result_message(client_id, 3, rows, origin=self.partition_id)
        self.publish(RESULTS_EX, with_id(body, msg_id(client_id, "q3", chunk, self.partition_id)),
                     key=gw_key)

    #Emite top local con msg_id(client, "top", 0, partición) y el eof "top-eof"
    def _emit_local_top(self, client_id, words) -> None:
        best = heapq.nsmallest(self.top_n, ((-(en + fr), w) for w, (en, fr) in words.items()))
        top = {w: -neg for neg, w in best}
        self.publish("", with_id(partial_top_message(top, client_id, origin=self.partition_id),
                                 msg_id(client_id, "top", 0, self.partition_id)), key=TOP_IN)
        self.publish("", with_id(eof_message(client_id, origin=self.partition_id),
                                 msg_id(client_id, "top-eof", 0, self.partition_id)), key=TOP_IN)


def main() -> None:
    pid = env_int("PARTITION_ID", alt="ID")
    run_stage(WordReducer(RabbitBus(broker_host()), pid, env_int("WM_REPLICAS"),
                          state_dir=state_dir(f"word_reducer_{pid}")))


if __name__ == "__main__":
    main()
