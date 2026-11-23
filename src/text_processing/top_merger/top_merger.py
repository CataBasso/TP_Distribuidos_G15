import logging

from common.accumulator import AccumulatorStage
from common.bus import RabbitBus
from common.ft.ids import msg_id
from common.protocol.messages import eof_message, result_message, with_id
from common.runner import broker_host, env_int, run_stage, state_dir
from common.topology import RESULTS_EX, TOP_IN, TOP_N, results_key

log = logging.getLogger(__name__)


class TopMerger(AccumulatorStage):
    name = "top_merger"
    data_type = "partial_top"

    #Guarda top_n y llama a la base con word_partitions como cantidad de EOF esperados
    def __init__(self, bus, word_partitions: int, top_n: int = TOP_N, state_dir=None):
        self.top_n = top_n
        super().__init__(bus, word_partitions, state_dir)

    def new_state(self):
        return {}

    def payload(self, message):
        return message["counts"]

    def merge(self, state, payload):
        for word, n in payload.items():
            state[word] = state.get(word, 0) + n

    #Escucha TOP_IN, abort y recupera el estado de los clientes pendientes
    def start(self) -> None:
        self.bus.listen(TOP_IN, self.handler(self.on_message))
        self.listen_abort()
        self.recover_pending()

    #Ordena por cantidad descendente y luego palabra, toma top_n y publica el resultado de Q4 y su EOF con msg_id
    def finish(self, client_id, state) -> None:
        ranked = sorted((state or {}).items(), key=lambda t: (-t[1], t[0]))[:self.top_n]
        gw_key = results_key(client_id)
        rows = [{"word": w, "count": n} for w, n in ranked]
        self.publish(RESULTS_EX, with_id(result_message(client_id, 4, rows), msg_id(client_id, "q4")),
                     key=gw_key)
        self.publish(RESULTS_EX, with_id(eof_message(client_id, query=4), msg_id(client_id, "q4-eof")),
                     key=gw_key)
        log.info("top_merger: cliente %s -> top %d", client_id, len(rows))


def main() -> None:
    run_stage(TopMerger(RabbitBus(broker_host()), env_int("WORD_PARTITIONS"),
                        state_dir=state_dir("top_merger")))


if __name__ == "__main__":
    main()
