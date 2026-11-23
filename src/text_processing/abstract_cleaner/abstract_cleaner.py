import logging
import re

from common.batch_stage import BatchStage
from common.bus import RabbitBus
from common.ft.ids import msg_id
from common.protocol.messages import stage_eof_message, tokens_batch_message, with_id
from common.runner import broker_host, env_int, env_str, run_stage, state_dir
from common.stopwords.stopwords import STOP_WORDS
from common.topology import STAGE_ABSTRACT_CLEANER, TOK_EX

STAGE = STAGE_ABSTRACT_CLEANER

WORD = re.compile(r"\w+")
NUMBER = re.compile(r"\d+")

#Limpia el abstract de un artículo y devuelve la lista de palabras que se conservan.
def clean_abstract(abstract):
    if not isinstance(abstract, str):
        return None

    return [w for w in WORD.findall(abstract.lower())
            if w not in STOP_WORDS and not NUMBER.fullmatch(w)]

#BatchStage se ocupa de consumir la cola, coordinar el EOF por cobertura de batch_id, persistir ese estado, manejar el abort y confirmar cada mensaje
class AbstractCleaner(BatchStage):
    name = STAGE
    ctrl_exchange = "ac_ctrl"

    #Bus -> conexion al broker, replica_id -> número de esta réplica, input_queue -> cola de entrada, state_dir -> directorio donde persiste su estado
    def __init__(self, bus, replica_id, input_queue="norm_in", state_dir=None):
        self.input_queue = input_queue
        super().__init__(bus, replica_id, state_dir)

    #Procesa un batch: limpia los abstracts y publica el resultado con msg_id determinista.
    def process_batch(self, client_id, message):
        batch_id = message["batch_id"]
        articles = []
        for row in message["rows"]:
            try:
                url, lang, period = row["url"], row["lang"], row["period"]
                words = clean_abstract(row["abstract"])
            except (KeyError, TypeError) as e:
                logging.warning("fila inválida (%s: %s), se descarta", type(e).__name__, e)
                continue
            articles.append([url, lang, period, None if words is None else len(words),
                             None if words is None else list(set(words))])
        # Siempre se publica el batch (aunque quede vacío): las etapas siguientes llevan la
        # cobertura de batch_id y necesitan ver todos.
        out = tokens_batch_message(articles, client_id, batch_id, origin=self.replica_id)
        self.publish(TOK_EX, with_id(out, msg_id(client_id, STAGE, batch_id)))

    #Publica el EOF de este cliente hacia las etapas siguientes, con su msg_id determinista.
    def publish_eof(self, client_id, total):
        eof = stage_eof_message(client_id, STAGE, self.replica_id, total=total)
        self.publish(TOK_EX, with_id(eof, msg_id(client_id, "ac-eof")))

#Punto de entrada: lee ID, INPUT_QUEUE y el host del broker del entorno, arma la etapa y la ejecuta
def main():
    rid = env_int("ID")
    run_stage(AbstractCleaner(RabbitBus(broker_host()), rid, env_str("INPUT_QUEUE", "norm_in"),
                              state_dir(f"abstract_cleaner_{rid}")))


if __name__ == "__main__":
    main()