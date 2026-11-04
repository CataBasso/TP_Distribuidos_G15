import csv
import logging
import re
import sys
from pathlib import Path

from common.protocol.messages import (
    eof_message, encode, result_message, stage_eof_message, tokens_message,
)
from common.stage import Stage
from common.topology import (
    LM_IN, RESULT_CHUNK_ROWS, RESULTS_EX, STAGE_ABSTRACT_CLEANER, TOK_EX, results_key,
)

log = logging.getLogger(__name__)

NORM_IN = "norm_in"
WM_IN = "wm_in"

STOPWORDS = frozenset("""
a an and are as at be but by for from has have he her his i if in into is it its of on or
she that the their them they this to was were which with you your not no we our us
au aux avec ce ces dans de des du elle en et il ils je la le les leur lui ma mais me mes
ne nous on ou par pas pour qu que qui sa se ses son sur ta te tes tu un une vos votre vous
est sont été être a ont c d j l m n s t y
""".split())

_WORD = re.compile(r"[^\W\d_]+", re.UNICODE)


def unique_words(text):
    """Palabras únicas (en minúsculas, solo letras, sin stopwords). None si no hay abstract."""
    if text is None:
        return None
    seen, words = set(), []
    for w in _WORD.findall(text.lower()):
        if w not in STOPWORDS and w not in seen:
            seen.add(w)
            words.append(w)
    return words


class StandInCleaner(Stage):
    name = "stand_in_cleaner"

    def __init__(self, bus, replica_id=0, norm_queue=NORM_IN):
        super().__init__(bus)
        self.replica_id, self.norm_queue = replica_id, norm_queue

    def start(self):
        # Las colas de tok_ex se declaran acá para que nada se pierda si este proceso arranca
        # antes que los mappers.
        self.bus.declare(LM_IN, TOK_EX, kind="fanout")
        self.bus.declare(WM_IN, TOK_EX, kind="fanout")
        self.bus.listen(self.norm_queue, self.handler(self._on_input))
        self.listen_abort()

    def _on_input(self, message):
        client_id = tuple(message["client_id"])
        if message["type"] == "batch":
            for row in message["rows"]:
                tokens = tokens_message(row["url"], row["lang"], unique_words(row["abstract"]),
                                        row["period"], client_id, origin=self.replica_id)
                tokens["batch_id"] = message["batch_id"]
                self.bus.send(TOK_EX, encode(tokens))
        elif message["type"] in ("eof", "stage_eof"):
            self.bus.send(TOK_EX, encode(stage_eof_message(
                client_id, STAGE_ABSTRACT_CLEANER, self.replica_id, total=message.get("total"))))

    def on_abort(self, client_id):
        pass


def _read_csv(path):
    with Path(path).open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


class StandInWordStage(Stage):
    name = "stand_in_words"

    def __init__(self, bus, word_partitions=1, expected_dir="data/expected"):
        super().__init__(bus)
        self.P = word_partitions
        self.q3 = [{"word": r["word"]} for r in _read_csv(Path(expected_dir) / "query3.csv")]
        self.q4 = [{"word": r["word"], "count": int(r["count"])}
                   for r in _read_csv(Path(expected_dir) / "query4.csv")]

    def start(self):
        self.bus.listen(WM_IN, self.handler(self._on_input), TOK_EX, kind="fanout")
        self.listen_abort()

    def _on_input(self, message):
        if message["type"] not in ("eof", "stage_eof"):
            return                                   # los tokens se ignoran
        client_id = tuple(message["client_id"])
        key = results_key(client_id)
        for query, rows, origins in ((3, self.q3, self.P), (4, self.q4, 1)):
            for i in range(0, len(rows), RESULT_CHUNK_ROWS):
                self.bus.send(RESULTS_EX, encode(
                    result_message(client_id, query, rows[i:i + RESULT_CHUNK_ROWS])), key=key)
            for origin in range(origins):
                eof = eof_message(client_id, origin=origin, query=query)
                self.bus.send(RESULTS_EX, encode(eof), key=key)
        log.info("stand-in Q3/Q4: cliente %s resultados y EOF publicados", client_id)

    def on_abort(self, client_id):
        pass


def main(argv=None):
    from common.bus import RabbitBus
    from common.runner import broker_host, env_int, env_str, run_stage
    which = (argv or sys.argv[1:] or [""])[0]
    bus = RabbitBus(broker_host())
    if which == "cleaner":
        run_stage(StandInCleaner(bus, env_int("REPLICA_ID", 0, alt="ID"),
                                 env_str("NORM_QUEUE", NORM_IN)))
    elif which == "words":
        run_stage(StandInWordStage(bus, env_int("WORD_PARTITIONS", 1),
                                   env_str("EXPECTED_DIR", "data/expected")))
    else:
        raise SystemExit("uso: python -m devtools.stand_ins cleaner|words")


if __name__ == "__main__":
    main()
