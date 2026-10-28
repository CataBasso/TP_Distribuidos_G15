"""
Resto del sistema FALSO, para probar a mano el Client y el Gateway con RabbitMQ real.

Hace de YearFilter ... Joiner: lee year_in, cuenta los articles de cada cliente y, al recibir
su EOF, publica en out_q.<gw_id> los resultados esperados (data/expected/*.csv) y 'done'.

    PYTHONPATH=src python tools/fake_system.py --gw-id 1 --rabbit-host localhost

No hace ningún cálculo: sirve solo para ver que lo que entra por year_in y lo que vuelve por
out_q.<gw_id> llega bien de punta a punta.
"""
import argparse
import csv
import logging
from collections import Counter
from pathlib import Path

from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.protocol import decode, done_message, encode, result_message

CHUNK = 100
log = logging.getLogger("fake_system")


def expected(expected_dir, name):
    with (Path(expected_dir) / name).open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def results_for(expected_dir):
    """(query, filas) tal como las entrega el Joiner."""
    q2 = [{"name": r["name"], "url": r["url"], "image": r["url"].encode()}
          for r in expected(expected_dir, "query2.csv")]
    return [
        (1, expected(expected_dir, "query1.csv")),
        (2, q2),
        (3, expected(expected_dir, "query3.csv")),
        (4, [{"word": r["word"], "count": int(r["count"])}
             for r in expected(expected_dir, "query4.csv")]),
        (5, [{"url": r["url"], "abstract_word_count": int(r["abstract_word_count"])}
             for r in expected(expected_dir, "query5.csv")]),
    ]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--gw-id", type=int, default=1)
    p.add_argument("--rabbit-host", default="localhost")
    p.add_argument("--expected-dir", default="data/expected")
    p.add_argument("--year-queue", default="year_in")
    args = p.parse_args()
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    year = MessageMiddlewareQueueRabbitMQ(args.rabbit_host, args.year_queue)
    out = MessageMiddlewareQueueRabbitMQ(args.rabbit_host, f"out_q.{args.gw_id}")
    articles = Counter()

    def on_year(body, ack, nack):
        msg = decode(body)
        cid = msg["client_id"]
        if msg["type"] == "article":
            articles[cid] += 1
        elif msg["type"] == "eof":
            log.info("cliente %s: %d articles y EOF; publico resultados", cid, articles[cid])
            for query, rows in results_for(args.expected_dir):
                for i in range(0, max(len(rows), 1), CHUNK):
                    out.send(encode(result_message(cid, query, rows[i:i + CHUNK])))
            out.send(encode(done_message(cid)))
            log.info("cliente %s: resultados y done publicados", cid)
            articles.pop(cid, None)
        ack()

    log.info("esperando mensajes en %s (Ctrl-C para salir)", args.year_queue)
    try:
        year.start_consuming(on_year)
    except KeyboardInterrupt:
        pass
    finally:
        year.close(); out.close()


if __name__ == "__main__":
    main()
