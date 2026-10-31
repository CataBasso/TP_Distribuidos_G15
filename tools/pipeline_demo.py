import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common.bus import RabbitBus                                           # noqa: E402
from common.protocol import decode                                         # noqa: E402
from common.protocol.messages import (                                     # noqa: E402
    encode, eof_message, result_message, tokens_message,
)
from common.topology import (                                              # noqa: E402
    OUT_Q, PERIOD_FROM_2021, PERIOD_UP_TO_2020, RESULTS_EX, RESULTS_Q, TOK_EX, queue_of,
)

OLD = [4, 6]
NEW = [3, 5, 6, 8]
EXPECTED_Q5 = {6, 8}


def tokens(client, url, n, period, batch_id):
    m = tokens_message(url, "en", [f"w{i}" for i in range(n)], period, client)
    m["batch_id"] = batch_id
    return encode(m)


def query_eof(client, query, origin):
    m = eof_message(client, origin=origin)
    m["query"] = query
    return encode(m)


def feed(args):
    bus = RabbitBus(args.host)
    client = (args.gw, args.client)
    key = str(args.gw)
    bus.declare(queue_of(RESULTS_Q, args.gw), RESULTS_EX, [key])
    for i, n in enumerate(OLD):
        bus.send(TOK_EX, tokens(client, f"http://old/{n}", n, PERIOD_UP_TO_2020, i))
    for n in NEW:
        bus.send(TOK_EX, tokens(client, f"http://new/{n}", n, PERIOD_FROM_2021, 9))
    for origin in range(args.cleaners):
        bus.send(TOK_EX, encode(eof_message(client, origin=origin)))
    # Q1-Q4 simuladas: un resultado de ejemplo y los EOF que corresponden a cada una
    bus.send(RESULTS_EX, encode(result_message(
        client, 1, [{"date_modified": "2019-05-01", "url": "http://old/4"}])), key=key)
    bus.send(RESULTS_EX, encode(result_message(
        client, 4, [{"word": "ejemplo", "count": 7}])), key=key)
    for query, origins in ((1, 1), (2, 1), (3, args.word_partitions), (4, 1)):
        for origin in range(origins):
            bus.send(RESULTS_EX, query_eof(client, query, origin), key=key)
    bus.close()
    print(f"cliente {client}: tokens, EOF del cleaner y resultados Q1-Q4 publicados")


def watch(args):
    bus = RabbitBus(args.host)
    client = (args.gw, args.client)
    got = {"q5": set(), "results": 0, "done": False}
    deadline = time.time() + args.timeout

    def on_message(body, ack, nack):
        m = decode(body)
        ack()
        if tuple(m["client_id"]) != client:
            return
        print("out_q:", m["type"], m.get("query", ""), m.get("rows", ""))
        if m["type"] == "result":
            got["results"] += 1
            if m["query"] == 5:
                got["q5"].update(r["abstract_word_count"] for r in m["rows"])
        elif m["type"] == "done":
            got["done"] = True
            bus.stop()

    bus.listen(queue_of(OUT_Q, args.gw), on_message)
    # corta por tiempo aunque no llegue 'done'
    import threading
    timer = threading.Timer(args.timeout, bus.stop)
    timer.daemon = True
    timer.start()
    bus.run()
    timer.cancel()
    ok = got["done"] and got["q5"] == EXPECTED_Q5
    print("done recibido:", got["done"], "| Q5 n =", sorted(got["q5"]),
          "| esperado", sorted(EXPECTED_Q5), "=>", "OK" if ok else "FALLÓ")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["feed", "watch"])
    p.add_argument("--host", default=os.environ.get("RABBIT_HOST", "localhost"))
    p.add_argument("--gw", type=int, default=1)
    p.add_argument("--client", type=int, default=1)
    p.add_argument("--cleaners", type=int, default=2, help="réplicas del AbstractCleaner (EOF a mandar)")
    p.add_argument("--word-partitions", type=int, default=2, help="P")
    p.add_argument("--timeout", type=float, default=30)
    a = p.parse_args()
    {"feed": feed, "watch": watch}[a.command](a)
