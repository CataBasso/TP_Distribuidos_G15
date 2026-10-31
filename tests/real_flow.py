"""
Corre todo el sistema en un solo proceso, con los componentes reales del zip (Client, Gateway,
YearFilter, Q1, Q2, ImageGetter) + las etapas de las tareas 7 y 8 + los reemplazos temporales
(devtools.stand_ins) sobre un RabbitMQ falso (tests/fake_rabbit.py). Sin Docker.
Imprime qué entró, qué salió y lo compara contra data/expected.
"""
import csv
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

os.environ.update({"ID": "0", "MOM_HOST": "fake", "INPUT_QUEUE": "year_in", "Q1_QUEUE": "q1_in",
                   "Q2_QUEUE": "q2_in", "NORM_QUEUE": "norm_in", "OUTPUT_QUEUE": "img_in"})

import fake_rabbit  # noqa: E402

A, K, P, LM = 2, 2, 1, 2          # particiones avg, particiones Q5, particiones de palabras, réplicas LM


class StubWiki:
    """Reemplaza a Wikimedia (la VM/sandbox no sale a internet): todo título tiene imagen."""
    def original_urls(self, host, titles):
        return {t: f"http://stub/{host}/{t}" for t in titles}

    def download(self, url):
        return b"img:" + url.encode()


def run(data_dir="data/sample/small_dataset", output_dir=None, timeout=60):
    output_dir = Path(output_dir or tempfile.mkdtemp(prefix="out_"))
    spool = Path(tempfile.mkdtemp(prefix="spool_"))
    fake_rabbit.install()
    for m in [m for m in sys.modules if m.split(".")[0] in
              ("filters", "image", "gateway", "client", "devtools", "common") and m != "common"
              and not m.startswith("common.middleware") and not m.startswith("common.protocol")]:
        del sys.modules[m]

    from common.bus import RabbitBus
    from devtools.stand_ins import StandInCleaner, StandInWordStage
    def env_for(input_queue, output_queue=None):
        # Cada componente real lee INPUT_QUEUE/OUTPUT_QUEUE al IMPORTARSE (como en su contenedor)
        os.environ["INPUT_QUEUE"] = input_queue
        if output_queue:
            os.environ["OUTPUT_QUEUE"] = output_queue

    env_for("q1_in")
    from filters.query1 import Q1Filter
    env_for("q2_in", "img_in")
    from filters.query2 import Q2Filter
    from filters.q5_filter.q5_filter import Q5Filter
    env_for("year_in")
    from filters.year_filter import YearFilter
    env_for("img_in")
    from image.image_getter import ImageGetter
    from results.joiner import Joiner
    from text_processing.avg_reducer.avg_reducer import AvgReducer
    from text_processing.length_mapper.length_mapper import LengthMapper
    from gateway.broker import RabbitGatewayBroker
    from gateway.gateway import Gateway
    from client.client import Client

    threads, stops = [], []

    def spawn(name, fn):
        t = threading.Thread(target=fn, name=name, daemon=True)
        t.start()
        threads.append(t)

    def real(component):                       # componentes del zip: setup_consumers + rc.run
        component.setup_consumers()
        stops.append(component.rc.stop)
        spawn(type(component).__name__, component.rc.run)

    def mine(stage):                           # mis etapas: Stage.start + bus.run
        stage.start()
        stops.append(stage.bus.stop)
        spawn(stage.name, stage.bus.run)

    bus = lambda: RabbitBus("fake")
    # Los que declaran colas de resultados / entradas primero, para no perder mensajes.
    mine(Joiner(bus(), 1, P, K))
    for k in range(K):
        mine(Q5Filter(bus(), k, LM, spool / f"q5_{k}"))
    for a in range(A):
        mine(AvgReducer(bus(), a, LM, K))
    for r in range(LM):
        mine(LengthMapper(bus(), r, 1, A, K))
    mine(StandInCleaner(bus()))
    mine(StandInWordStage(bus(), P))
    real(ImageGetter(wiki=StubWiki()))
    real(Q1Filter())
    real(Q2Filter())
    real(YearFilter())

    gateway = Gateway(1, RabbitGatewayBroker("fake", 1), "127.0.0.1", 0)
    port = gateway.start()
    t0 = time.time()
    client = Client("127.0.0.1", port, data_dir, str(output_dir), 100, 10_000_000)
    ct = threading.Thread(target=client.run, daemon=True)
    ct.start()
    ct.join(timeout)
    elapsed = time.time() - t0
    finished = not ct.is_alive()
    gateway.stop()
    for s in stops:
        try:
            s()
        except Exception:
            pass
    fake_rabbit.uninstall()
    shutil.rmtree(spool, ignore_errors=True)
    return finished, elapsed, output_dir


def rows(path):
    with Path(path).open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def compare(output_dir, expected_dir="data/expected"):
    report = {}
    for q, key in ((1, ("url",)), (2, ("name", "url")), (3, ("word",)), (4, ("word", "count")),
                   (5, ("url", "abstract_word_count"))):
        got = rows(Path(output_dir) / f"output_query_{q}.csv")
        exp = rows(Path(expected_dir) / f"query{q}.csv")
        g = {tuple(r[k] for k in key) for r in got}
        e = {tuple(r[k] for k in key) for r in exp}
        report[q] = (len(got), len(exp), len(g & e), len(g - e), len(e - g))
    return report


if __name__ == "__main__":
    logging.basicConfig(level=getattr(logging, os.environ.get("LVL","WARNING")))
    data = sys.argv[1] if len(sys.argv) > 1 else "data/sample/small_dataset"
    out = sys.argv[2] if len(sys.argv) > 2 else None
    ok, secs, out_dir = run(data, out)
    print(f"terminó: {ok}  ({secs:.1f}s)  salida: {out_dir}")
    print("query | filas obtenidas | esperadas | coinciden | sobran | faltan")
    for q, (n, ne, hit, extra, miss) in compare(out_dir).items():
        print(f"  Q{q}  | {n:5d} | {ne:5d} | {hit:5d} | {extra:4d} | {miss:4d}")
