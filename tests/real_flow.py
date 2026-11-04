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
import scenarios as scn  # noqa: E402

DATASET = os.environ.get("DATASET", "data/sample/small_dataset")
EXPECTED = os.environ.get("EXPECTED_DIR", "data/expected/small")
PURGE = ("filters", "image", "gateway", "client", "results", "text_processing", "devtools")


class StubWiki:
    """Reemplaza a Wikimedia (el sandbox no sale a internet): todo título tiene imagen."""
    def original_urls(self, host, titles):
        return {t: f"http://stub/{host}/{t}" for t in titles}

    def download(self, url):
        return b"img:" + url.encode()


def run(s, data_dir=DATASET, output_dir=None, timeout=120):
    """Corre el escenario s (dict completo de réplicas). Devuelve (terminó, segundos, carpeta)."""
    output_dir = Path(output_dir or tempfile.mkdtemp(prefix="out_"))
    spool = Path(tempfile.mkdtemp(prefix="spool_"))
    fake_rabbit.install()
    for m in [m for m in sys.modules if m.split(".")[0] in PURGE]:
        del sys.modules[m]
    import importlib

    def load(module, input_queue=None, output_queue=None):
        if input_queue:
            os.environ["INPUT_QUEUE"] = input_queue
        if output_queue:
            os.environ["OUTPUT_QUEUE"] = output_queue
        return importlib.import_module(module)

    from common.bus import RabbitBus
    q1 = load("filters.q1_filter.query1", "q1_in")
    q2 = load("filters.q2_filter.query2", "q2_in", "img_in")
    yf = load("filters.year_filter.year_filter", "year_in")
    ig = load("image.image_getter", "img_in")
    ac = load("text_processing.abstract_cleaner.abstract_cleaner", "norm_in")
    from filters.q5_filter.q5_filter import Q5Filter
    from common.topology import expected_result_eofs
    from text_processing.avg_reducer.avg_reducer import AvgReducer
    from text_processing.length_mapper.length_mapper import LengthMapper
    from text_processing.top_merger.top_merger import TopMerger
    from text_processing.word_mapper.word_mapper import WordMapper
    from text_processing.word_reducer.word_reducer import WordReducer
    from gateway.broker import RabbitGatewayBroker
    from gateway.gateway import Gateway
    from client.client import Client

    stops = []

    def spawn(name, fn):
        threading.Thread(target=fn, name=name, daemon=True).start()

    def real(mod, cls, i, *a, **kw):           # componentes de la otra gente: leen ID del módulo
        mod.ID = i
        c = getattr(mod, cls)(*a, **kw)
        c.setup_consumers()
        stops.append(c.rc.stop)
        spawn(f"{cls}{i}", c.rc.run)

    def mine(stage):                           # etapas con Stage: start() + bus.run()
        stage.start()
        stops.append(stage.bus.stop)
        spawn(stage.name, stage.bus.run)

    bus = lambda: RabbitBus("fake")
    AC, LM, WM, P, A, K = s["ac"], s["lm"], s["wm"], s["P"], s["A"], s["K"]
    for k in range(K):
        mine(Q5Filter(bus(), k, LM, spool / f"q5_{k}"))
    for a in range(A):
        mine(AvgReducer(bus(), a, LM, K))
    mine(TopMerger(bus(), P))
    for p in range(P):
        mine(WordReducer(bus(), p, WM))
    for r in range(WM):
        mine(WordMapper(bus(), r, AC, P))
    for r in range(LM):
        mine(LengthMapper(bus(), r, AC, A, K))
    for i in range(AC):
        real(ac, "AbstractCleaner", i)
    for i in range(s["img"]):
        real(ig, "ImageGetter", i, wiki=StubWiki())
    for i in range(s["q1"]):
        real(q1, "Q1Filter", i)
    for i in range(s["q2"]):
        real(q2, "Q2Filter", i)
    for i in range(s["year"]):
        real(yf, "YearFilter", i)

    gateways = [Gateway(g + 1, RabbitGatewayBroker("fake", g + 1), "127.0.0.1", 0,
                        expected_eofs=expected_result_eofs(P, K)) for g in range(s["gw"])]
    ports = [gw.start() for gw in gateways]
    t0 = time.time()
    errors = []

    def run_client(c):
        try:
            Client("127.0.0.1", ports[c % len(ports)], data_dir, str(output_dir / f"client_{c}"), 100, 10_000_000).run()
        except Exception as e:                  # noqa: BLE001
            errors.append(f"client_{c}: {e}")

    cts = [threading.Thread(target=run_client, args=(c,), daemon=True) for c in range(s["clients"])]
    for t in cts:
        t.start()
    for t in cts:
        t.join(max(0.1, timeout - (time.time() - t0)))
    finished = not any(t.is_alive() for t in cts) and not errors
    elapsed = time.time() - t0
    for gw in gateways:
        gw.stop()
    for stop in stops:
        try:
            stop()
        except Exception:                       # noqa: BLE001
            pass
    fake_rabbit.uninstall()
    shutil.rmtree(spool, ignore_errors=True)
    if errors:
        print("   ", "; ".join(errors))
    return finished, elapsed, output_dir


from verify_output import compare, ok_report, rows  # noqa: E402,F401  (tools/)


def check(output_dir, clients, expected=EXPECTED):
    """True si el CSV de cada cliente coincide con el esperado."""
    return all(ok_report(compare(Path(output_dir) / f"client_{c}", expected)) for c in range(clients))


def main(argv=None):
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("scenarios", nargs="*", type=int)
    p.add_argument("--random", type=int, default=0)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--timeout", type=int, default=120)
    a = p.parse_args(argv)
    logging.basicConfig(level=getattr(logging, os.environ.get("LVL", "WARNING")))
    names = a.scenarios or ([] if a.random else list(scn.SCENARIOS))
    todo = {n: scn.full(scn.SCENARIOS[n][1]) for n in names}
    import random
    rng = random.Random(a.seed)
    for i in range(a.random):
        todo[100 + i] = scn.random_scenario(rng)
    bad = 0
    for n, s in todo.items():
        finished, secs, out = run(s, timeout=a.timeout)
        good = finished and check(out, s["clients"])
        bad += not good
        print(f"[{'OK ' if good else 'FALLA'}] escenario {n:>3}  {secs:5.1f}s  {scn.describe(s)}")
        if not good:
            for c in range(s["clients"]):
                rep = compare(Path(out) / f"client_{c}", EXPECTED)
                print(f"     client_{c}: " + "  ".join(
                    f"Q{q}:{hit}/{ne}(+{ex}/-{mi})" for q, (_, ne, hit, ex, mi) in rep.items()))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
