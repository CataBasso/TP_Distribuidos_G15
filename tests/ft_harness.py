import os
import random
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
    def original_urls(self, host, titles):
        return {t: f"http://stub/{host}/{t}" for t in titles}

    def download(self, url):
        return b"img:" + url.encode()


class Legacy:
    """Adaptador para las etapas de Victoria que todavía no migraron a Stage (YearFilter, Q1, Q2,
    ImageGetter): leen ID del módulo y tienen su propia conexión. No tienen estado persistente,
    por eso el arnés NO las mata (ver LEGACY). Cuando se migren, borrar este adaptador."""

    def __init__(self, mod, cls, i, *a, **kw):
        mod.ID = i
        self.stage = getattr(mod, cls)(*a, **kw)
        self.name = f"{cls}{i}"
        self.bus = self

    @property
    def rc(self):
        return self.stage.rc

    def start(self):
        self.stage.setup_consumers()

    def run(self):
        self.stage.rc.run()

    def stop(self):
        self.stage.rc.stop()


LEGACY = ("year_", "q1_", "q2_", "img_")


class Cluster:
    def __init__(self, s, root=None, data_dir=DATASET, failpoints=None, target=None,
                 chaos=None, seed=0, restart_delay=0.05):
        self.s = s
        self.root = Path(root or tempfile.mkdtemp(prefix="ft_"))
        self.data_dir = data_dir
        self.output = self.root / "out"
        self.failpoints, self.target = failpoints, target
        self.restart_delay = restart_delay
        self.chaos, self.rng = chaos, random.Random(seed)
        self.units = {}                  # nombre -> dict(factory, stage, thread)
        self.gateways = {}               # nombre -> dict(factory, gw)
        self.ports = {}
        self.restarts = {}
        self.closing = False
        self.errors = []
        self._lock = threading.Lock()

    # ---------------------------------------------------------------- construcción
    def _imports(self):
        fake_rabbit.install()
        for m in [m for m in sys.modules if m.split(".")[0] in PURGE]:
            del sys.modules[m]
        from common import bus as _b  # noqa: F401
        global failpoints_mod
        from common.ft import failpoints as failpoints_mod  # noqa: F401
        return failpoints_mod

    def build(self):
        fp = self._imports()
        from common.bus import RabbitBus
        from common.topology import expected_result_eofs
        import importlib

        def load(module, input_queue=None, output_queue=None):
            if input_queue:
                os.environ["INPUT_QUEUE"] = input_queue
            if output_queue:
                os.environ["OUTPUT_QUEUE"] = output_queue
            return importlib.import_module(module)
        q1 = load("filters.q1_filter.query1", "q1_in")
        q2 = load("filters.q2_filter.query2", "q2_in", "img_in")
        yf = load("filters.year_filter.year_filter", "year_in")
        ig = load("image.image_getter", "img_in")
        from filters.q5_filter.q5_filter import Q5Filter
        from gateway.broker import RabbitGatewayBroker
        from gateway.gateway import Gateway
        from text_processing.abstract_cleaner.abstract_cleaner import AbstractCleaner
        from text_processing.avg_reducer.avg_reducer import AvgReducer
        from text_processing.length_mapper.length_mapper import LengthMapper
        from text_processing.top_merger.top_merger import TopMerger
        from text_processing.word_mapper.word_mapper import WordMapper
        from text_processing.word_reducer.word_reducer import WordReducer
        self.Gateway, self.RabbitGatewayBroker = Gateway, RabbitGatewayBroker
        self.expected_eofs = None
        s, R = self.s, self.root
        AC, LM, WM, P, A, K = s["ac"], s["lm"], s["wm"], s["P"], s["A"], s["K"]
        self.expected_eofs = expected_result_eofs(P, K)
        bus = lambda: RabbitBus("fake")                                    # noqa: E731
        sd = lambda n: str(R / "state" / n)                                # noqa: E731
        u = self.units
        for k in range(K):
            u[f"q5_{k}"] = dict(f=lambda k=k: Q5Filter(bus(), k, LM, str(R / "spool" / f"q5_{k}"),
                                                       state_dir=sd(f"q5_{k}")))
        for a in range(A):
            u[f"avg_{a}"] = dict(f=lambda a=a: AvgReducer(bus(), a, LM, K, sd(f"avg_{a}")))
        u["top"] = dict(f=lambda: TopMerger(bus(), P, state_dir=sd("top")))
        for p in range(P):
            u[f"wr_{p}"] = dict(f=lambda p=p: WordReducer(bus(), p, WM, state_dir=sd(f"wr_{p}")))
        for r in range(WM):
            u[f"wm_{r}"] = dict(f=lambda r=r: WordMapper(bus(), r, AC, P, state_dir=sd(f"wm_{r}")))
        for r in range(LM):
            u[f"lm_{r}"] = dict(f=lambda r=r: LengthMapper(bus(), r, AC, A, K, state_dir=sd(f"lm_{r}")))
        for i in range(AC):
            u[f"ac_{i}"] = dict(f=lambda i=i: AbstractCleaner(bus(), i, "norm_in", sd(f"ac_{i}")))
        for i in range(s["img"]):
            u[f"img_{i}"] = dict(f=lambda i=i: Legacy(ig, "ImageGetter", i, wiki=StubWiki()))
        for i in range(s["q1"]):
            u[f"q1_{i}"] = dict(f=lambda i=i: Legacy(q1, "Q1Filter", i))
        for i in range(s["q2"]):
            u[f"q2_{i}"] = dict(f=lambda i=i: Legacy(q2, "Q2Filter", i))
        for i in range(s["year"]):
            u[f"year_{i}"] = dict(f=lambda i=i: Legacy(yf, "YearFilter", i))
        for g in range(s["gw"]):
            self.gateways[f"gw_{g}"] = dict(gid=g + 1)
        fp.set_handler(self._on_failpoint)
        fp.configure(self.failpoints or "", target=self.target)
        return fp

    def _on_failpoint(self):
        """En un proceso real: os._exit(137). Acá: el hilo muere. Si es un Gateway, además se lo
        marca como caído para que su manejador de cliente no publique un abort (no corre ningún finally)."""
        name = threading.current_thread().name
        for g in self.gateways.values():
            gw = g.get("gw")
            if gw is not None and name.startswith(f"gw{g['gid']}"):
                self._hard_kill(g)
        from common.ft.failpoints import SimulatedCrash
        raise SimulatedCrash(name)

    def _hard_kill(self, g):
        """SIGKILL del Gateway: el socket de escucha y todas las conexiones se cierran de golpe y
        el proceso no ejecuta ningún finally (por eso se marca _stop: el manejador no aborta)."""
        gw = g["gw"]
        gw._stop.set()
        g["dead"] = True
        try:
            gw.broker._results_rc.kill()      # 1.º el broker ve la conexión caída (lo no confirmado vuelve a la cola)
        except Exception:
            pass
        try:
            gw._server.close()
        except Exception:
            pass
        with gw._sessions_lock:
            sessions = list(gw._sessions.values())
        for sess in sessions:
            try:
                sess.close()
            except Exception:
                pass

    # ---------------------------------------------------------------- ciclo de vida
    def spawn(self, name):
        from common.ft.failpoints import SimulatedCrash
        with self._lock:
            unit = self.units[name]
            stage = unit.get("stage")
            try:
                stage = unit["f"]()
                stage.start()
                t = threading.Thread(target=stage.bus.run, name=name, daemon=True)
            except SimulatedCrash:
                # un failpoint disparó dentro de la construcción o de start() (p. ej. al republicar en recover_pending):
                # el proceso murió antes de consumir; el watcher lo reinicia en la próxima vuelta
                t = threading.Thread(target=lambda: None, name=name, daemon=True)
            unit["stage"], unit["thread"] = stage, t
            t.start()

    def spawn_gateway(self, name):
        g = self.gateways[name]
        gw = self.Gateway(g["gid"], self.RabbitGatewayBroker("fake", g["gid"]), "127.0.0.1",
                          self.ports.get(name, 0), expected_eofs=self.expected_eofs,
                          state_dir=str(self.root / "state" / name), resume_grace=20)
        g["gw"], g["dead"] = gw, False
        self.ports[name] = gw.start()

    def crash(self, name):
        """Caída externa (SIGKILL) de una etapa o Gateway."""
        if name in self.gateways:
            self._hard_kill(self.gateways[name])
            return
        unit = self.units[name]
        unit["stage"].bus.rc.kill()

    def _watch(self):
        while not self.closing:
            time.sleep(0.02)
            for name, unit in list(self.units.items()):
                t = unit.get("thread")
                if t is not None and not t.is_alive() and not self.closing:
                    time.sleep(self.restart_delay)
                    self.restarts[name] = self.restarts.get(name, 0) + 1
                    try:
                        unit["stage"].bus.rc._release()
                    except Exception:
                        pass
                    self.spawn(name)
            for name, g in list(self.gateways.items()):
                if g.get("dead") and not self.closing:
                    try:
                        g["gw"].crash()
                    except Exception:
                        pass
                    time.sleep(self.restart_delay)
                    self.restarts[name] = self.restarts.get(name, 0) + 1
                    self.spawn_gateway(name)
            if self.chaos and self.rng.random() < self.chaos:
                victims = [n for n in list(self.units) + list(self.gateways) if not n.startswith(LEGACY)]
                self.crash(self.rng.choice(victims))

    # ---------------------------------------------------------------- ejecución
    def run(self, timeout=120):
        from client.client import Client
        self.build()
        for name in self.units:
            self.spawn(name)
        for name in self.gateways:
            self.spawn_gateway(name)
        threading.Thread(target=self._watch, daemon=True, name="watcher").start()
        names = list(self.gateways)
        t0 = time.time()

        self.clients = {}

        def run_client(c):
            try:
                cl = self.clients[c] = Client("127.0.0.1", self.ports[names[c % len(names)]],
                                              self.data_dir, str(self.output / f"client_{c}"),
                                              100, 10_000_000, retry_delay=0.1)
                cl.run()
            except Exception as e:                      # noqa: BLE001
                self.errors.append(f"client_{c}: {e}")

        cts = [threading.Thread(target=run_client, args=(c,), daemon=True, name=f"client-{c}")
               for c in range(self.s["clients"])]
        for t in cts:
            t.start()
        for t in cts:
            t.join(max(0.1, timeout - (time.time() - t0)))
        self.elapsed = time.time() - t0
        self.finished = not any(t.is_alive() for t in cts) and not self.errors
        self.close()
        return self.finished

    def close(self):
        self.closing = True
        for g in self.gateways.values():
            try:
                g["gw"].stop()
            except Exception:
                pass
        for unit in self.units.values():
            try:
                unit["stage"].bus.stop()
            except Exception:
                pass
        time.sleep(0.1)
        fake_rabbit.uninstall()
        try:
            from common.ft import failpoints
            failpoints.set_handler(None)
            failpoints.configure("")
        except Exception:
            pass

    def cleanup(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def report(self):
        from verify_output import compare
        out = []
        for c in range(self.s["clients"]):
            rep = compare(self.output / f"client_{c}", EXPECTED)
            out.append(f"client_{c}: " + " ".join(f"Q{q}:{hit}/{ne}(+{ex}/-{mi})"
                                                  for q, (_, ne, hit, ex, mi) in rep.items()))
        return "; ".join(out)

    def check(self):
        """Resultado correcto: mismas filas que las esperadas (como conjuntos) y, además, Q3, Q4 y Q5
        con la MISMA CANTIDAD de filas (sin repetidas). Q1 y Q2 se comparan sólo como conjuntos: sus
        etapas (YearFilter/Q1/Q2/ImageGetter) todavía no ponen msg_id y, si el Gateway reenvía un
        batch tras caer, pueden repetir filas (limitación conocida, ver docs/FALLAS.md)."""
        from verify_output import compare, ok_report
        for c in range(self.s["clients"]):
            rep = compare(self.output / f"client_{c}", EXPECTED)
            if not ok_report(rep):
                return False
            if any(rep[q][0] != rep[q][1] for q in (3, 4, 5)):      # (obtenidas != esperadas)
                return False
        return True


def run_case(s, failpoints=None, target=None, chaos=None, seed=0, timeout=120):
    c = Cluster(s, failpoints=failpoints, target=target, chaos=chaos, seed=seed)
    try:
        ok = c.run(timeout)
        good = ok and c.check()
        return good, c
    finally:
        pass
