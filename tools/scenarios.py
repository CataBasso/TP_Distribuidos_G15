#!/usr/bin/env python3
import argparse
import random
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KEYS = ("clients", "gw", "year", "q1", "q2", "img", "ac", "lm", "wm", "P", "A", "K")
BASE = dict(clients=1, gw=1, year=1, q1=1, q2=1, img=1, ac=1, lm=1, wm=1, P=1, A=1, K=1)

SCENARIOS = {
    1: ("Un cliente, una réplica de cada etapa", {}),
    2: ("Múltiples clientes, una réplica de cada etapa", dict(clients=3)),
    3: ("Múltiples clientes, filtros replicados (year/q1/q2/imágenes)",
        dict(clients=3, year=2, q1=2, q2=2, img=2)),
    4: ("Múltiples clientes, rama de longitud/Q5 replicada (ac, lm, A, K)",
        dict(clients=3, ac=2, lm=2, A=2, K=2)),
    5: ("Múltiples clientes, rama de palabras/Q3-Q4 replicada (ac, wm, P)",
        dict(clients=3, ac=2, wm=2, P=2)),
    6: ("Múltiples clientes, todo replicado (incluye 2 Gateways)",
        dict(clients=3, gw=2, year=2, q1=2, q2=2, img=2, ac=2, lm=3, wm=2, P=2, A=2, K=3)),
    7: ("Múltiples clientes repartidos en varios Gateways (una réplica del resto)",
        dict(clients=4, gw=2)),
}

SIZES = {"small": ("small_dataset", "small"), "medium": ("medium_dataset", "medium")}


def full(overrides):
    s = dict(BASE)
    s.update(overrides)
    return s


def random_scenario(rng, max_replicas=3):
    s = {k: rng.randint(1, max_replicas) for k in KEYS if k != "clients"}
    s["clients"] = rng.randint(1, 4)
    return s


def describe(s):
    return " ".join(f"{k}={s[k]}" for k in KEYS)


def _svc(name, dockerfile, env, depends=None, context="./src/", volumes=(), ports=(),
         healthcheck=None):
    lines = [f"  {name}:", "    build:", f"      context: {context}",
             f"      dockerfile: {dockerfile}", f"    container_name: {name}"]
    if depends == "rabbit":
        lines += ["    depends_on:", "      rabbitmq:", "        condition: service_healthy"]
    elif depends:
        lines += ["    depends_on:"] + [f"    - {d}" for d in depends]
    if env:
        lines += ["    environment:"] + [f"    - {e}" for e in env]
    if healthcheck:
        lines += healthcheck
    if ports:
        lines += ["    ports:"] + [f"    - {p}" for p in ports]
    if volumes:
        lines += ["    volumes:"] + [f"    - {v}" for v in volumes]
    return "\n".join(lines)


def compose(s, size="small"):
    """YAML del docker-compose del escenario s (dict completo)."""
    dataset, _ = SIZES[size]
    mom = "MOM_HOST=rabbitmq"
    py = "PYTHONUNBUFFERED=1"
    out = []

    out.append(_svc("rabbitmq", "Dockerfile", ["RABBITMQ_LOG_LEVELS=error"], context="./src/rabbitmq",
                    ports=["5672:5672", "15672:15672"], healthcheck=[
                        "    healthcheck:", "      interval: 5s", "      retries: 10",
                        "      start_period: 50s",
                        "      test: rabbitmq-diagnostics check_port_connectivity",
                        "      timeout: 3s"]))
 
    stage_names = []
    for g in range(s["gw"]):
        name = f"gateway_{g}"
        stage_names.append(name)
        out.append(_svc(name, "gateway/Dockerfile",
                        [f"GATEWAY_ID={g + 1}", "GATEWAY_PORT=5000", "RABBIT_HOST=rabbitmq",
                         f"WORD_PARTITIONS={s['P']}", f"Q5_PARTITIONS={s['K']}", py], "rabbit"))

    def replicas(prefix, count, dockerfile, env_of):
        for i in range(count):
            name = f"{prefix}_{i}"
            stage_names.append(name)
            out.append(_svc(name, dockerfile, [f"ID={i}", mom] + env_of + [py], "rabbit"))

    replicas("year_filter", s["year"], "filters/year_filter/Dockerfile",
             ["INPUT_QUEUE=year_in", "Q1_QUEUE=q1_in", "Q2_QUEUE=q2_in", "NORM_QUEUE=norm_in"])
    replicas("q1_filter", s["q1"], "filters/q1_filter/Dockerfile", ["INPUT_QUEUE=q1_in"])
    replicas("q2_filter", s["q2"], "filters/q2_filter/Dockerfile",
             ["INPUT_QUEUE=q2_in", "OUTPUT_QUEUE=img_in"])
    replicas("image_getter", s["img"], "image/Dockerfile", ["INPUT_QUEUE=img_in"])
    replicas("abstract_cleaner", s["ac"], "text_processing/abstract_cleaner/Dockerfile",
             ["INPUT_QUEUE=norm_in"])
    replicas("length_mapper", s["lm"], "text_processing/length_mapper/Dockerfile",
             [f"CLEANER_REPLICAS={s['ac']}", f"AVG_PARTITIONS={s['A']}", f"Q5_PARTITIONS={s['K']}"])
    replicas("avg_reducer", s["A"], "text_processing/avg_reducer/Dockerfile",
             [f"LM_REPLICAS={s['lm']}", f"Q5_PARTITIONS={s['K']}"])
    replicas("q5_filter", s["K"], "filters/q5_filter/Dockerfile", [f"LM_REPLICAS={s['lm']}"])
    replicas("word_mapper", s["wm"], "text_processing/word_mapper/Dockerfile",
             [f"CLEANER_REPLICAS={s['ac']}", f"WORD_PARTITIONS={s['P']}"])
    replicas("word_reducer", s["P"], "text_processing/word_reducer/Dockerfile",
             [f"WM_REPLICAS={s['wm']}"])
    stage_names.append("top_merger")
    out.append(_svc("top_merger", "text_processing/top_merger/Dockerfile",
                    [mom, f"WORD_PARTITIONS={s['P']}", py], "rabbit"))

    for c in range(s["clients"]):
        out.append(_svc(f"client_{c}", "client/Dockerfile",
                        [f"GATEWAY_HOST=gateway_{c % s['gw']}", "GATEWAY_PORT=5000", "DATA_DIR=/data",
                         f"OUTPUT_DIR=/output/client_{c}", py], stage_names,
                        volumes=[f"./data/sample/{dataset}:/data:ro", "./output:/output"]))
    header = f"# Generado por tools/scenarios.py -- {describe(s)}\n"
    return header + "services:\n" + "\n".join(out) + "\n"


def write_all(size, random_n, seed):
    d = ROOT / "scenarios"
    d.mkdir(exist_ok=True)
    written = {}
    for n, (_, over) in SCENARIOS.items():
        written[n] = full(over)
    rng = random.Random(seed)
    for i in range(random_n):
        written[100 + i] = random_scenario(rng)
    for n, s in written.items():
        (d / f"{n}.yaml").write_text(compose(s, size))
    return written


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    sub.add_parser("ids", help="números de los escenarios fijos, separados por espacios")
    g = sub.add_parser("generate")
    g.add_argument("--size", choices=list(SIZES), default="small")
    g.add_argument("--random", type=int, default=0, help="cantidad de escenarios aleatorios extra")
    g.add_argument("--seed", type=int, default=0)
    sw = sub.add_parser("switch")
    sw.add_argument("n", type=int)
    a = p.parse_args(argv)
    if a.cmd == "ids":
        print(" ".join(str(n) for n in SCENARIOS))
    elif a.cmd == "list":
        for n, (desc, over) in SCENARIOS.items():
            print(f"{n}) {desc}\n     {describe(full(over))}")
    elif a.cmd == "generate":
        for n, s in write_all(a.size, a.random, a.seed).items():
            print(f"scenarios/{n}.yaml  {describe(s)}")
    else:
        src = ROOT / "scenarios" / f"{a.n}.yaml"
        if not src.exists():
            sys.exit(f"no existe {src}; corré `python3 tools/scenarios.py generate`")
        shutil.copy(src, ROOT / "docker-compose.yaml")
        print(f"docker-compose.yaml <- scenarios/{a.n}.yaml")
    return 0


if __name__ == "__main__":
    sys.exit(main())
