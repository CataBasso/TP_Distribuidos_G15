#!/usr/bin/env python3
import argparse
import csv
import re
import subprocess
import sys
from pathlib import Path

KEYS = {1: ("url",), 2: ("name", "url"), 3: ("word",), 4: ("word", "count"),
        5: ("url", "abstract_word_count")}


def rows(path):
    with Path(path).open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def compare(client_dir, expected_dir):
    """{consulta: (obtenidas, esperadas, coinciden, sobran, faltan)} de un cliente."""
    report = {}
    for q, key in KEYS.items():
        path = Path(client_dir) / f"output_query_{q}.csv"
        got = rows(path) if path.exists() else []
        exp = rows(Path(expected_dir) / f"query{q}.csv")
        g = {tuple(r[k] for k in key) for r in got}
        e = {tuple(r[k] for k in key) for r in exp}
        report[q] = (len(got), len(exp), len(g & e), len(g - e), len(e - g))
    return report


def ok_report(rep):
    return all(extra == 0 and miss == 0 for _, _, _, extra, miss in rep.values())


def compose_clients(compose="docker-compose.yaml"):
    return sorted(set(re.findall(r"^  (client_\d+):$", Path(compose).read_text(), re.M)),
                  key=lambda n: int(n.split("_")[1]))


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--wait", action="store_true")
    p.add_argument("--output", default="output")
    p.add_argument("--expected", default="data/expected/small")
    p.add_argument("--compose", default="docker-compose.yaml")
    a = p.parse_args(argv)
    clients = compose_clients(a.compose)
    if a.wait:
        res = subprocess.run(["docker", "container", "wait"] + clients, capture_output=True, text=True)
        codes = res.stdout.split()
        if codes.count("0") != len(clients):
            print(f"algún cliente terminó con error (códigos: {codes}); mirá `docker compose logs`")
            return 1
    bad = 0
    for c in clients:
        rep = compare(Path(a.output) / c, a.expected)
        good = ok_report(rep)
        bad += not good
        print(f"[{'OK ' if good else 'FALLA'}] {c}: " + "  ".join(
            f"Q{q}:{hit}/{ne}(+{ex}/-{mi})" for q, (_, ne, hit, ex, mi) in rep.items()))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
