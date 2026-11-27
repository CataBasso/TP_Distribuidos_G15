import argparse
import random
import subprocess
import sys
import time

PROTECTED = ("rabbitmq", "client_")
# Etapas que todavía no persisten estado (Victoria #18): matarlas en Docker puede perder resultados.
# Con --all se incluyen igual. Borrar esta tupla cuando se migren.
NOT_YET_FT = ("year_filter_", "q1_filter_", "q2_filter_", "image_getter_")

#Nombres de los contenedores en ejecución del compose actual
def list_containers(runner=None):
    run = runner or _docker
    out = run(["ps", "--format", "{{.Names}}"])
    return sorted(n for n in out.split() if n)


def _docker(args):
    return subprocess.run(["docker"] + args, capture_output=True, text=True, check=False).stdout


def eligible(names, include_all=False):
    out = []
    for n in names:
        if n.startswith(PROTECTED):
            continue
        if n.startswith(NOT_YET_FT) and not include_all:
            continue
        out.append(n)
    return out

#Devuelve la lista de contenedores a matar para el escenario dado
def pick_victims(scenario, names, rng, include_all=False):
    pool = eligible(names, include_all)
    if scenario == "random":
        return [rng.choice(pool)] if pool else []
    if scenario == "gateway":
        gws = [n for n in pool if n.startswith("gateway_")]
        return [rng.choice(gws)] if gws else []
    if scenario == "storm":
        return rng.sample(pool, max(1, len(pool) // 2)) if pool else []
    if scenario.startswith("stage:"):
        prefix = scenario.split(":", 1)[1]
        return [n for n in pool if n.startswith(prefix)]
    raise SystemExit(f"escenario desconocido: {scenario}")


def kill(names, dry_run=False, runner=None):
    run = runner or _docker
    for n in names:
        print(f"[chaos] kill -9 {n}", flush=True)
        if not dry_run:
            run(["kill", "-s", "KILL", n])


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--scenario", default="random")
    p.add_argument("--interval", type=float, default=5.0, help="segundos entre ataques (random)")
    p.add_argument("--duration", type=float, default=60.0, help="segundos totales (random)")
    p.add_argument("--once", action="store_true", help="un solo ataque y termina")
    p.add_argument("--all", action="store_true",
                   help="incluir también a las etapas que todavía no persisten estado")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    rng = random.Random(a.seed)
    deadline = time.time() + a.duration
    while True:
        names = list_containers()
        victims = pick_victims(a.scenario, names, rng, a.all)
        if not victims:
            print("[chaos] no hay víctimas elegibles ")
        kill(victims, a.dry_run)
        if a.once or a.scenario != "random":
            return 0                                    
        if time.time() + a.interval > deadline:
            return 0
        time.sleep(a.interval)


if __name__ == "__main__":
    sys.exit(main())
