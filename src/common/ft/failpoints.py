import logging
import os
import random

log = logging.getLogger(__name__)


class SimulatedCrash(BaseException):
    """Caída simulada en tests. Es BaseException para que ningún `except Exception` la trague."""


def _exit():
    os._exit(137)


_handler = _exit
_target = None
_spec = None
_hits = {}
_rng = random.Random()


def _parse(text):
    spec = {}
    for item in filter(None, (t.strip() for t in (text or "").split(","))):
        if "@" in item:
            name, p = item.split("@", 1)
            spec[name] = ("prob", float(p))
        else:
            name, _, n = item.partition(":")
            spec[name] = ("count", int(n or 1))
    return spec


def _marker():
    root = os.environ.get("STATE_DIR")
    return os.path.join(root, ".failpoints_fired") if root else None


def _fired_before():
    path = _marker()
    try:
        with open(path) as f:
            return {line.strip() for line in f if line.strip()}
    except (TypeError, OSError):
        return set()


def _remember(name):
    path = _marker()
    if path is None:
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a") as f:
            f.write(name + "\n")
            f.flush()
            os.fsync(f.fileno())
    except OSError:
        pass


def configure(text=None, seed=None, target=None):
    global _spec, _target
    _target = target
    _spec = _parse(os.environ.get("FAILPOINTS", "") if text is None else text)
    for name in _fired_before():
        if _spec.get(name, ("", 0))[0] == "count":
            del _spec[name]
    _hits.clear()
    if seed is not None:
        _rng.seed(seed)


def set_handler(fn=None):
    global _handler
    _handler = fn or _exit


def hit(name):
    global _spec
    if _spec is None:
        configure()
    if not _spec:
        return
    rule = _spec.get(name)
    if rule is None:
        return
    if _target is not None:
        import threading
        if not threading.current_thread().name.startswith(_target):
            return
    kind, value = rule
    if kind == "count":
        _hits[name] = _hits.get(name, 0) + 1
        fire = _hits[name] == value
    else:
        fire = _rng.random() < value
    if fire:
        if kind == "count":
            _remember(name)
        log.critical("FAILPOINT %s: el proceso termina de forma abrupta", name)
        _handler()
