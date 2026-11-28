import logging
import os
import signal

def env_int(name: str, default=None, alt: str = None) -> int:
    value = os.environ.get(name)
    if value is None and alt:
        value = os.environ.get(alt)
    if value is None:
        if default is None:
            raise SystemExit(f"falta la variable de entorno {name}")
        return default
    return int(value)

def env_float(name: str) -> float:
    value = os.environ.get(name)
    if value is None:
        raise SystemExit(f"falta la variable de entorno {name}")
    return float(value)

def env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def broker_host() -> str:
    return os.environ.get("MOM_HOST") or os.environ.get("RABBIT_HOST") or "localhost"

#Directorio donde la etapa guarda su estado
def state_dir(name: str):
    root = os.environ.get("STATE_DIR")
    return os.path.join(root, name) if root else None


def run_stage(stage, level: str = None) -> None:
    logging.basicConfig(level=level or os.environ.get("LOG_LEVEL", "INFO"),format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    responder = None
    if os.environ.get("NODE_NAME"):
        from common.health import start_from_env
        responder = start_from_env()
    signal.signal(signal.SIGTERM, lambda *_: stage.stop())
    try:
        stage.run()
    except KeyboardInterrupt:
        stage.stop()
    finally:
        if responder is not None:
            responder.stop()
        stage.bus.close()
