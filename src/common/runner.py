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


def env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def broker_host() -> str:
    return os.environ.get("MOM_HOST") or os.environ.get("RABBIT_HOST") or "localhost"


def run_stage(stage, level: str = None) -> None:
    logging.basicConfig(level=level or os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    signal.signal(signal.SIGTERM, lambda *_: stage.stop())
    try:
        stage.run()
    except KeyboardInterrupt:
        stage.stop()
    finally:
        stage.bus.close()
