import logging
import os
from enum import StrEnum
from itertools import zip_longest


class LogResult(StrEnum):
    in_progress = "in-progress"
    fail = "fail"
    success = "success"


def init(component=None, level=None):
    """Configura el logging del proceso. component se imprime en cada línea."""
    level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    name = f" {component.replace('%', '%%')}" if component else ""
    logging.basicConfig(
        level=level,
        format=f"%(asctime)s %(levelname)s{name} %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )


def _format_value(value) -> str:
    text = str(value)
    return f'"{text}"' if not text or " " in text else text


def _parse_args(args) -> str:
    # Si falta un valor, se muestra "?" en vez de perder el dato o romper el proceso.
    pairs = zip_longest(args[::2], args[1::2], fillvalue="?")
    return " ".join(f"{key}={_format_value(value)}" for key, value in pairs)


def _log(level, action, result, args):
    if logging.getLogger().isEnabledFor(level):
        logging.log(level, f"action={action} result={result} {_parse_args(args)}".rstrip())


def debug(action: str, result: LogResult, *args):
    _log(logging.DEBUG, action, result, args)


def info(action: str, result: LogResult, *args):
    _log(logging.INFO, action, result, args)


def warning(action: str, result: LogResult, *args):
    _log(logging.WARNING, action, result, args)


def error(action: str, result: LogResult, *args):
    _log(logging.ERROR, action, result, args)