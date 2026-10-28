"""python -m gateway   (configuración por variables de entorno, ver config.py)"""
import logging
import signal
import sys

from .broker import RabbitGatewayBroker
from .config import GatewayConfig
from .gateway import Gateway


def main() -> int:
    cfg = GatewayConfig.from_env()
    logging.basicConfig(level=cfg.log_level.upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    gateway = Gateway(cfg.gw_id, RabbitGatewayBroker(cfg.rabbit_host, cfg.gw_id),
                      cfg.host, cfg.port)
    stop = lambda *_: gateway._stop.set()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    gateway.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
