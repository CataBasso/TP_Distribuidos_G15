"""python -m gateway   (configuración por variables de entorno, ver config.py)"""
import signal
import sys

from .broker import RabbitGatewayBroker
from .config import GatewayConfig
from .gateway import Gateway
from common.logger import logger

def main() -> int:
    cfg = GatewayConfig.from_env()
    logger.init(component="gateway", level=cfg.log_level)
    gateway = Gateway(cfg.gw_id, RabbitGatewayBroker(cfg.rabbit_host, cfg.gw_id),
                      cfg.host, cfg.port)
    stop = lambda *_: gateway._stop.set()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    gateway.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
