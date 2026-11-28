import signal
import sys
import os

from .broker import RabbitGatewayBroker
from .config import GatewayConfig
from .gateway import Gateway
from common.health import start_from_env
from common.logger import logger
from common.runner import state_dir
from common.topology import expected_result_eofs

def main() -> int:
    cfg = GatewayConfig.from_env()
    logger.init(component="gateway", level=cfg.log_level)
    gateway = Gateway(cfg.gw_id, RabbitGatewayBroker(cfg.rabbit_host, cfg.gw_id),
                      cfg.host, cfg.port,
                      expected_eofs=expected_result_eofs(cfg.word_partitions, cfg.q5_partitions),
                      state_dir=state_dir(f"gateway_{cfg.gw_id}"))
    stop = lambda *_: gateway._stop.set()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    responder = None
    if os.environ.get("NODE_NAME"):
        responder = start_from_env()
    try:
        gateway.serve_forever()
    finally:
        if responder is not None:
            responder.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
