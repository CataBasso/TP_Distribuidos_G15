import signal
import sys

from .broker import RabbitGatewayBroker
from .config import GatewayConfig
from .gateway import Gateway
from common.logger import logger
from common.topology import expected_result_eofs

def main() -> int:
    cfg = GatewayConfig.from_env()
    logger.init(component="gateway", level=cfg.log_level)
    gateway = Gateway(cfg.gw_id, RabbitGatewayBroker(cfg.rabbit_host, cfg.gw_id),
                      cfg.host, cfg.port,
                      expected_eofs=expected_result_eofs(cfg.word_partitions, cfg.q5_partitions))
    stop = lambda *_: gateway._stop.set()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    gateway.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
