import signal
import sys

from common.logger import logger
from common.logger.logger import LogResult

from .config import SupervisorConfig
from .supervisor import Supervisor


def main() -> int:
    logger.init(component="supervisor")
    try:
        config = SupervisorConfig()
    except ValueError as e:                    
        logger.error("config_invalid", LogResult.fail, "reason", e)
        return 2
    supervisor = Supervisor(config)
    stop = lambda *_: supervisor.stop()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    supervisor.run()                           
    return 0

if __name__ == "__main__":
    sys.exit(main())
