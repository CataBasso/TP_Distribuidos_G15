import subprocess

from common.logger import logger
from common.logger.logger import LogResult

STOP_TIMEOUT = 3     
RUN_TIMEOUT = 30    

class Restarter:

    def __init__(self, run=subprocess.run, stop_timeout=STOP_TIMEOUT, run_timeout=RUN_TIMEOUT):
        self.run = run                     
        self.stop_timeout = stop_timeout
        self.run_timeout = run_timeout

    def restart(self, node):
        try:
            result = self.run(self._command(node), capture_output=True, text=True,
                          timeout=self.run_timeout)
        except (OSError, subprocess.TimeoutExpired) as e:
            logger.error("node_restart", LogResult.fail, "node", node, "error", e)
            return False
        
        if result.returncode != 0:
            logger.error("node_restart", LogResult.fail, "node", node, "stderr", result.stderr.strip())
            return False
        logger.info("node_restart", LogResult.success, "node", node)
        return True

    def _command(self, node):
        return ['docker', 'restart', '-t', str(self.stop_timeout), node]

