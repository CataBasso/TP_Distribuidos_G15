import logging
import subprocess

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
            logging.error("no se pudo reiniciar '%s': %s", node, e)
            return False
        
        if result.returncode != 0:
            return False
        logging.info("nodo '%s' reiniciado", node)
        return True

    def _command(self, node):
        return ['docker', 'restart', '-t', str(self.stop_timeout), node]

