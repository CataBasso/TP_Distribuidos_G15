import socket
import threading
import time

from common.logger import logger
from common.logger.logger import LogResult
from common.runner import env_int

DEFAULT_PORT = 9000
RECV_SIZE = 1024
SOCKET_TIMEOUT = 0.2      

def _parse_ping(data):
    try:
        parts = data.decode().split()
        if len(parts) != 2 or parts[0] != "PING":
            return None
        return int(parts[1])
    except ValueError:                    
        return None

class HealthResponder:

    def __init__(self, node_name, port=DEFAULT_PORT, host=""):
        if not node_name or any(c.isspace() for c in node_name):
            raise ValueError(f"nombre de nodo invalido: {node_name!r}")
        self._node_name = node_name
        self._host = host
        self.port = port            
        self._last_progress = time.monotonic()
        self._sock = None
        self._thread = None
        self._stop_event = threading.Event()

    def touch(self):
        self._last_progress = time.monotonic()

    def start(self):
        if self._thread is not None:
            return
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.bind((self._host, self.port))
            sock.settimeout(SOCKET_TIMEOUT)
        except OSError:
            sock.close()
            raise                             
        self._sock = sock
        self.port = sock.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)   
        self._thread.start()
        logger.info("heartbeat_responder_started", LogResult.success, "node", self._node_name, "port", self.port)

    def stop(self):
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        if self._sock is not None:
            self._sock.close()

    close = stop

    def _serve(self):
        while not self._stop_event.is_set():
            try:
                data, address = self._sock.recvfrom(RECV_SIZE)
            except socket.timeout:
                continue
            except OSError:                  
                return
            seq = _parse_ping(data)
            if seq is None:
                logger.debug("heartbeat_ignored", LogResult.fail, "data", data[:100])
                continue
            try:
                self._sock.sendto(self._pong(seq), address)
            except OSError as e:
                logger.debug("heartbeat_reply", LogResult.fail, "address", address, "error", e)

    def _pong(self, seq):
        progress = time.monotonic() - self._last_progress
        return f"PONG {seq} {self._node_name} {progress:.3f}".encode()


def start_from_env():
    import os
    name = os.environ.get("NODE_NAME")
    if not name:
        raise SystemExit("falta la variable de entorno NODE_NAME")
    responder = HealthResponder(name, env_int("HEARTBEAT_PORT", DEFAULT_PORT))
    responder.start()
    return responder
