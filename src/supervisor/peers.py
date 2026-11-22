import queue
import socket
import threading

from common import safe_socket
from common.logger import logger
from common.logger.logger import LogResult
from . import messages

MAX_MESSAGE_SIZE = 1024
INITIAL_RECONNECT_DELAY = 0.5
MAX_RECONNECT_DELAY = 3.0
BACKOFF = 2.0
CONNECT_TIMEOUT = 1.0


def _close_socket(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)     
    except OSError:
        pass
    sock.close()


class PeerHandler:

    class EventType:
        MESSAGE = "MESSAGE"            
        DISCONNECT = "DISCONNECT"    

    def __init__(self, peer_id, events):
        self._peer_id = peer_id
        self._events = events
        self._lock = threading.Lock()  
        self._send_queue = None       
        self._socket = None
        self._closed = False
        self._stop_event = threading.Event()

    def _sender_loop(self, sock, send_queue):
        while True:
            data = send_queue.get()
            if data is None:
                break
            try:
                safe_socket.send_packet(sock, data)
            except (OSError, safe_socket.FrameError) as e:
                logger.debug("peer_send", LogResult.fail, "peer_id", self._peer_id, "error", e)
                _close_socket(sock)
                break

    def _receiver_loop(self, sock):
        while True:
            try:
                data = safe_socket.recv_packet(sock, max_size=MAX_MESSAGE_SIZE)
            except (OSError, safe_socket.FrameError) as e:
                logger.warning("peer_connection_lost", LogResult.fail, "peer_id", self._peer_id, "error", e)
                return
            if data is None:                    
                return
            try:
                message = messages.decode(data)
            except ValueError as e:
                logger.debug("peer_message_ignored", LogResult.fail, "peer_id", self._peer_id, "error", e, "data", data[:100])
                continue
            self._events.put((self.EventType.MESSAGE, self._peer_id, message))

    def _run_session(self, sock):
        logger.info("peer_connected", LogResult.success, "peer_id", self._peer_id)
        send_queue = queue.Queue()
        with self._lock:
            previous, self._socket, self._send_queue = self._socket, sock, send_queue
        if previous is not None:                  
            _close_socket(previous)
        sender = threading.Thread(target=self._sender_loop, args=(sock, send_queue), daemon=True)
        sender.start()
        self._receiver_loop(sock)
        with self._lock:                         
            current = self._socket is sock
            if current:
                self._socket = self._send_queue = None
        _close_socket(sock)
        send_queue.put(None)
        sender.join(timeout=1.0)
        if current and not self._closed:         
            self._events.put((self.EventType.DISCONNECT, self._peer_id, None))

    def _run(self, address, my_id):
        delay = INITIAL_RECONNECT_DELAY
        while not self._closed:
            try:
                sock = socket.create_connection(address(), timeout=CONNECT_TIMEOUT)
                sock.settimeout(None)
                safe_socket.send_packet(sock, messages.encode(messages.hello(my_id)))
                delay = INITIAL_RECONNECT_DELAY
                self._run_session(sock)
            except (OSError, safe_socket.FrameError) as e:
                logger.debug("peer_connect", LogResult.fail, "peer_id", self._peer_id, "error", e)
            if not self._closed:
                self._stop_event.wait(delay)
                delay = min(delay * BACKOFF, MAX_RECONNECT_DELAY)

    def is_connected(self):
        return self._send_queue is not None

    def send(self, data):
        send_queue = self._send_queue
        if send_queue is None:
            return False
        send_queue.put(data)
        return True

    def accept(self, sock):
        threading.Thread(target=self._run_session, args=(sock,), daemon=True).start()

    def connect(self, address, my_id):
        threading.Thread(target=self._run, args=(address, my_id), daemon=True).start()

    def close(self):
        self._closed = True
        self._stop_event.set()
        sock = self._socket
        if sock is not None:
            _close_socket(sock)
