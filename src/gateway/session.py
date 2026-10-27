import socket
import threading

from common.protocol import send_frame


class ClientSession:

    def __init__(self, sock: socket.socket, client_id: tuple):
        self.sock = sock
        self.client_id = client_id #(gw_id, n)
        self.n = client_id[1]
        self.eof_sent = False #el cliente terminó de enviar su dataset
        self.done = False #se le entregaron todos los resultados
        self._send_lock = threading.Lock()
        self._closed = False

    def send(self, message: dict) -> None:
        with self._send_lock:
            if self._closed:
                raise ConnectionError("sesión cerrada")
            send_frame(self.sock, message)

    def close(self) -> None:
        self._closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR) #despierta a quien esté en recv()
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass

    @property
    def closed(self) -> bool:
        return self._closed
