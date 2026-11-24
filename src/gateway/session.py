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
        self.superseded = False   # otra conexión reanudó esta sesión
        self.pending = {}        # msg_id -> ack del broker: resultados enviados que el cliente aún no confirmó
        self._pending_lock = threading.Lock()

    #Envía bajo lock (dos hilos escriben en el mismo socket)
    def send(self, message: dict) -> None:
        with self._send_lock:
            if self._closed:
                raise ConnectionError("sesión cerrada")
            send_frame(self.sock, message)

    #Guarda el ack y el mensaje hasta el RESULT_ACK
    def hold(self, msg_id, ack, message=None) -> None:
        with self._pending_lock:
            self.pending[msg_id] = (ack, message)

    #El cliente confirmó, se devuelve el ack para ejecutarlo
    def confirm(self, msg_id):
        with self._pending_lock:
            entry = self.pending.pop(msg_id, None)
        return entry[0] if entry else None

    #Devuelve los mensajes pendientes de confirmación (para reenviarlos al reconectar)
    def unconfirmed(self) -> list:
        with self._pending_lock:
            return [m for _ack, m in self.pending.values() if m is not None]

    #Vacía los pendientes y devuelve sus acks
    def drain_pending(self) -> list:
        with self._pending_lock:
            acks = [ack for ack, _m in self.pending.values()]
            self.pending.clear()
            return acks

    #Luego de DONE, shutdown(SHUT_WR) sin cerrar el socket
    def finish(self) -> None:
        with self._send_lock:
            self._closed = True
            try:
                self.sock.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    #Cierra el socket y marca la sesión como cerrada
    def close(self) -> None:
        self._closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR) 
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass

    @property
    def closed(self) -> bool:
        return self._closed
