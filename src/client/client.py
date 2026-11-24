import logging
import os
import socket
import threading
import time

from common.protocol import (
    FrameError, recv_frame, send_frame, TYPE_NAMES, WELCOME, RESULT, DONE, ERROR,
    resume_message, result_ack_message,
)
from common.protocol.external import send_batch, send_eof
from .dataset import iter_batches, DEFAULT_BATCH_ROWS, DEFAULT_BATCH_BYTES
from .results_writer import ResultWriter

log = logging.getLogger("client")

_WELCOME, _RESULT, _DONE, _ERROR = (TYPE_NAMES[t] for t in (WELCOME, RESULT, DONE, ERROR))


class ClientError(Exception):
    """La sesión terminó sin recibir los resultados completos"""


class _Disconnected(Exception):
    """Se perdió la conexión con el Gateway (se reintenta reanudando la sesión)."""


class Client:

    def __init__(self, host, port, data_dir, output_dir,
                 batch_rows=DEFAULT_BATCH_ROWS, batch_bytes=DEFAULT_BATCH_BYTES,
                 connect_retries=30, retry_delay=1.0, reconnect_retries=None, max_reconnects=None):
        self.host, self.port = host, port
        self.data_dir, self.output_dir = data_dir, output_dir
        self.batch_rows, self.batch_bytes = batch_rows, batch_bytes
        self.connect_retries, self.retry_delay = connect_retries, retry_delay
        # al reconectar se espera más: el Supervisor tarda unos segundos en reiniciar al Gateway
        self.reconnect_retries = reconnect_retries or int(os.environ.get("RECONNECT_RETRIES", "120"))
        self.max_reconnects = max_reconnects if max_reconnects is not None \
            else int(os.environ.get("MAX_RECONNECTS", "50"))
        self.client_id = None
        self._sock = None
        self._send_lock = threading.Lock()     
        self._done = threading.Event()
        self._receiver_error = None
        self._stopping = False
        self._seen = set()                     
        self.reconnects = 0

    #Reintenta; al reconectar espera más
    def _connect(self, retries=None):
        last = None
        retries = retries or self.connect_retries
        for attempt in range(1, retries + 1):
            if self._stopping:
                break
            try:
                return socket.create_connection((self.host, self.port), timeout=10)
            except OSError as e:
                last = e
                log.info("Gateway no disponible (%s), intento %d/%d", e, attempt, retries)
                time.sleep(self.retry_delay)
        raise ClientError(f"no se pudo conectar a {self.host}:{self.port}: {last}")

    #Envío protegido con lock
    def _send(self, message) -> None:
        with self._send_lock:
            send_frame(self._sock, message)

    #Guarda un resultado sólo si su msg_id es nuevo y siempre manda RESULT_ACK, también para los repetidos
    def _receive_results(self, writer: ResultWriter):
        try:
            while True:
                message = recv_frame(self._sock)
                if message is None:
                    raise _Disconnected("el Gateway cerró la conexión antes de enviar 'done'")
                kind = message["type"]
                if kind == _RESULT:
                    mid = message.get("msg_id")
                    if mid is None or mid not in self._seen:
                        writer.handle(message)
                        if mid is not None:
                            self._seen.add(mid)
                    if mid is not None:         # se confirma siempre (también los repetidos)
                        self._send(result_ack_message(mid, self.client_id))
                elif kind == _DONE:
                    self._done.set()
                    return
                elif kind == _ERROR:
                    raise ClientError(f"error del Gateway [{message['code']}]: {message['message']}")
                else:
                    log.warning("mensaje inesperado del Gateway: %s", kind)
        except (OSError, FrameError, _Disconnected) as e:
            if not self._stopping:
                self._receiver_error = _Disconnected(str(e))
        except Exception as e:   # error al escribir un resultado, ClientError del Gateway, etc.
            self._receiver_error = e
        finally:
            self._done.set()

    #Primera vez: toma el client_id del WELCOME. Al reconectar: manda RESUME y lee next_batch y eof_sent
    def _handshake(self):
        """Devuelve (próximo batch_id a enviar, si el EOF ya se publicó)."""
        welcome = recv_frame(self._sock)
        if not welcome or welcome["type"] != _WELCOME:
            raise _Disconnected(f"se esperaba 'welcome', llegó {welcome}")
        if self.client_id is None:
            self.client_id = tuple(welcome["client_id"])
            log.info("conectado como client_id=%s", self.client_id)
            return 0, False
        # reconexión: el 'welcome' recibido es de una sesión provisoria; se reanuda la propia
        self._send(resume_message(self.client_id))
        resumed = recv_frame(self._sock)
        if not resumed:
            raise _Disconnected("el Gateway cerró la conexión al reanudar")
        if resumed["type"] == _ERROR:
            raise ClientError(f"no se pudo reanudar la sesión: {resumed['message']}")
        log.info("sesión %s reanudada: el Gateway espera el batch %s", self.client_id,
                 resumed.get("next_batch"))
        return resumed.get("next_batch", 0), bool(resumed.get("eof_sent"))

    #Envía los batches que faltan y el EOF si corresponde. Devuelve la cantidad de filas enviadas en esta conexión
    def _send_dataset(self, next_batch: int, eof_sent: bool) -> int:
        sent_rows = 0
        for batch_id, rows in enumerate(iter_batches(self.data_dir, self.batch_rows, self.batch_bytes)):
            if self._done.is_set():
                return sent_rows
            if batch_id < next_batch:
                continue                         
            with self._send_lock:
                send_batch(self._sock, batch_id, rows)
            sent_rows += len(rows)
        if not eof_sent:
            with self._send_lock:
                send_eof(self._sock)
        log.info("dataset enviado (%d filas en esta conexión)", sent_rows)
        return sent_rows

    #Reconecta, reanuda y sigue hasta recibir DONE
    def run(self) -> None:
        writer = ResultWriter(self.output_dir)
        try:
            while True:
                if self.reconnects:
                    self._sock = self._connect(self.reconnect_retries)
                else:
                    self._sock = self._connect()
                self._sock.settimeout(None)
                self._done.clear()
                self._receiver_error = None
                try:
                    next_batch, eof_sent = self._handshake()
                except (OSError, FrameError, _Disconnected) as e:
                    self._sock.close()
                    if self.client_id is None:            # nunca llegó a tener sesión: se reintenta como nueva
                        time.sleep(self.retry_delay)
                        continue
                    self._count_reconnect(e)
                    continue
                receiver = threading.Thread(target=self._receive_results, args=(writer,), daemon=True)
                receiver.start()
                try:
                    self._send_dataset(next_batch, eof_sent)
                except OSError as e:
                    if self._receiver_error is None and not self._stopping:
                        self._receiver_error = _Disconnected(str(e))
                receiver.join()
                try:
                    self._sock.close()
                except OSError:
                    pass
                if self._stopping:
                    log.info("cliente detenido por SIGTERM: sesión cerrada")
                    return
                err = self._receiver_error
                if err is None:
                    log.info("resultados completos: %s", writer.rows_written)
                    return
                if isinstance(err, _Disconnected):
                    self._count_reconnect(err)
                    continue
                raise ClientError(f"sesión interrumpida: {err}") from err
        finally:
            writer.close()

    #Cuenta las reconexiones y falla si se pasa de max_reconnects
    def _count_reconnect(self, cause) -> None:
        self.reconnects += 1
        if self.reconnects > self.max_reconnects:
            raise ClientError(f"demasiadas reconexiones ({self.reconnects}): {cause}")
        log.warning("conexión perdida (%s): se reintenta reanudar la sesión (%d)", cause, self.reconnects)
        time.sleep(self.retry_delay)

    def stop(self) -> None:
        self._stopping = True
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
