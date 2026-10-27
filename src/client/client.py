import logging
import socket
import threading
import time

from common.protocol import (
    FrameError, recv_frame, TYPE_NAMES, WELCOME, RESULT, DONE, ERROR,
)
from common.protocol.external import send_batch, send_eof
from .dataset import iter_batches, DEFAULT_BATCH_ROWS, DEFAULT_BATCH_BYTES
from .results_writer import ResultWriter

log = logging.getLogger("client")

#recv_frame devuelve el tipo como nombre ("result"); las constantes del protocolo son códigos.
_WELCOME, _RESULT, _DONE, _ERROR = (TYPE_NAMES[t] for t in (WELCOME, RESULT, DONE, ERROR))


class ClientError(Exception):
    """La sesión terminó sin recibir los resultados completos"""


class Client:
    def __init__(self, host, port, data_dir, output_dir,
                 batch_rows=DEFAULT_BATCH_ROWS, batch_bytes=DEFAULT_BATCH_BYTES,
                 connect_retries=30, retry_delay=1.0):
        self.host, self.port = host, port
        self.data_dir, self.output_dir = data_dir, output_dir
        self.batch_rows, self.batch_bytes = batch_rows, batch_bytes
        self.connect_retries, self.retry_delay = connect_retries, retry_delay
        self.client_id = None
        self._sock = None
        self._done = threading.Event()
        self._receiver_error = None
        self._stopping = False

    def _connect(self):
        last = None
        for attempt in range(1, self.connect_retries + 1):
            try:
                return socket.create_connection((self.host, self.port), timeout=10)
            except OSError as e:
                last = e
                log.info("Gateway no disponible (%s), intento %d/%d", e, attempt, self.connect_retries)
                time.sleep(self.retry_delay)
        raise ClientError(f"no se pudo conectar a {self.host}:{self.port}: {last}")

    def _receive_results(self, writer: ResultWriter):
        try:
            while True:
                message = recv_frame(self._sock)
                if message is None:
                    raise ClientError("el Gateway cerró la conexión antes de enviar 'done'")
                kind = message["type"]
                if kind == _RESULT:
                    writer.handle(message)
                elif kind == _DONE:
                    self._done.set()
                    return
                elif kind == _ERROR:
                    raise ClientError(f"error del Gateway [{message['code']}]: {message['message']}")
                else:
                    log.warning("mensaje inesperado del Gateway: %s", kind)
        except (OSError, FrameError, ClientError) as e:
            if not self._stopping:
                self._receiver_error = e
        except Exception as e:   # error al escribir un resultado, mensaje mal formado, etc.
            self._receiver_error = e
        finally:
            self._done.set()

    def run(self) -> None:
        sock = self._sock = self._connect()
        sock.settimeout(None)
        writer = None
        try:
            welcome = recv_frame(sock)
            if not welcome or welcome["type"] != _WELCOME:
                raise ClientError(f"se esperaba 'welcome', llegó {welcome}")
            self.client_id = tuple(welcome["client_id"])
            log.info("conectado como client_id=%s", self.client_id)

            writer = ResultWriter(self.output_dir)
            receiver = threading.Thread(target=self._receive_results, args=(writer,), daemon=True)
            receiver.start()

            sent_rows = 0
            try:
                for batch_id, rows in enumerate(iter_batches(self.data_dir, self.batch_rows, self.batch_bytes)):
                    if self._done.is_set():          
                        break
                    send_batch(sock, batch_id, rows)
                    sent_rows += len(rows)
                else:
                    send_eof(sock)
                    log.info("dataset enviado: %d filas", sent_rows)
            except OSError as e:
                if self._receiver_error is None:
                    self._receiver_error = e

            receiver.join()
            if self._receiver_error is not None:
                raise ClientError(f"sesión interrumpida: {self._receiver_error}") \
                    from self._receiver_error
            log.info("resultados completos: %s", writer.rows_written)
        finally:
            if writer is not None:
                writer.close()
            sock.close()

    #Corta la sesión (por ejemplo ante SIGTERM)
    def stop(self) -> None:
        self._stopping = True
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
