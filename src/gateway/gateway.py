import itertools
import logging
import socket
import threading

from common.protocol import (
    FrameError, recv_frame, decode, TYPE_NAMES, BATCH, EOF, RESULT, DONE,
    welcome_message, error_message, result_message, done_message,
)
from common.protocol.internal import serialize_article, serialize_eof, serialize_abort
from .session import ClientSession

log = logging.getLogger("gateway")

# decode()/recv_frame() devuelven el tipo como nombre ("batch"); las constantes son códigos.
_BATCH, _EOF, _RESULT, _DONE = (TYPE_NAMES[t] for t in (BATCH, EOF, RESULT, DONE))


class Gateway:
    def __init__(self, gw_id: int, broker, host: str = "0.0.0.0", port: int = 5000):
        self.gw_id = gw_id
        self.broker = broker
        self.host, self.port = host, port
        self._server = None
        self._sessions: dict[int, ClientSession] = {}
        self._sessions_lock = threading.Lock()
        self._next_n = itertools.count(1)
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    # ------------------------------------------------------------------ ciclo de vida
    def start(self) -> int:
        """Empieza a escuchar y devuelve el puerto real (útil con port=0). No bloquea."""
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind((self.host, self.port))
        self._server.listen(64)
        self._server.settimeout(0.5)            # para poder revisar self._stop
        self.port = self._server.getsockname()[1]
        for target, name in ((self._accept_loop, "accept"), (self._results_loop, "results")):
            t = threading.Thread(target=target, name=f"gw{self.gw_id}-{name}", daemon=True)
            t.start()
            self._threads.append(t)
        log.info("Gateway %d escuchando en %s:%d", self.gw_id, self.host, self.port)
        return self.port

    def wait(self) -> None:
        self._stop.wait()

    def serve_forever(self) -> None:
        self.start()
        self.wait()
        self.stop()

    def stop(self) -> None:
        """Apagado ordenado (SIGTERM): deja de aceptar, corta el consumo de resultados y
        cierra las sesiones; cada hilo de cliente publica el abort de lo que quedó a medias."""
        if self._stop.is_set() and self._server is None:
            return
        self._stop.set()
        try:
            self.broker.stop_results()
        except Exception:
            log.exception("error al detener el consumo de resultados")
        with self._sessions_lock:
            sessions = list(self._sessions.values())
        for s in sessions:
            s.close()
        for t in self._threads:
            t.join(timeout=5)
        if self._server is not None:
            self._server.close()
            self._server = None
        log.info("Gateway %d detenido", self.gw_id)

    # ------------------------------------------------------------------ clientes
    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                conn, addr = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            t = threading.Thread(target=self._handle_client, args=(conn, addr),
                                 name=f"gw{self.gw_id}-client", daemon=True)
            t.start()
            self._threads.append(t)

    def _register(self, conn) -> ClientSession:
        session = ClientSession(conn, (self.gw_id, next(self._next_n)))
        with self._sessions_lock:
            self._sessions[session.n] = session
        return session

    def _unregister(self, session: ClientSession) -> None:
        with self._sessions_lock:
            self._sessions.pop(session.n, None)

    def _handle_client(self, conn, addr):
        session = self._register(conn)
        publisher = None
        cid = session.client_id
        log.info("cliente %s conectado desde %s", cid, addr)
        try:
            publisher = self.broker.open_publisher()
            session.send(welcome_message(cid))
            batch_id = 0
            while True:
                message = recv_frame(conn)
                if message is None:                       # el cliente cerró
                    break
                kind = message.get("type")
                if kind == _BATCH:
                    for row in message["rows"]:
                        publisher.send_year(serialize_article(row, cid, batch_id, self.gw_id))
                    batch_id += 1
                elif kind == _EOF and not session.eof_sent:
                    publisher.send_year(serialize_eof(cid, self.gw_id))
                    session.eof_sent = True
                    log.info("cliente %s terminó de enviar (%d batches)", cid, batch_id)
                else:
                    session.send(error_message("invalid_message", f"mensaje inesperado: {kind}",
                                               cid))
                    break
        except FrameError as e:
            # mensaje que viola el protocolo (campos de más, tipo desconocido, truncado...)
            log.info("cliente %s: %s", cid, e)
            try:
                session.send(error_message("invalid_message", str(e), cid))
            except Exception:
                pass
        except (OSError, ConnectionError) as e:
            log.info("cliente %s desconectado: %s", cid, e)
        except Exception:
            log.exception("error atendiendo al cliente %s", cid)
            try:
                session.send(error_message("internal_error", "error interno del Gateway", cid))
            except Exception:
                pass
        finally:
            if not session.done:
                self._abort(session, publisher)
            self._unregister(session)
            session.close()
            if publisher is not None:
                try:
                    publisher.close()
                except Exception:
                    log.exception("error cerrando el publicador de %s", cid)

    def _abort(self, session: ClientSession, publisher) -> None:
        """El cliente se fue (o el Gateway se apaga) sin haber recibido sus resultados."""
        log.warning("abort del cliente %s", session.client_id)
        try:
            if publisher is None:
                publisher = self.broker.open_publisher()
                try:
                    publisher.send_abort(serialize_abort(session.client_id))
                finally:
                    publisher.close()
            else:
                publisher.send_abort(serialize_abort(session.client_id))
        except Exception:
            log.exception("no se pudo publicar el abort de %s", session.client_id)

    # ------------------------------------------------------------------ resultados
    def _results_loop(self):
        try:
            self.broker.run_results(self._on_result)
        except Exception:
            if not self._stop.is_set():
                log.exception("se cortó el consumo de resultados; apagando el Gateway")
                self._stop.set()

    def _on_result(self, body: bytes, ack, nack):
        """Reenvía un mensaje de out_q.<gw_id> (result o done) al cliente dueño."""
        try:
            message = decode(body)
            gw_id, n = message["client_id"]
        except (ValueError, KeyError, TypeError, IndexError):
            log.error("mensaje ilegible en out_q: se descarta")
            ack()
            return
        if gw_id != self.gw_id:
            log.error("resultado para otro Gateway (%s): se descarta", gw_id)
            ack()
            return
        with self._sessions_lock:
            session = self._sessions.get(n)
        if session is None:
            log.info("resultado para el cliente (%s,%s) que ya no está: se descarta", gw_id, n)
            ack()
            return
        try:
            kind = message["type"]
            if kind == _RESULT:
                session.send(result_message(session.client_id, message["query"], message["rows"]))
            elif kind == _DONE:
                session.send(done_message(session.client_id))
                session.done = True
                session.close()                 # el hilo del cliente termina sin abort
                log.info("cliente %s recibió todos sus resultados", session.client_id)
            else:
                log.error("tipo desconocido en out_q: %s", kind)
        except OSError:
            log.info("cliente %s ya no responde: se descarta el resultado", session.client_id)
        ack()
