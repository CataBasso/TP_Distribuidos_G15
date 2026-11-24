import itertools
import os
import time
import socket
import threading

from common.ft import failpoints
from common.logger.logger import LogResult
from common.logger import logger
from common.protocol import (
    FrameError, recv_frame, TYPE_NAMES, BATCH, EOF, RESUME, RESULT_ACK,
    welcome_message, error_message,
)
from common.protocol import batch_message, eof_message
from common.protocol.internal import serialize, serialize_abort
from common.topology import expected_result_eofs
from .journal import SessionJournal
from .results import ResultForwarder
from .results_tracker import ResultTracker
from .resume import ResumeManager
from .session import ClientSession

_BATCH, _EOF, _RESUME, _RESULT_ACK = (
    TYPE_NAMES[t] for t in (BATCH, EOF, RESUME, RESULT_ACK))


class Gateway:

    #Crea el journal, calcula el próximo n de sesión y  ResultForwarder y ResumeManager
    def __init__(self, gw_id: int, broker, host: str = "0.0.0.0", port: int = 5000,
                 expected_eofs: dict = None, epoch: int = None, state_dir: str = None,
                 resume_grace: float = None):
        self.gw_id = gw_id
        self.broker = broker
        self.expected_eofs = expected_eofs or expected_result_eofs(1, 1)
        self.tracker = ResultTracker(self.expected_eofs)
        self.host, self.port = host, port
        self._server = None
        self._sessions: dict[int, ClientSession] = {}
        self._sessions_lock = threading.Lock()
        if epoch is None:
            epoch = int(os.environ.get("GATEWAY_EPOCH", time.time()))
        self.journal = SessionJournal(state_dir)
        self._next_n = itertools.count(max((epoch << 20) | 1, self.journal.last_n + 1))
        grace = float(os.environ.get("RESUME_GRACE", "30")) if resume_grace is None else resume_grace
        self.results = ResultForwarder(self)          # reenvío de resultados (results.py)
        self.resume = ResumeManager(self, grace)      # reanudación de sesiones (resume.py)
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    #Empieza a escuchar y devuelve el puerto real
    def start(self) -> int:
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind((self.host, self.port))
        self._server.listen(64)
        self._server.settimeout(0.5)
        self.port = self._server.getsockname()[1]

        results = threading.Thread(target=self._results_loop, name=f"gw{self.gw_id}-results",
                                   daemon=True)
        results.start()
        self._threads.append(results)
        ready = getattr(self.broker, "results_ready", None)
        if ready is not None and not self.resume.unresumed and not ready.wait(timeout=30):
            logger.warning("results_queue_not_ready", LogResult.fail, "gw_id", self.gw_id)
        accept = threading.Thread(target=self._accept_loop, name=f"gw{self.gw_id}-accept",
                                  daemon=True)
        accept.start()
        self._threads.append(accept)
        logger.info("gateway_started", LogResult.success, "gw_id", self.gw_id, "host", self.host,
                    "port", self.port, "sessions_to_resume", len(self.resume.unresumed))
        return self.port

    #Bloquea hasta que se pida parar
    def wait(self) -> None:
        self._stop.wait()

    def serve_forever(self) -> None:
        self.start()
        self.wait()
        self.stop()

    #Stop ordenado. No aborta a los clientes, sus sesiones siguen en el journal
    def stop(self) -> None:
        if self._stop.is_set() and self._server is None:
            return
        self._stop.set()
        try:
            self.broker.stop_results()
        except Exception as e:
            logger.error("results_consumer_stop_failed", LogResult.fail, "gw_id", self.gw_id,
                         "error", e)
        with self._sessions_lock:
            sessions = list(self._sessions.values())
        for s in sessions:
            s.close()
        for t in self._threads:
            t.join(timeout=5)
        if self._server is not None:
            self._server.close()
            self._server = None
        self.journal.store.close()
        logger.info("gateway_stopped", LogResult.success, "gw_id", self.gw_id)

    crash = stop    

    #Acepta conexiones de clientes y crea un hilo para cada uno
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

    #Crea la sesión con un client_id nuevo
    def _register(self, conn) -> ClientSession:
        n = next(self._next_n)
        session = ClientSession(conn, (self.gw_id, n))
        with self._sessions_lock:
            self._sessions[n] = session
        return session

    #Borra la sesión de la lista de activas
    def _unregister(self, session: ClientSession) -> None:
        with self._sessions_lock:
            if self._sessions.get(session.n) is session:
                del self._sessions[session.n]

    #Protocolo de un cliente: recibe mensajes y los publica en el broker, reenvia resultados, confirma ACKs, etc.
    def _handle_client(self, conn, addr):
        session = self._register(conn)
        publisher = None
        cid = session.client_id
        logger.info("client_connected", LogResult.in_progress, "client_id", cid, "address", addr)
        try:
            publisher = self.broker.open_publisher()
            self.journal.open(session.n)       
            session.send(welcome_message(cid))
            first = True                   
            while True:
                message = recv_frame(conn)
                if message is None:
                    break
                kind = message.get("type")
                if kind == _RESUME and first:
                    first = False
                    resumed = self.resume.resume(conn, session, message)
                    if resumed is None:
                        break
                    session = resumed
                    cid = session.client_id
                    self.resume.maybe_done(session)
                    continue
                first = False
                rec = self.journal.sessions.get(session.n)
                if rec is None and kind in (_BATCH, _EOF):
                    continue                   
                if kind == _BATCH:
                    batch_id = rec["next"]
                    publisher.send_year(serialize(
                        batch_message(cid, batch_id, message["rows"], origin=self.gw_id)))
                    failpoints.hit("gw_after_publish_batch")
                    self.journal.batch(session.n, batch_id + 1)
                elif kind == _EOF:
                    if rec["eof"] is None:
                        publisher.send_year(serialize(
                            eof_message(cid, origin=self.gw_id, total=rec["next"])))
                        failpoints.hit("gw_after_publish_eof")
                        self.journal.eof(session.n, rec["next"])
                        session.eof_sent = True
                        logger.info("client_eof_received", LogResult.success, "client_id", cid,
                                    "batches", rec["next"])
                elif kind == _RESULT_ACK:
                    ack = session.confirm(message["msg_id"])
                    if ack is not None:
                        self.broker.defer(ack)
                else:
                    session.send(error_message("invalid_message", f"mensaje inesperado: {kind}",
                                               cid))
                    break
        except FrameError as e:
            logger.warning("invalid_client_message", LogResult.fail, "client_id", cid, "reason", e)
            try:
                session.send(error_message("invalid_message", str(e), cid))
            except Exception:
                pass
        except (OSError, ConnectionError) as e:
            logger.warning("client_disconnected", LogResult.fail, "client_id", cid, "reason", e)
        except Exception as e:
            logger.error("client_handler_failed", LogResult.fail, "client_id", cid, "error", e)
            try:
                session.send(error_message("internal_error", "error interno del Gateway", cid))
            except Exception:
                pass
        finally:
            self._finish_handler(session, publisher)

    #Cierra el hilo: si el Gateway se detiene o la sesión se reanudó, no aborta. Si el cliente se fue sin terminar, aborta. Libera pendientes
    def _finish_handler(self, session: ClientSession, publisher) -> None:
        cid = session.client_id
        if self._stop.is_set() or session.superseded:
            pass          
        else:
            if not session.done:
                self._abort(session, publisher)
                self.journal.close(session.n)
            self.tracker.forget(cid)
        for ack in session.drain_pending():     
            if not self._stop.is_set():
                self._safe_defer(ack)
        self._unregister(session)
        session.close()
        if publisher is not None:
            try:
                publisher.close()
            except Exception as e:
                logger.error("publisher_close_failed", LogResult.fail, "client_id", cid, "error", e)

    #Intenta diferir un ACK, manejando posibles errores
    def _safe_defer(self, ack):
        try:
            self.broker.defer(ack)
        except Exception as e:                     
            logger.error("ack_defer_failed", LogResult.fail, "gw_id", self.gw_id, "error", e)

    #Avisa a las etapas por abort_ex que el cliente se abortó
    def _abort(self, session: ClientSession, publisher) -> None:
        logger.warning("client_aborted", LogResult.fail, "client_id", session.client_id)
        try:
            if publisher is None:
                publisher = self.broker.open_publisher()
                try:
                    publisher.send_abort(serialize_abort(session.client_id))
                finally:
                    publisher.close()
            else:
                publisher.send_abort(serialize_abort(session.client_id))
        except Exception as e:
            logger.error("abort_publish_failed", LogResult.fail, "client_id", session.client_id,
                         "error", e)
    
    #Espera o descarta las sesiones por reanudar y consume resultados
    def _results_loop(self):
        try:
            self.resume.wait_and_drop()                  
            self.broker.run_results(self.results.on_result)
        except Exception as e:
            if not self._stop.is_set():
                logger.error("results_consumer_failed", LogResult.fail, "gw_id", self.gw_id,
                             "error", e)
                self._stop.set()