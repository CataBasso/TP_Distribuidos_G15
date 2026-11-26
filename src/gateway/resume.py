import threading
import time
from common.logger import logger
from common.logger.logger import LogResult
from common.protocol import error_message, welcome_message

from .session import ClientSession


class ResumeManager:

    #Carga del journal las sesiones que esperan reanudar y detecta cuáles ya tenían todos sus EOF de resultados
    def __init__(self, gw, grace: float):
        self.gw = gw
        self.grace = grace
        self.unresumed = set(gw.journal.sessions)
        self.complete = set()
        for n, rec in gw.journal.sessions.items():
            for query, origins in rec["reof"].items():
                for origin in origins:
                    if gw.tracker.add((gw.gw_id, n), query, origin):
                        self.complete.add(n)
        self._resumed = threading.Event()
        if not self.unresumed:
            self._resumed.set()

    #Atiende RESUME: valida, reemplaza la sesión vieja, le pasa los resultados sin confirmar y responde WELCOME
    def resume(self, conn, provisional: ClientSession, message: dict):
        gw = self.gw
        gw_id, n = message["client_id"]
        rec = gw.journal.sessions.get(n)
        if gw_id != gw.gw_id or rec is None:
            provisional.send(error_message("unknown_session", f"sesión {gw_id}.{n} desconocida",
                                           (gw_id, n)))
            return None
        gw.journal.close(provisional.n)             
        with gw._sessions_lock:
            old = gw._sessions.pop(n, None)
            gw._sessions.pop(provisional.n, None)
            session = ClientSession(conn, (gw_id, n))
            gw._sessions[n] = session
        if old is not None and old is not provisional:
            old.superseded = True
            session.pending.update(old.pending)    
            old.pending.clear()
            session.eofs.extend(old.eofs)
            old.eofs.clear()
            old.close()
        session.eof_sent = rec["eof"] is not None
        self.unresumed.discard(n)
        if not self.unresumed:
            self._resumed.set()
        session.send(welcome_message(session.client_id, next_batch=rec["next"],
                                     eof_sent=session.eof_sent, resumed=True))
        for out in session.unconfirmed():            # resultados enviados que el cliente nunca confirmó
            session.send(out)
        logger.info("session_resumed", LogResult.success, "client_id", session.client_id,
                    "next_batch", rec["next"], "eof", rec["eof"])
        return session

    #Si los EOF ya estaban completos antes de caer, manda el DONE apenas el cliente reanuda
    def maybe_done(self, session: ClientSession) -> None:
        if session.n in self.complete:
            self.gw.results.send_done(session)

    #Espera hasta RESUME_GRACE y descarta las sesiones que no volvieron
    def wait_and_drop(self) -> None:
        gw = self.gw
        deadline = time.monotonic() + self.grace
        while self.unresumed and not gw._stop.is_set():
            left = deadline - time.monotonic()
            if left <= 0:
                break
            self._resumed.wait(timeout=min(left, 0.2))
        for n in list(self.unresumed):               
            if gw._stop.is_set():
                return
            self._drop(n)

    #Aborta una sesión no reanudada y la borra del journal
    def _drop(self, n: int) -> None:
        gw = self.gw
        cid = (gw.gw_id, n)
        logger.warning("session_not_resumed", LogResult.fail, "client_id", cid)
        session = ClientSession(None, cid)
        try:
            gw._abort(session, None)
        finally:
            gw.journal.close(n)
            gw.tracker.forget(cid)
            self.unresumed.discard(n)
            if not self.unresumed:
                self._resumed.set()