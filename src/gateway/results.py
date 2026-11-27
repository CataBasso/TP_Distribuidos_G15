"""Reenvío de resultados del broker a los clientes.

Cada resultado que llega de las etapas se manda al cliente dueño. Si trae `msg_id`, el ack al broker
se RETIENE hasta que el cliente confirme con RESULT_ACK: así un resultado nunca se da por entregado
si el cliente cayó antes de guardarlo.
"""
from common.logger import logger
from common.logger.logger import LogResult
from common.protocol import (
    TYPE_NAMES, RESULT, EOF, decode, done_message, result_message, with_id,
)

from .session import ClientSession

_RESULT, _EOF = TYPE_NAMES[RESULT], TYPE_NAMES[EOF]


class ResultForwarder:
    def __init__(self, gw):
        self.gw = gw

    #Recibe cada resultado de las etapas y descarta lo ilegible o de otro Gateway. Un RESULT con msg_id se retiene hasta el RESULT_ACK.
    def send_done(self, session: ClientSession) -> None:
        gw = self.gw
        session.send(done_message(session.client_id))
        session.done = True
        gw.resume.complete.discard(session.n)
        gw.journal.close(session.n)
        session.finish()
        logger.info("client_results_completed", LogResult.success, "client_id", session.client_id)
    
    #Cuenta un EOF de resultados: lo persiste en el journal (antes de confirmarlo) y, si con él están todos los de la sesión, manda el DONE
    def _result_eof(self, session: ClientSession, message: dict) -> None:
        gw = self.gw
        try:
            query, origin = message.get("query"), message.get("origin", 0)
            complete = gw.tracker.add(session.client_id, query, origin)
            gw.journal.result_eof(session.n, query, origin)     # persistir antes del ack
        except ValueError as e:
            logger.warning("invalid_result_eof", LogResult.fail,
                           "client_id", session.client_id, "reason", e)
            complete = False
        if complete:
            gw.resume.complete.add(session.n)
            self.send_done(session)

    #Se ejecuta en el hilo de resultados cuando el cliente confirmó todas sus filas: procesa los EOF que estaban esperando y recién entonces los confirma al broker
    def release_eofs(self, session: ClientSession) -> None:
        for message, ack in session.release_eofs():
            try:
                self._result_eof(session, message)
            except OSError as e:
                logger.warning("client_not_responding", LogResult.fail,
                               "client_id", session.client_id, "error", e)
            ack()

    #Manda DONE, cierra la sesión en el journal y cierra sólo la escritura del socket
    def on_result(self, body: bytes, ack, nack):
        gw = self.gw
        try:
            message = decode(body)
            gw_id, n = message["client_id"]
        except (ValueError, KeyError, TypeError, IndexError):
            logger.error("invalid_result_message", LogResult.fail, "gw_id", gw.gw_id,
                         "reason", "mensaje ilegible en results_q")
            ack()
            return
        if gw_id != gw.gw_id:
            logger.error("result_for_wrong_gateway", LogResult.fail, "expected_gw_id", gw.gw_id,
                         "received_gw_id", gw_id)
            ack()
            return
        with gw._sessions_lock:
            session = gw._sessions.get(n)
        if session is None:
            logger.info("result_for_disconnected_client", LogResult.in_progress,
                        "client_id", (gw_id, n))
            ack()
            return
        try:
            kind = message["type"]
            if kind == _RESULT:
                out = result_message(session.client_id, message["query"], message["rows"])
                mid = message.get("msg_id")
                if mid is not None:
                    with_id(out, mid)
                    session.hold(mid, ack, out)  # se confirma con el RESULT_ACK del cliente
                session.send(out)
                if mid is not None:
                    return
            elif kind == _EOF:
                if session.defer_eof((message, ack)):
                    return          
                self._result_eof(session, message)
            else:
                logger.error("unknown_result_type", LogResult.fail,
                             "client_id", session.client_id, "type", kind)
        except OSError as e:
            logger.warning("client_not_responding", LogResult.fail,
                           "client_id", session.client_id, "error", e)
            if message.get("msg_id") is not None:
                return      # sigue pendiente: se reenvía si el cliente reanuda, o se libera al cerrar la sesión
        ack()

    