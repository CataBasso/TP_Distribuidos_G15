"""
Base común de las etapas con estado por cliente (LengthMapper, AvgReducer, Q5Filter, Joiner).

Qué resuelve:
  * decodificar los mensajes (uno inválido se descarta con un log, no tira la etapa),
  * escuchar abort_ex y avisar a la etapa para que libere el estado del cliente,
  * ignorar los mensajes tardíos de un cliente abortado,
  * contar EOF por cliente (EofCounter).

Cada subclase implementa on_abort(client_id) y registra sus colas en start().
"""
import logging

from common.protocol import decode
from common.topology import ABORT_EX

log = logging.getLogger(__name__)

#Cuenta, por cliente, de cuantos orígenes distintos llego un EOF
class EofCounter:

    def __init__(self, expected: int):
        self.expected = expected
        self._seen = {}

    def add(self, client_id, origin) -> bool:
        seen = self._seen.setdefault(tuple(client_id), set())
        before = len(seen)
        seen.add(origin)
        return before < self.expected <= len(seen)

    def complete(self, client_id) -> bool:
        return len(self._seen.get(tuple(client_id), ())) >= self.expected

    def forget(self, client_id) -> None:
        self._seen.pop(tuple(client_id), None)

    def clear(self) -> None:
        self._seen.clear()


class Stage:
    name = "stage"

    def __init__(self, bus):
        self.bus = bus
        self.aborted = set()

    def start(self) -> None:
        raise NotImplementedError

    def on_abort(self, client_id) -> None:
        raise NotImplementedError

    def run(self) -> None:
        self.start()
        self.bus.run()

    def stop(self) -> None:
        self.bus.stop()

    def listen_abort(self) -> None:
        self.bus.listen(None, self._on_abort_body, ABORT_EX, kind="fanout")

    def _on_abort_body(self, body, ack, nack):
        try:
            message = decode(body)
            if message["type"] == "abort" and "client_id" in message:
                client_id = tuple(message["client_id"])
                self.aborted.add(client_id)
                self.on_abort(client_id)
                log.info("%s: abort de %s", self.name, client_id)
        except Exception:
            log.exception("%s: abort inválido", self.name)
        finally:
            ack()

    def handler(self, fn):
        def callback(body, ack, nack):
            try:
                message = decode(body)
            except Exception:
                log.exception("%s: mensaje inválido, se descarta", self.name)
                nack(requeue=False)
                return
            client_id = message.get("client_id")
            if client_id is not None and tuple(client_id) in self.aborted:
                ack()
                return
            try:
                fn(message)
            except Exception:
                log.exception("%s: error procesando %s", self.name, message.get("type"))
                nack(requeue=False)
                return
            ack()
        return callback
