from common.bounded_set import BoundedSet
import logging

from common.ft import failpoints
from common.protocol import decode
from common.protocol.messages import encode
from common.topology import ABORT_EX

log = logging.getLogger(__name__)

#Devuelve la tupla de errores que significan "se perdió la conexión"
def _transport_errors():
    from common.middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
    return (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, ConnectionError, OSError)

#Cuenta de qué orígenes llegó el EOF de cada cliente
class EofCounter:
    
    def __init__(self, expected: int):
        self.expected = expected
        self._seen = {}

    #Anota el EOF de un origen. Devuelve True sólo en la llamada que completa el conjunto
    def add(self, client_id, origin) -> bool:
        seen = self._seen.setdefault(tuple(client_id), set())
        before = len(seen)
        seen.add(origin)
        return before < self.expected <= len(seen)

    #Devuelve True si ya llegaron todos los EOF esperados
    def complete(self, client_id) -> bool:
        return len(self._seen.get(tuple(client_id), ())) >= self.expected

    #Borra lo registrado de un cliente
    def forget(self, client_id) -> None:
        self._seen.pop(tuple(client_id), None)

    #Borra lo registrado de todos
    def clear(self) -> None:
        self._seen.clear()

    #Pasa el estado de memoria a un formato guardable.
    def dump(self) -> dict:
        return {f"{c[0]}.{c[1]}": sorted(o) for c, o in self._seen.items()}
    
    #Reconstruye el estado de memoria desde el formato guardable.
    def load(self, state: dict) -> None:
        self._seen = {tuple(int(x) for x in k.split(".")): set(o) for k, o in state.items()}


class Stage:
    name = "stage"

    def __init__(self, bus):
        self.bus = bus
        self.aborted = BoundedSet()
        self._durables = []    

    #Registra un StateStore o BatchEof para compactarlo periódicamente
    def durable(self, obj):
        self._durables.append(obj)
        return obj

    def start(self) -> None:
        raise NotImplementedError

    def on_abort(self, client_id) -> None:
        raise NotImplementedError

    def run(self) -> None:
        self.start()
        self.bus.run()

    def stop(self) -> None:
        self.bus.stop()

    #Publica un mensaje y marca el failpoint after_publish
    def publish(self, exchange, message, key="") -> None:
        self.bus.send(exchange, encode(message), key=key)
        failpoints.hit("after_publish")

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
        except _transport_errors():
            raise
        except Exception:
            log.exception("%s: abort inválido", self.name)
        finally:
            ack()

    def handler(self, fn):
        transport = _transport_errors()

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
            except transport:
                raise                  #crash-only: el proceso termina y lo reinicia el Supervisor
            except Exception:
                log.exception("%s: error procesando %s", self.name, message.get("type"))
                nack(requeue=False)
                return
            for d in self._durables:
                d.checkpoint()
            failpoints.hit("before_ack")
            ack()
            failpoints.hit("after_ack")
        return callback
