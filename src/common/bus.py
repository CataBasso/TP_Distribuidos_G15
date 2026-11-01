from collections import deque
from itertools import count


class InMemoryBus:
    """Broker en memoria, de un solo hilo y determinista. Muchas etapas pueden compartir un
    mismo InMemoryBus (así se prueba el flujo completo). drain() entrega todo lo pendiente."""

    def __init__(self):
        self.queues = {}            # nombre -> deque de cuerpos
        self.consumers = {}         # nombre -> [callback]
        self._rr = {}               # nombre -> índice round-robin
        self.bindings = {}          # exchange -> {"kind": str, "binds": [(queue, key)]}
        self._anon = count()
        self.acks = 0
        self.stopped = False
        self.sent = []              # (exchange, key, body): todo lo publicado, para inspeccionar

    # --- declaración ---
    def declare(self, queue, exchange=None, keys=(), kind="direct"):
        self.queues.setdefault(queue, deque())
        if exchange:
            ex = self.bindings.setdefault(exchange, {"kind": kind, "binds": []})
            for key in (keys or [""]):
                if (queue, key) not in ex["binds"]:
                    ex["binds"].append((queue, key))

    def listen(self, queue, callback, exchange=None, keys=(), kind="direct"):
        if queue is None:
            queue = f"anon.{next(self._anon)}"
        self.declare(queue, exchange, keys, kind)
        self.consumers.setdefault(queue, []).append(callback)
        return queue

    # --- publicación ---
    def send(self, exchange, body, key=""):
        self.sent.append((exchange, key, body))
        if not exchange:
            self.queues.setdefault(key, deque()).append(body)
            return
        ex = self.bindings.get(exchange)
        if ex is None:
            return                                  # exchange sin colas: se pierde, como RabbitMQ
        targets = []
        for queue, bound in ex["binds"]:
            if (ex["kind"] == "fanout" or bound == key) and queue not in targets:
                targets.append(queue)
        for queue in targets:
            self.queues[queue].append(body)

    # --- consumo ---
    def drain(self, max_steps=1_000_000):
        """Entrega mensajes hasta que no quede ninguno con consumidor. Devuelve cuántos."""
        steps = 0
        progressed = True
        while progressed and steps < max_steps and not self.stopped:
            progressed = False
            for name in list(self.consumers):
                q = self.queues[name]
                if not q:
                    continue
                cbs = self.consumers[name]
                idx = self._rr.get(name, 0) % len(cbs)
                self._rr[name] = idx + 1
                body = q.popleft()
                self._deliver(cbs[idx], name, body)
                progressed = True
                steps += 1
        return steps

    def _deliver(self, callback, name, body):
        def ack():
            self.acks += 1

        def nack(requeue=True):
            if requeue:
                self.queues[name].appendleft(body)

        callback(body, ack, nack)

    def pending(self, queue) -> int:
        return len(self.queues.get(queue, ()))

    def bodies(self, exchange=None, key=None):
        return [b for ex, k, b in self.sent
                if (exchange is None or ex == exchange) and (key is None or k == key)]

    def run(self):
        self.drain()

    def stop(self):
        self.stopped = True

    def close(self):
        pass


class RabbitBus:
    """Bus sobre common.middleware. Una conexión (RabbitConnection) por proceso: todas las
    colas que se consumen comparten el hilo, y los callbacks corren de a uno."""

    def __init__(self, host, prefetch=10, durable=True, kinds=None):
        from common.middleware import RabbitConnection
        from common.topology import EXCHANGE_KINDS
        self.host, self.durable = host, durable
        self.kinds = dict(EXCHANGE_KINDS if kinds is None else kinds)
        self.rc = RabbitConnection(host, prefetch=prefetch)
        self._endpoints = {}
        self._consumers = []

    def _exchange(self, exchange, kind, keys=(), queue=None):
        from common.middleware import MessageMiddlewareExchangeRabbitMQ
        return MessageMiddlewareExchangeRabbitMQ(
            self.host, exchange, list(keys), exchange_type=kind, queue_name=queue,
            durable=self.durable, connection=self.rc)

    def _queue(self, queue):
        from common.middleware import MessageMiddlewareQueueRabbitMQ
        return MessageMiddlewareQueueRabbitMQ(self.host, queue, connection=self.rc,
                                              durable=self.durable)

    def declare(self, queue, exchange=None, keys=(), kind="direct"):
        if exchange:
            self._exchange(exchange, self.kinds.get(exchange, kind), keys, queue)
        else:
            self._queue(queue)

    def listen(self, queue, callback, exchange=None, keys=(), kind="direct"):
        if exchange:
            ep = self._exchange(exchange, self.kinds.get(exchange, kind), keys, queue)
        else:
            ep = self._queue(queue)
        ep.consume(callback)
        self._consumers.append(ep)
        return queue

    def send(self, exchange, body, key=""):
        if not exchange:
            ep = self._endpoints.get(("", key)) or self._endpoints.setdefault(("", key), self._queue(key))
            ep.send(body)
            return
        ep = self._endpoints.get(exchange)
        if ep is None:
            kind = self.kinds.get(exchange, "direct")
            ep = self._endpoints.setdefault(exchange, self._exchange(exchange, kind))
        ep.send(body, key)

    def run(self):
        self.rc.run()

    def stop(self):
        self.rc.stop()

    def close(self):
        self.rc.close()
