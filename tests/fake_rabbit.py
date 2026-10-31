"""
RabbitMQ falso, en memoria y con hilos, para correr el sistema REAL (Gateway, YearFilter, Q1,
Q2, ImageGetter y las etapas de las tareas 7 y 8) en un solo proceso, sin Docker.

Reemplaza las clases de common.middleware por otras con la misma interfaz:

    RabbitConnection            run() bloquea consumiendo; stop() lo corta; close()
    MessageMiddlewareQueueRabbitMQ(host, cola, connection=, durable=)    send / consume / close
    MessageMiddlewareExchangeRabbitMQ(host, exchange, claves, tipo, queue_name=, ...)  send / consume

Cada RabbitConnection corre en su propio hilo (como un proceso con su conexión). Varios
consumidores de una misma cola compiten; los exchanges direct y fanout rutean como RabbitMQ;
una cola anónima de un exchange es exclusiva de quien la crea.

Uso:
    broker = install()      # parchea common.middleware; devuelve el FakeBroker
    ...                     # crear y correr las etapas
    uninstall()
"""
import collections
import itertools
import threading

import common.middleware as mw_pkg
import common.middleware.middleware_rabbitmq as mw_rabbit


class FakeBroker:
    def __init__(self):
        self.cond = threading.Condition()
        self.queues = collections.defaultdict(collections.deque)
        self.exchanges = {}                         # nombre -> {"kind", "binds": [(cola, clave)]}
        self._anon = itertools.count()
        self.published = []                         # (exchange, clave, cuerpo)

    def declare_queue(self, name):
        with self.cond:
            self.queues[name]

    def declare_exchange(self, name, kind):
        with self.cond:
            ex = self.exchanges.setdefault(name, {"kind": kind, "binds": []})
            if ex["kind"] != kind:
                raise RuntimeError(f"PRECONDITION_FAILED: exchange {name} ya existe como {ex['kind']}")

    def bind(self, queue, exchange, key):
        with self.cond:
            binds = self.exchanges[exchange]["binds"]
            if (queue, key) not in binds:
                binds.append((queue, key))

    def anon_queue(self):
        name = f"amq.gen-{next(self._anon)}"
        self.declare_queue(name)
        return name

    def publish(self, exchange, key, body):
        with self.cond:
            self.published.append((exchange, key, body))
            if not exchange:
                self.queues[key].append(body)
            else:
                ex = self.exchanges[exchange]
                targets = []
                for queue, bound in ex["binds"]:
                    if (ex["kind"] == "fanout" or bound == key) and queue not in targets:
                        targets.append(queue)
                for queue in targets:
                    self.queues[queue].append(body)
            self.cond.notify_all()

    def pending(self, queue):
        with self.cond:
            return len(self.queues[queue])


BROKER = None


class RabbitConnection:
    def __init__(self, host=None, heartbeat=600, retries=10, delay=1.0, confirm=False, prefetch=1, **kw):
        self.broker = BROKER
        self.confirm = confirm
        self.consumers = []                         # (cola, callback)
        self._stopped = False

    def add_consumer(self, queue, callback):
        with self.broker.cond:
            self.consumers.append((queue, callback))
            self.broker.cond.notify_all()

    def run(self):
        b = self.broker
        while True:
            with b.cond:
                while not self._stopped:
                    ready = [(q, cb) for q, cb in self.consumers if b.queues[q]]
                    if ready:
                        break
                    b.cond.wait(timeout=0.2)
                if self._stopped:
                    return
                queue, cb = ready[0]
                body = b.queues[queue].popleft()
            acked = []
            cb(body, lambda: acked.append(True), lambda requeue=True: self._nack(queue, body, requeue))

    def _nack(self, queue, body, requeue):
        if requeue:
            with self.broker.cond:
                self.broker.queues[queue].appendleft(body)
                self.broker.cond.notify_all()

    def stop(self):
        with self.broker.cond:
            self._stopped = True
            self.broker.cond.notify_all()

    def close(self):
        self.stop()


class _Endpoint:
    def _init(self, connection):
        self._own = connection is None
        self.rc = connection if connection is not None else RabbitConnection()
        self.broker = BROKER

    def consume(self, callback):
        self.rc.add_consumer(self._queue_to_consume(), callback)

    def start_consuming(self, callback):
        self.consume(callback)
        self.rc.run()

    def stop_consuming(self):
        self.rc.stop()

    def close(self):
        if self._own:
            self.rc.close()


class MessageMiddlewareQueueRabbitMQ(_Endpoint):
    def __init__(self, host, queue_name, connection=None, durable=True):
        self._init(connection)
        self.queue_name = queue_name
        self.broker.declare_queue(queue_name)

    def _queue_to_consume(self):
        return self.queue_name

    def send(self, message):
        self.broker.publish("", self.queue_name, message)


class MessageMiddlewareExchangeRabbitMQ(_Endpoint):
    def __init__(self, host, exchange_name, routing_keys, exchange_type="direct", queue_name=None,
                 durable=True, connection=None):
        self._init(connection)
        self.exchange_name, self.routing_keys, self.kind = exchange_name, list(routing_keys), exchange_type
        self.queue_name = queue_name
        self._anon = None
        self.broker.declare_exchange(exchange_name, exchange_type)
        if queue_name is not None:
            self.broker.declare_queue(queue_name)
            self._bind(queue_name)

    def _bind(self, queue):
        for key in ([""] if self.kind == "fanout" else self.routing_keys):
            self.broker.bind(queue, self.exchange_name, key)

    def _queue_to_consume(self):
        if self.queue_name is not None:
            return self.queue_name
        if self._anon is None:
            self._anon = self.broker.anon_queue()
            self._bind(self._anon)
        return self._anon

    def send(self, message, routing_key=None):
        if routing_key is None:
            routing_key = self.routing_keys[0] if self.routing_keys else ""
        self.broker.publish(self.exchange_name, routing_key, message)


_ORIGINALS = {}


def install() -> FakeBroker:
    global BROKER
    BROKER = FakeBroker()
    for mod in (mw_pkg, mw_rabbit):
        for name, fake in (("RabbitConnection", RabbitConnection),
                           ("MessageMiddlewareQueueRabbitMQ", MessageMiddlewareQueueRabbitMQ),
                           ("MessageMiddlewareExchangeRabbitMQ", MessageMiddlewareExchangeRabbitMQ)):
            _ORIGINALS[(mod, name)] = getattr(mod, name)
            setattr(mod, name, fake)
    return BROKER


def uninstall():
    for (mod, name), original in _ORIGINALS.items():
        setattr(mod, name, original)
    _ORIGINALS.clear()
