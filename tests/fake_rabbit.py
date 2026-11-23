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
import time

from common.ft import failpoints
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

    def remove_queue(self, name):
        """Borra una cola exclusiva (al cerrarse la conexión que la creó) y sus bindings."""
        with self.cond:
            self.queues.pop(name, None)
            for ex in self.exchanges.values():
                ex["binds"] = [(q, k) for q, k in ex["binds"] if q != name]

    def requeue(self, queue, bodies):
        """Reentrega al principio de la cola los mensajes que no se confirmaron (orden original)."""
        with self.cond:
            if queue in self.queues:
                for body in reversed(bodies):
                    self.queues[queue].appendleft(body)
            self.cond.notify_all()

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
    """Conexión falsa. Rastrea los mensajes entregados y no confirmados: si la conexión muere
    (kill(), una caída simulada o close()), el broker los reentrega, como RabbitMQ."""

    def __init__(self, host=None, heartbeat=600, retries=10, delay=1.0, confirm=False, prefetch=1, **kw):
        self.broker = BROKER
        self.confirm = confirm
        self.consumers = []                         # (cola, callback)
        self.anon = []                              # colas exclusivas de esta conexión
        self.outstanding = []                       # [cola, cuerpo, estado] entregados sin confirmar
        self._stopped = False
        self._killed = False
        self.crashed = False
        self._dead = False                          # SIGKILL: los acks posteriores no cuentan
        self.periodic = []                          # [intervalo, fn, próximo]

    def add_consumer(self, queue, callback):
        with self.broker.cond:
            self.consumers.append((queue, callback))
            self.broker.cond.notify_all()

    def add_periodic(self, interval, fn):
        self.periodic.append([interval, fn, time.monotonic() + interval])

    def call_threadsafe(self, fn):
        fn()

    def run(self):
        b = self.broker
        try:
            while True:
                with b.cond:
                    while not self._stopped and not self._killed:
                        ready = [(q, cb) for q, cb in self.consumers if b.queues[q]]
                        if ready:
                            break
                        b.cond.wait(timeout=0.2)
                        self._tick()
                    if self._stopped or self._killed:
                        return
                    queue, cb = ready[0]
                    body = b.queues[queue].popleft()
                entry = [queue, body, "open"]
                self.outstanding.append(entry)

                def ack(entry=entry):
                    if not self._dead:
                        entry[2] = "acked"

                def nack(requeue=True, entry=entry):
                    if not self._dead:
                        entry[2] = "requeue" if requeue else "dropped"

                try:
                    cb(body, ack, nack)
                except failpoints.SimulatedCrash:
                    self.crashed = True
                    return
                finally:
                    self._settle()
        finally:
            self._release()

    def _tick(self):
        now = time.monotonic()
        for item in self.periodic:
            if now >= item[2]:
                item[2] = now + item[0]
                item[1]()

    def _settle(self):
        """Después de cada callback: los nack(requeue=True) vuelven a la cola; los ack/dropped se descartan.
        Los mensajes sin ack (acks diferidos del Gateway) siguen pendientes."""
        keep = []
        for entry in self.outstanding:
            if entry[2] == "requeue":
                self.broker.requeue(entry[0], [entry[1]])
            elif entry[2] == "open" and entry is self.outstanding[-1] and False:
                keep.append(entry)
            elif entry[2] == "open":
                keep.append(entry)
        self.outstanding = keep

    def _release(self):
        """La conexión terminó: reentrega lo no confirmado y borra las colas exclusivas."""
        by_queue = {}
        for queue, body, state in self.outstanding:
            if state == "open":
                by_queue.setdefault(queue, []).append(body)
        self.outstanding = []
        for queue, bodies in by_queue.items():
            self.broker.requeue(queue, bodies)
        for name in self.anon:
            self.broker.remove_queue(name)
        self.anon = []

    def kill(self):
        """Caída externa (SIGKILL): lo no confirmado vuelve a la cola YA (el broker detecta que la
        conexión se cortó) y lo que el código "zombi" siga haciendo no confirma nada."""
        self._dead = True
        with self.broker.cond:
            self._killed = True
            self.broker.cond.notify_all()
        self._release()

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
            self.rc.anon.append(self._anon)
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
