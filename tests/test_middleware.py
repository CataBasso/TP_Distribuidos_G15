import os
import threading
import uuid

from common.middleware import (
    RabbitConnection, MessageMiddlewareQueueRabbitMQ, MessageMiddlewareExchangeRabbitMQ, partition,
)

HOST = os.environ.get("RABBIT_HOST", "localhost")


def uid():
    return uuid.uuid4().hex[:8]


def drain(endpoint, n=1):
    """Consume n mensajes y devuelve sus cuerpos."""
    got = []

    def on_message(body, ack, nack):
        got.append(body)
        ack()
        if len(got) == n:
            endpoint.stop_consuming()

    endpoint.start_consuming(on_message)
    return got


def test_queue_send_and_receive():
    q = MessageMiddlewareQueueRabbitMQ(HOST, f"t_{uid()}", durable=False)
    q.send(b"hola")
    assert drain(q) == [b"hola"]
    q.delete()
    q.close()


def test_direct_routes_by_key():
    ex = f"dx_{uid()}"
    p0 = MessageMiddlewareExchangeRabbitMQ(HOST, ex, ["0"], queue_name=f"p0_{uid()}", durable=False)
    p1 = MessageMiddlewareExchangeRabbitMQ(HOST, ex, ["1"], queue_name=f"p1_{uid()}", durable=False)
    prod = MessageMiddlewareExchangeRabbitMQ(HOST, ex, [], durable=False)
    prod.send(b"para-0", routing_key="0")
    prod.send(b"para-1", routing_key="1")
    assert drain(p0) == [b"para-0"]
    assert drain(p1) == [b"para-1"]
    p0.delete(); p1.delete()


def test_fanout_reaches_every_queue():
    ex = f"fx_{uid()}"
    a = MessageMiddlewareExchangeRabbitMQ(HOST, ex, [], exchange_type="fanout",
                                          queue_name=f"a_{uid()}", durable=False)
    b = MessageMiddlewareExchangeRabbitMQ(HOST, ex, [], exchange_type="fanout",
                                          queue_name=f"b_{uid()}", durable=False)
    prod = MessageMiddlewareExchangeRabbitMQ(HOST, ex, [], exchange_type="fanout", durable=False)
    prod.send(b"hola")
    assert drain(a) == [b"hola"]
    assert drain(b) == [b"hola"]
    a.delete(); b.delete()


def test_named_queue_keeps_messages_sent_before_consuming():
    ex = f"nx_{uid()}"
    cons = MessageMiddlewareExchangeRabbitMQ(HOST, ex, ["k"], queue_name=f"q_{uid()}", durable=False)
    prod = MessageMiddlewareExchangeRabbitMQ(HOST, ex, ["k"], durable=False)
    prod.send(b"temprano")                 # el consumidor todavía no está escuchando
    assert drain(cons) == [b"temprano"]
    cons.delete()


def test_two_queues_one_connection():
    rc = RabbitConnection(HOST)
    a = MessageMiddlewareQueueRabbitMQ(HOST, f"a_{uid()}", connection=rc, durable=False)
    b = MessageMiddlewareQueueRabbitMQ(HOST, f"b_{uid()}", connection=rc, durable=False)
    a.send(b"A")
    b.send(b"B")
    got = []

    def on_message(body, ack, nack):
        got.append(body)
        ack()
        if len(got) == 2:
            rc.stop()

    a.consume(on_message)
    b.consume(on_message)
    rc.run()
    assert sorted(got) == [b"A", b"B"]
    a.delete(); b.delete()
    rc.close()


def test_stop_from_another_thread():
    rc = RabbitConnection(HOST)
    q = MessageMiddlewareQueueRabbitMQ(HOST, f"s_{uid()}", connection=rc, durable=False)
    q.consume(lambda body, ack, nack: ack())
    threading.Timer(0.3, rc.stop).start()
    rc.run()                               # si no vuelve, corta el timeout del test
    q.delete()
    rc.close()

def test_partition_is_deterministic_and_in_range():
    assert partition("ada", 4) == partition("ada", 4)
    assert all(0 <= partition(f"w{i}", 5) < 5 for i in range(1000))


def test_partition_uses_every_partition():
    assert {partition(f"w{i}", 4) for i in range(1000)} == {0, 1, 2, 3}