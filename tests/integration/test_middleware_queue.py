import os, uuid
from common.middleware import MessageMiddlewareQueueRabbitMQ

HOST = os.environ.get("RABBIT_HOST", "localhost")

def test_send_and_receive():
    q = MessageMiddlewareQueueRabbitMQ(HOST, f"test_{uuid.uuid4().hex}")
    q.send(b"hola")
    received = []

    def on_message(body, ack, nack):
        received.append(body)
        ack()
        q.stop_consuming()

    q.start_consuming(on_message)
    q.close()
    assert received == [b"hola"]