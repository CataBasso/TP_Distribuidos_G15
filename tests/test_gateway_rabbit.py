import os
import socket
import threading
import uuid

import pytest

from common.middleware import (
    RabbitConnection, MessageMiddlewareQueueRabbitMQ, MessageMiddlewareDisconnectedError,
)
from common.protocol import send_frame, recv_frame, encode, decode, result_message, done_message
from gateway.broker import RabbitGatewayBroker, out_queue_name
from gateway.gateway import Gateway

HOST = os.environ.get("RABBIT_HOST", "localhost")


def broker_available():
    try:
        RabbitConnection(HOST, retries=1, delay=0).close()
        return True
    except MessageMiddlewareDisconnectedError:
        return False


pytestmark = pytest.mark.skipif(not broker_available(), reason=f"no hay RabbitMQ en {HOST}")


def test_batch_goes_through_year_in_and_result_comes_back_through_out_q():
    uid = uuid.uuid4().hex[:8]
    gw_id = 5000 + int(uid[:3], 16) % 4000
    year_q, abort_ex = f"year_in_{uid}", f"abort_ex_{uid}"
    broker = RabbitGatewayBroker(HOST, gw_id, year_q, abort_ex, durable=False)
    gateway = Gateway(gw_id, broker, "127.0.0.1", 0)
    port = gateway.start()
    received = []

    def system():
        year = MessageMiddlewareQueueRabbitMQ(HOST, year_q, durable=False)
        out = MessageMiddlewareQueueRabbitMQ(HOST, out_queue_name(gw_id), durable=False)

        def on_message(body, ack, nack):
            msg = decode(body)
            received.append(msg)
            if msg["type"] == "eof":
                out.send(encode(result_message(msg["client_id"], 3, [{"word": "ada"}])))
                out.send(encode(done_message(msg["client_id"])))
                year.stop_consuming()
            ack()

        year.start_consuming(on_message)
        year.delete(); year.close(); out.close()

    t = threading.Thread(target=system, daemon=True); t.start()
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=10)
        welcome = recv_frame(s)
        assert welcome["client_id"][0] == gw_id
        send_frame(s, {"type": "batch", "batch_id": 0, "rows": [{"url": "u"}]})
        send_frame(s, {"type": "eof"})
        r = recv_frame(s)
        assert (r["type"], r["query"], r["rows"]) == ("result", 3, [{"word": "ada"}])
        assert recv_frame(s)["type"] == "done"
        t.join(10)
        assert [m["type"] for m in received] == ["article", "eof"]
    finally:
        gateway.stop()
        leftover = MessageMiddlewareQueueRabbitMQ(HOST, out_queue_name(gw_id), durable=False)
        leftover.delete(); leftover.close()
