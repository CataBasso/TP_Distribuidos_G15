import os
import socket
import threading
import uuid

import pytest

from common.middleware import (
    RabbitConnection, MessageMiddlewareQueueRabbitMQ, MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareDisconnectedError,
)
from common.protocol import send_frame, recv_frame, encode, decode, result_message, eof_message
from common.topology import expected_result_eofs
from gateway.broker import RabbitGatewayBroker, results_queue_name
from gateway.gateway import Gateway

HOST = os.environ.get("RABBIT_HOST", "localhost")


def broker_available():
    try:
        RabbitConnection(HOST, retries=1, delay=0).close()
        return True
    except MessageMiddlewareDisconnectedError:
        return False


pytestmark = pytest.mark.skipif(not broker_available(), reason=f"no hay RabbitMQ en {HOST}")


def test_batch_goes_through_year_in_and_result_comes_back_through_results_q():
    uid = uuid.uuid4().hex[:8]
    gw_id = 5000 + int(uid[:3], 16) % 4000
    year_q, abort_ex, results_ex = f"year_in_{uid}", f"abort_ex_{uid}", f"results_ex_{uid}"
    results_q = f"results_q_{uid}"
    broker = RabbitGatewayBroker(HOST, gw_id, year_q, abort_ex, durable=False,
                                 results_exchange=results_ex, results_queue=results_q)
    gateway = Gateway(gw_id, broker, "127.0.0.1", 0, expected_eofs=expected_result_eofs(1, 1))
    port = gateway.start()      # espera a que results_q exista antes de aceptar clientes
    received = []

    def system():
        year = MessageMiddlewareQueueRabbitMQ(HOST, year_q, durable=False)
        out = MessageMiddlewareExchangeRabbitMQ(HOST, results_ex, [], exchange_type="direct",
                                                durable=False)

        def publish(message):
            out.send(encode(message), routing_key=str(gw_id))

        def on_message(body, ack, nack):
            msg = decode(body)
            received.append(msg)
            if msg["type"] == "eof":
                cid = msg["client_id"]
                publish(result_message(cid, 3, [{"word": "ada"}]))
                for query in (1, 2, 3, 4, 5):          # un EOF por consulta: el Gateway da el 'done'
                    publish(eof_message(cid, origin=0, query=query))
                year.stop_consuming()
            ack()

        year.start_consuming(on_message)
        year.delete(); year.close(); out.delete(); out.close()

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
        assert [m["type"] for m in received] == ["batch", "eof"]
    finally:
        gateway.stop()
        leftover = MessageMiddlewareQueueRabbitMQ(HOST, results_q, durable=False)
        leftover.delete(); leftover.close()
