import socket
import threading
from pathlib import Path

import pytest

from client.client import Client, ClientError
from common.protocol import send_frame, recv_frame

SAMPLE = Path("data/sample/small_dataset")
pytestmark = pytest.mark.skipif(not SAMPLE.exists(), reason="falta el dataset de ejemplo")


class FakeGateway:
    """Acepta un cliente, le manda welcome, lee hasta eof y responde según el guion."""
    def __init__(self, script):
        self.server = socket.socket(); self.server.bind(("127.0.0.1", 0)); self.server.listen(1)
        self.port = self.server.getsockname()[1]
        self.script = script
        self.received = []
        self.thread = threading.Thread(target=self._serve, daemon=True); self.thread.start()

    def _serve(self):
        conn, _ = self.server.accept()
        send_frame(conn, {"type": "welcome", "client_id": (1, 7)})
        while True:
            msg = recv_frame(conn)
            if msg is None:
                break
            self.received.append(msg)
            if msg["type"] == "eof":
                self.script(conn)
                break
        conn.close(); self.server.close()


def make_client(port, tmp_path, **kw):
    return Client("127.0.0.1", port, SAMPLE, tmp_path / "out", batch_rows=500,
                  connect_retries=2, retry_delay=0.05, **kw)


def test_sends_all_batches_then_eof_and_writes_results(tmp_path):
    def script(conn):
        send_frame(conn, {"type": "result", "query": 3, "rows": [{"word": "ada"}]})
        send_frame(conn, {"type": "done"})
    gw = FakeGateway(script)
    c = make_client(gw.port, tmp_path)
    c.run(); gw.thread.join(5)
    kinds = [m["type"] for m in gw.received]
    assert kinds[-1] == "eof" and set(kinds[:-1]) == {"batch"}
    ids = [m["batch_id"] for m in gw.received if m["type"] == "batch"]
    assert ids == list(range(len(ids)))                  # el protocolo exige batch_id
    assert sum(len(m["rows"]) for m in gw.received if m["type"] == "batch") == 2246
    assert c.client_id == (1, 7)
    assert (tmp_path / "out" / "output_query_3.csv").read_text().splitlines() == ["word", "ada"]


def test_fails_if_gateway_closes_without_done(tmp_path):
    gw = FakeGateway(lambda conn: None)
    with pytest.raises(ClientError):
        make_client(gw.port, tmp_path).run()


def test_fails_on_gateway_error(tmp_path):
    gw = FakeGateway(lambda conn: send_frame(conn, {"type": "error", "code": "x", "message": "boom"}))
    with pytest.raises(ClientError, match="boom"):
        make_client(gw.port, tmp_path).run()


def test_connect_failure_after_retries(tmp_path):
    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    with pytest.raises(ClientError, match="no se pudo conectar"):
        make_client(port, tmp_path).run()
