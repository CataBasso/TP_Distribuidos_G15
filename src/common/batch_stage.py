import logging
import os

from common.ft.batch_eof import BatchEof
from common.protocol.messages import encode
from common.stage import Stage

log = logging.getLogger(__name__)


class BatchStage(Stage):
    ctrl_exchange = None
    input_queue = None
    batch_types = ("batch",)
    emit_all = False

    def __init__(self, bus, replica_id: int, state_dir=None):
        super().__init__(bus)
        self.replica_id = replica_id
        self.eof = self.durable(BatchEof(
            self.name, replica_id, self._send_control, self.publish_eof,
            state_dir=os.path.join(state_dir, "eof") if state_dir else None, emit_all=self.emit_all))

    def declare_outputs(self) -> None:
        """Declara las colas de salida (para poder publicar antes de que el consumidor arranque)"""

    def bind_input(self) -> None:
        self.bus.listen(self.input_queue, self.handler(self._on_data))

    def process_batch(self, client_id, message: dict) -> None:
        raise NotImplementedError

    def publish_eof(self, client_id, total: int) -> None:
        raise NotImplementedError

    def start(self) -> None:
        self.declare_outputs()
        self.bus.listen(None, self.handler(self._on_ctrl), self.ctrl_exchange, kind="fanout")
        self.bind_input()
        self.listen_abort()
        self.eof.resync()       

    def _send_control(self, message: dict) -> None:
        self.bus.send(self.ctrl_exchange, encode(message))

    def _on_data(self, message: dict) -> None:
        client_id = tuple(message["client_id"])
        if self.eof.is_closed(client_id):
            return                              
        kind = message["type"]
        if kind in self.batch_types:
            self.process_batch(client_id, message)
            self.eof.batch_done(client_id, message.get("batch_id", 0))
        elif kind in ("eof", "stage_eof"):
            if "total" not in message:
                raise ValueError(f"el EOF de {self.input_queue} debe traer el total de batches")
            self.eof.eof_seen(client_id, message["total"])
        else:
            log.warning("%s: tipo inesperado %s", self.name, kind)

    def _on_ctrl(self, message: dict) -> None:
        if message["type"] == "control":
            self.eof.on_control(message)

    def on_abort(self, client_id) -> None:
        self.eof.abort(client_id)
