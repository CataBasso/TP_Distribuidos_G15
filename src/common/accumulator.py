import logging

from common.bounded_set import BoundedSet
from common.ft.ids import client_key, parse_client_key
from common.ft.store import StateStore
from common.stage import EofCounter, Stage

log = logging.getLogger(__name__)
DONE_KEPT = 2000


class AccumulatorStage(Stage):
   
    data_type = None

    #Cuantos EOF hay que recibir (uno por réplica anterior)
    def __init__(self, bus, upstream_replicas: int, state_dir=None):
        super().__init__(bus)
        self.eofs = EofCounter(upstream_replicas)
        self.state = {}       # ck -> estado del cliente
        self.seen = {}        # ck -> {msg_id} ya incorporados
        self.done = {}        # ck -> True (terminados, los más recientes)
        self.aborted_keys = BoundedSet()
        self.store = self.durable(StateStore(state_dir, self._apply, self._dump, self._load))
        self.store.recover()

    def new_state(self):
        raise NotImplementedError

    #Qué guardar en el WAL
    def payload(self, message):
        raise NotImplementedError

    def merge(self, state, payload):
        raise NotImplementedError

    def finish(self, client_id, state) -> None:
        raise NotImplementedError

    def dump_state(self, state):
        return state

    def load_state(self, raw):
        return raw

    #Libera estado, ids vistos y EOF de un cliente
    def _forget(self, ck):
        self.state.pop(ck, None)
        self.seen.pop(ck, None)
        self.eofs.forget(parse_client_key(ck))

    #Aplica un registro del WAL
    def _apply(self, r):
        op, ck = r[0], r[1]
        if op == "data":
            if ck not in self.state:
                self.state[ck] = self.new_state()
            self.merge(self.state[ck], r[3])
            if r[2] is not None:
                self.seen.setdefault(ck, set()).add(r[2])
        elif op == "eof":
            self.eofs._seen.setdefault(parse_client_key(ck), set()).add(r[2])
        elif op == "done":
            self.done[ck] = True
            while len(self.done) > DONE_KEPT:
                self.done.pop(next(iter(self.done)))
            self._forget(ck)
        elif op == "abort":
            self.aborted_keys.add(ck)
            self._forget(ck)

    def _dump(self):
        return {"state": {ck: self.dump_state(s) for ck, s in self.state.items()},
                "seen": {ck: sorted(s) for ck, s in self.seen.items()},
                "eofs": self.eofs.dump(), "done": list(self.done),
                "aborted": list(self.aborted_keys)}

    def _load(self, s):
        self.state = {ck: self.load_state(raw) for ck, raw in s["state"].items()}
        self.seen = {ck: set(ids) for ck, ids in s["seen"].items()}
        self.eofs.load(s["eofs"])
        self.done = {ck: True for ck in s["done"]}
        self.aborted_keys = BoundedSet()
        for ck in s["aborted"]:
            self.aborted_keys.add(ck)

    #Termina los clientes cuyo EOF ya estaba completo al caer
    def recover_pending(self) -> None:
        for client_id in list(self.eofs._seen):
            self._maybe_finish(client_id)

    #Procesa un dato o un EOF
    def on_message(self, message: dict) -> None:
        client_id = tuple(message["client_id"])
        ck = client_key(client_id)
        if ck in self.done or ck in self.aborted_keys:
            return
        kind = message["type"]
        if kind == self.data_type:
            mid = message.get("msg_id")
            if mid is None or mid not in self.seen.get(ck, ()):
                self.store.commit(["data", ck, mid, self.payload(message)])
        elif kind == "eof":
            origin = message.get("origin", 0)
            if origin not in self.eofs._seen.get(client_id, ()):
                self.store.commit(["eof", ck, origin])
        else:
            log.warning("%s: tipo inesperado %s", self.name, kind)
            return
        self._maybe_finish(client_id)

    #Si llegaron todos los EOF, publica y persiste
    def _maybe_finish(self, client_id) -> None:
        ck = client_key(client_id)
        if ck in self.done or not self.eofs.complete(client_id):
            return
        self.finish(client_id, self.state.get(ck))      
        self.store.commit(["done", ck])                 
        self.after_release()

    #Persiste el abort y libera el estado
    def on_abort(self, client_id) -> None:
        ck = client_key(client_id)
        if ck not in self.aborted_keys:
            self.store.commit(["abort", ck])
            self.after_release()

    def after_release(self) -> None:
        """Se llama cuando se liberó el estado de un cliente (terminó o abortó)."""
