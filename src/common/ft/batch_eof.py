import logging

from common.bounded_set import BoundedSet
from common.ft import failpoints
from common.ft.coverage import covers, from_ranges, to_ranges
from common.ft.ids import client_key, parse_client_key
from common.ft.store import StateStore

log = logging.getLogger(__name__)
DONE_KEPT = 2000
ANNOUNCE_DONE = 64


class BatchEof:
    def __init__(self, stage: str, replica_id: int, send_control, on_complete, state_dir=None,
                 emit_all: bool = False, **journal_kw):
        self.stage, self.replica = stage, replica_id
        self.send_control = send_control
        self.on_complete = on_complete
        self.emit_all = emit_all
        self.own = {}       
        self.total = {}      
        self.holds = set()   
        self.done = {}       
        self.others = {}    
        self.aborted = BoundedSet()
        self.store = StateStore(state_dir, self._apply, self._dump, self._load, **journal_kw)
        self.store.recover()

    def _apply(self, r):
        op, ck = r[0], r[1]
        if op == "own":
            self.own.setdefault(ck, set()).add(r[2])
        elif op == "total":
            self.total[ck] = r[2]
        elif op == "hold":
            self.holds.add(ck)
        elif op == "done":
            self.done[ck] = r[2]
            while len(self.done) > DONE_KEPT:
                self.done.pop(next(iter(self.done)))
            for d in (self.own, self.total, self.others):
                d.pop(ck, None)
            self.holds.discard(ck)
        elif op == "abort":
            self.aborted.add(ck)
            for d in (self.own, self.total, self.others, self.done):
                d.pop(ck, None)
            self.holds.discard(ck)

    def _dump(self):
        return {"own": {ck: to_ranges(s) for ck, s in self.own.items()},
                "total": dict(self.total), "holds": sorted(self.holds),
                "done": dict(self.done), "aborted": list(self.aborted)}

    def _load(self, s):
        self.own = {ck: from_ranges(r) for ck, r in s["own"].items()}
        self.total = dict(s["total"])
        self.holds = set(s["holds"])
        self.done = dict(s["done"])
        self.aborted = BoundedSet()
        for ck in s["aborted"]:
            self.aborted.add(ck)

    def pending(self):
        return [ck for ck in self.total if ck not in self.done]

    def is_closed(self, client_id) -> bool:
        ck = client_key(client_id)
        return ck in self.done or ck in self.aborted

    def batch_done(self, client_id, batch_id) -> None:
        ck = client_key(client_id)
        if ck in self.done or ck in self.aborted:
            return
        if batch_id not in self.own.get(ck, ()):
            self.store.commit(["own", ck, batch_id])
        if ck in self.total:
            self._broadcast(ck)
        self._check(ck)

    def eof_seen(self, client_id, total, holds=True) -> None:
        ck = client_key(client_id)
        if ck in self.done or ck in self.aborted:
            return
        if holds and ck not in self.holds:
            self.store.commit(["hold", ck])
        self.learn_total(client_id, total)

    def learn_total(self, client_id, total) -> None:
        ck = client_key(client_id)
        if ck in self.done or ck in self.aborted:
            return
        if ck not in self.total:
            self.store.commit(["total", ck, total])
            self._broadcast(ck)
        self._check(ck)

    def on_control(self, message: dict) -> None:
        if message.get("stage") != self.stage or message.get("replica") == self.replica:
            return
        if message.get("sync"):
            self._answer_sync(message)
            return
        if "client_id" not in message:
            return
        client_id = tuple(message["client_id"])
        ck = client_key(client_id)
        if ck in self.done or ck in self.aborted:
            return
        if message.get("total") is not None:
            self.learn_total(client_id, message["total"])
        self.others.setdefault(ck, set()).update(from_ranges(message.get("ranges", ())))
        self._check(ck)

    def _answer_sync(self, message):
        wanted = [client_key(message["client_id"])] if "client_id" in message else None
        for ck in (wanted or list(self.total) + list(self.done)):
            if ck in self.done:
                total = self.done[ck]
                self._send(ck, total, [[0, total - 1]] if total else [])
            elif ck in self.total:
                self._broadcast(ck)

    def resync(self) -> None:
        for ck in self.pending():
            self._broadcast(ck)
            self.send_control(self._msg(None, None, [], sync=True, client_id=parse_client_key(ck)))
        self.send_control(self._msg(None, None, [], sync=True))
        for ck in list(self.done)[-ANNOUNCE_DONE:]:      # también lo ya terminado (ver más abajo)
            total = self.done[ck]
            self._send(ck, total, [[0, total - 1]] if total else [])
        for ck in self.pending():
            self._check(ck)

    def abort(self, client_id) -> None:
        ck = client_key(client_id)
        if ck not in self.aborted:
            self.store.commit(["abort", ck])

    def _msg(self, ck, total, ranges, sync=False, client_id=None):
        from common.protocol.messages import control_message
        cid = parse_client_key(ck) if ck is not None else client_id
        return control_message(cid, self.stage, self.replica, sum(b - a + 1 for a, b in ranges),
                               total=total, ranges=ranges if not sync else None, sync=sync)

    def _send(self, ck, total, ranges):
        self.send_control(self._msg(ck, total, ranges))

    def _broadcast(self, ck):
        self._send(ck, self.total.get(ck), to_ranges(self.own.get(ck, ())))

    def _check(self, ck) -> None:
        total = self.total.get(ck)
        if total is None or ck in self.done:
            return
        if not covers(self.own.get(ck, set()) | self.others.get(ck, set()), total):
            return
        client_id = parse_client_key(ck)
        if ck in self.holds or self.emit_all:
            self.on_complete(client_id, total)       # 1.º publicar el EOF...
            failpoints.hit("after_eof_publish")
        self.store.commit(["done", ck, total])       # 2.º recordar que terminó
        log.info("%s[%d]: cliente %s completo (%d batches)", self.stage, self.replica, client_ids(ck), total)

    def checkpoint(self) -> None:
        self.store.checkpoint()


def client_ids(ck):
    return parse_client_key(ck)
