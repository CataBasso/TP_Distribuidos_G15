import logging
import os

from common.bounded_set import BoundedSet
from common.ft import failpoints
from common.ft.ids import client_key, msg_id, parse_client_key
from common.ft.store import StateStore
from common.protocol.messages import eof_message, result_message, with_id
from common.stage import EofCounter, Stage
from common.topology import AVG_EX, AVG_READY_Q, Q5_IN, Q5_PEX, RESULTS_EX, queue_of, results_key
from common.utils.spool import BlockSpool

log = logging.getLogger(__name__)
PROGRESS_EVERY = 50      # cada cuántos bloques emitidos se persiste el progreso de la emisión
DONE_KEPT = 2000


class Q5Filter(Stage):
    """Q5: artículos >= 2021 con más palabras que el promedio de los <= 2020.

    Mientras el promedio del cliente no se conoce, los bloques (batches de artículos) se guardan
    en un spool EN DISCO que sobrevive a los reinicios. Cuando llega `average_ready` se emiten
    los bloques guardados. Cada bloque produce un mensaje de resultado con msg_id determinista
    (client, 'q5', batch_id, k), igual si se emite en vivo o desde el spool, así que reemitir
    después de una caída solo genera duplicados que el cliente descarta.

    Estado persistente (WAL + snapshot): promedio conocido, EOF recibidos por origen, progreso de
    la emisión del spool (cantidad de bloques ya emitidos) y clientes terminados.
    """
    name = "q5_filter"

    def __init__(self, bus, partition_id: int, upstream_replicas: int, spool_dir=None,
                 chunk_rows: int = None, spool=None, state_dir=None):
        super().__init__(bus)
        self.partition_id = partition_id
        self.spool = spool if spool is not None else BlockSpool(spool_dir)
        self.eofs = EofCounter(upstream_replicas)
        self.average = {}      
        self.emitted = {}      
        self.done = {}
        self.aborted_keys = BoundedSet()
        self._spooled = {}    
        self.store = self.durable(StateStore(state_dir or (str(self.spool.dir) + ".state"),
                                             self._apply, self._dump, self._load))
        self.store.recover()
        self._rebuild_spooled()

    #Aplica un registro del WAL al estado en memoria
    def _apply(self, r):
        op, ck = r[0], r[1]
        if op == "avg":
            self.average[ck] = r[2]
        elif op == "eof":
            self.eofs._seen.setdefault(parse_client_key(ck), set()).add(r[2])
        elif op == "progress":
            self.emitted[ck] = r[2]
        elif op in ("done", "abort"):
            if op == "done":
                self.done[ck] = True
                while len(self.done) > DONE_KEPT:
                    self.done.pop(next(iter(self.done)))
            else:
                self.aborted_keys.add(ck)
            for d in (self.average, self.emitted, self._spooled):
                d.pop(ck, None)
            self.eofs.forget(parse_client_key(ck))

    #Devuelve el estado en memoria
    def _dump(self):
        return {"avg": dict(self.average), "emitted": dict(self.emitted), "eofs": self.eofs.dump(),
                "done": list(self.done), "aborted": list(self.aborted_keys)}

    #Reconstruye el estado en memoria desde un estado persistente
    def _load(self, s):
        self.average, self.emitted = dict(s["avg"]), dict(s["emitted"])
        self.eofs.load(s["eofs"])
        self.done = {ck: True for ck in s["done"]}
        self.aborted_keys = BoundedSet()
        for ck in s["aborted"]:
            self.aborted_keys.add(ck)

    #Relee el spool para saber qué msg_id ya están guardados y borra los restos de clientes terminados o abortados
    def _rebuild_spooled(self):
        for client_id in self.spool.clients():
            ck = client_key(client_id)
            if ck in self.done or ck in self.aborted_keys:
                self.spool.delete(client_id)        # restos de un cliente que ya terminó o abortó
                continue
            self._spooled[ck] = {mid for mid, _b, _a in self.spool.read(client_id)}

    #Escucha artículos y promedio, y retoma lo que quedó a medias
    def start(self) -> None:
        k = self.partition_id
        self.bus.listen(queue_of(Q5_IN, k), self.handler(self._on_article), Q5_PEX, [str(k)])
        self.bus.listen(queue_of(AVG_READY_Q, k), self.handler(self._on_average), AVG_EX, kind="fanout")
        self.listen_abort()
        for ck in list(self.average):               
            self._replay(parse_client_key(ck))
            self._try_finish(parse_client_key(ck))
        for client_id in list(self.eofs._seen):
            self._try_finish(client_id)

    #Devuelve True si el cliente ya terminó o abortó
    def _closed(self, ck) -> bool:
        return ck in self.done or ck in self.aborted_keys

    #Si el promedio ya se conoce, emite el bloque en el acto. Si no, lo guarda en el spool
    def _on_article(self, message: dict) -> None:
        client_id = tuple(message["client_id"])
        ck = client_key(client_id)
        if self._closed(ck):
            return
        kind = message["type"]
        if kind == "q5_articles_batch":
            mid, batch_id = message.get("msg_id"), message.get("batch_id", 0)
            if ck in self.average:
                self._emit_block(client_id, batch_id, message["articles"])
            elif mid is None or mid not in self._spooled.get(ck, ()):
                self.spool.append(client_id, mid, batch_id, message["articles"])
                failpoints.hit("after_spool_append")
                self._spooled.setdefault(ck, set()).add(mid)
        elif kind == "eof":
            origin = message.get("origin", 0)
            if origin not in self.eofs._seen.get(client_id, ()):
                self.store.commit(["eof", ck, origin])
        else:
            log.warning("q5_filter: tipo inesperado %s", kind)
            return
        self._try_finish(client_id)

    #Procesa un mensaje de average_ready
    def _on_average(self, message: dict) -> None:
        if message["type"] != "average_ready":
            return
        client_id = tuple(message["client_id"])
        ck = client_key(client_id)
        if self._closed(ck):
            return
        if ck not in self.average:
            self.spool.sync()                                   
            self.store.commit(["avg", ck, message["average"]])  
        self._replay(client_id)
        self._try_finish(client_id)

    #Filtra los artículos con más palabras que el promedio y publica un mensaje por bloque
    def _emit_block(self, client_id, batch_id, articles) -> None:
        average = self.average[client_key(client_id)]
        if average is None:
            return
        rows = [{"url": url, "abstract_word_count": int(n)} for url, n in articles if int(n) > average]
        if rows:
            out = result_message(client_id, 5, rows, origin=self.partition_id)
            self.publish(RESULTS_EX, with_id(out, msg_id(client_id, "q5", batch_id, self.partition_id)),
                         key=results_key(client_id))

    #Emite los bloques del spool que todavía no se emitieron
    def _replay(self, client_id) -> None:
        ck = client_key(client_id)
        if ck not in self.average or not self.spool.exists(client_id):
            return
        blocks = self.spool.read(client_id)
        start = self.emitted.get(ck, 0)
        for i in range(start, len(blocks)):
            _mid, batch_id, articles = blocks[i]
            self._emit_block(client_id, batch_id, articles)
            failpoints.hit("during_replay")
            if (i + 1) % PROGRESS_EVERY == 0:
                self.store.commit(["progress", ck, i + 1])
        if len(blocks) != start:
            self.store.commit(["progress", ck, len(blocks)])

    #Si el cliente ya terminó o abortó, no hace nada. Si ya recibió todos los EOF, emite el EOF de Q5, marca al cliente como terminado y borra el spool
    def _try_finish(self, client_id) -> None:
        ck = client_key(client_id)
        if self._closed(ck) or ck not in self.average or not self.eofs.complete(client_id):
            return
        self._replay(client_id)
        eof = eof_message(client_id, origin=self.partition_id)
        eof["query"] = 5
        self.publish(RESULTS_EX, with_id(eof, msg_id(client_id, "q5-eof", 0, self.partition_id)),
                     key=results_key(client_id))                # 1.º publicar el EOF...
        self.store.commit(["done", ck])                         # 2.º registrar que terminó...
        self.spool.delete(client_id)                            # 3.º y recién ahí borrar el spool
        log.info("q5_filter[%d]: cliente %s terminado", self.partition_id, client_id)

    #Borra el spool y persiste el abort
    def on_abort(self, client_id) -> None:
        ck = client_key(client_id)
        self.spool.delete(client_id)
        if ck not in self.aborted_keys:
            self.store.commit(["abort", ck])


def main() -> None:
    from common.bus import RabbitBus
    from common.runner import broker_host, env_int, env_str, run_stage, state_dir
    k = env_int("PARTITION_ID", alt="ID")
    root = os.environ.get("STATE_DIR")
    spool_dir = env_str("SPOOL_DIR", os.path.join(root, f"q5_{k}", "spool") if root else f"spool/q5_{k}")
    run_stage(Q5Filter(RabbitBus(broker_host()), k, env_int("LM_REPLICAS"), spool_dir,
                       state_dir=state_dir(f"q5_{k}/state")))


if __name__ == "__main__":
    main()
