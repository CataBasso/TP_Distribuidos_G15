import csv
import os
from collections import OrderedDict
from pathlib import Path
from typing import Iterator


class Spool:

    def __init__(self, directory, max_open: int = 128, fsync: bool = False, wipe: bool = True):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.max_open = max_open
        self.fsync = fsync
        self._open: "OrderedDict[tuple, tuple]" = OrderedDict()    
        if wipe:
            for pattern in ("client_*.csv", "client_*.jsonl"):     
                for path in self.dir.glob(pattern):
                    path.unlink()

    def path(self, client_id) -> Path:
        gw_id, n = client_id
        return self.dir / f"client_{int(gw_id)}_{int(n)}.csv"


    def _handle(self, client_id):
        key = tuple(client_id)
        entry = self._open.get(key)
        if entry is not None:
            self._open.move_to_end(key)
            return entry
        while len(self._open) >= self.max_open:
            _, (oldest, _) = self._open.popitem(last=False)
            oldest.close()
        f = open(self.path(key), "a", encoding="utf-8", newline="")
        entry = self._open[key] = (f, csv.writer(f))
        return entry

    def append(self, client_id, record) -> None:
        f, writer = self._handle(client_id)
        writer.writerow(record)
        f.flush()
        if self.fsync:
            os.fsync(f.fileno())

    def append_many(self, client_id, records) -> None:
        f, writer = self._handle(client_id)
        writer.writerows(records)
        f.flush()
        if self.fsync:
            os.fsync(f.fileno())

    def _close(self, client_id) -> None:
        entry = self._open.pop(tuple(client_id), None)
        if entry is not None:
            entry[0].close()

    def read(self, client_id) -> Iterator:
        self._close(client_id)
        path = self.path(client_id)
        if not path.exists():
            return
        with open(path, encoding="utf-8", newline="") as f:
            for record in csv.reader(f):
                if record:
                    yield record


    def exists(self, client_id) -> bool:
        return self.path(client_id).exists()

    def delete(self, client_id) -> None:
        self._close(client_id)
        self.path(client_id).unlink(missing_ok=True)

    def close(self) -> None:
        for f, _ in self._open.values():
            f.close()
        self._open.clear()


class BlockSpool:

    #Crea el directorio y lee FSYNC y FSYNC_EVERY. NO borra nada al empezar, porque los bloques pueden ser leídos después de un reinicio.
    def __init__(self, directory, max_open: int = 128, fsync: str = None):
        import os
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.max_open = max_open
        self.policy = (fsync or os.environ.get("FSYNC", "batch")).lower()
        self.fsync_every = int(os.environ.get("FSYNC_EVERY", "32"))
        self._unsynced = 0
        self._open: "OrderedDict[tuple, object]" = OrderedDict()

    #Devuelve el path del bloque del cliente, que es client_{gw}_{n}.blk
    def path(self, client_id) -> Path:
        gw_id, n = client_id
        return self.dir / f"client_{int(gw_id)}_{int(n)}.blk"

    #Lista los clientes que tienen archivo. Sirve para reconstruir el estado después de un reinicio
    def clients(self) -> list:
        out = []
        for p in sorted(self.dir.glob("client_*.blk")):
            _, gw, n = p.stem.split("_")
            out.append((int(gw), int(n)))
        return out

    #Abre el archivo del cliente y lo devuelve. Mantiene un máximo de max_open abiertos, cerrando los más antiguos.
    def _handle(self, client_id):
        key = tuple(client_id)
        f = self._open.get(key)
        if f is not None:
            self._open.move_to_end(key)
            return f
        while len(self._open) >= self.max_open:
            _, oldest = self._open.popitem(last=False)
            oldest.close()
        f = self._open[key] = open(self.path(key), "ab")
        return f

    #Escribe un bloque con el mismo formato de registro del WAL y hace fsync
    def append(self, client_id, msg_id, batch_id, articles) -> None:
        from common.ft.journal import _frame
        f = self._handle(client_id)
        f.write(_frame([msg_id, batch_id, articles]))
        f.flush()
        self._unsynced += 1
        if self.policy == "always" or (self.policy == "batch" and self._unsynced >= self.fsync_every):
            self.sync()

    #Fuerza a disco lo pendiente
    def sync(self) -> None:
        if self.policy != "never":
            for f in self._open.values():
                os.fsync(f.fileno())
        self._unsynced = 0

    #Lee los bloques completos en orden y trunca la cola rota si la hay
    def read(self, client_id) -> list:
        from common.ft.journal import read_frames
        self._close(client_id)
        path = self.path(client_id)
        records, good = read_frames(path)
        if path.exists() and path.stat().st_size != good:
            with open(path, "r+b") as f:
                f.truncate(good)
        return [(r[0], r[1], r[2]) for r in records]

    #Cierra el archivo abierto del cliente, si lo hay
    def _close(self, client_id) -> None:
        f = self._open.pop(tuple(client_id), None)
        if f is not None:
            f.close()

    #Devuelve True si hay un archivo de bloque para el cliente
    def exists(self, client_id) -> bool:
        return self.path(client_id).exists()

    #Borra el archivo del cliente y cierra el abierto si lo hay
    def delete(self, client_id) -> None:
        self._close(client_id)
        self.path(client_id).unlink(missing_ok=True)

    #Cierra todos los archivos abiertos y borra la lista de abiertos
    def close(self) -> None:
        for f in self._open.values():
            f.close()
        self._open.clear()
