import json
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
        self._open: "OrderedDict[tuple, object]" = OrderedDict()
        if wipe:
            for path in self.dir.glob("client_*.jsonl"):
                path.unlink()

    #Ruta del archivo del cliente
    def path(self, client_id) -> Path:
        gw_id, n = client_id
        return self.dir / f"client_{int(gw_id)}_{int(n)}.jsonl"


    def _handle(self, client_id):
        key = tuple(client_id)
        f = self._open.get(key)
        if f is not None:
            self._open.move_to_end(key)
            return f
        while len(self._open) >= self.max_open:
            _, oldest = self._open.popitem(last=False)
            oldest.close()
        f = open(self.path(key), "a", encoding="utf-8")
        self._open[key] = f
        return f

    #Agrega un registro y lo baja a disco
    def append(self, client_id, record) -> None:
        f = self._handle(client_id)
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
        f.flush()
        if self.fsync:
            os.fsync(f.fileno())

    def _close(self, client_id) -> None:
        f = self._open.pop(tuple(client_id), None)
        if f is not None:
            f.close()

    #Registros del cliente, en orden
    def read(self, client_id) -> Iterator:
        self._close(client_id)
        path = self.path(client_id)
        if not path.exists():
            return
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)


    def exists(self, client_id) -> bool:
        return self.path(client_id).exists()

    def delete(self, client_id) -> None:
        self._close(client_id)
        self.path(client_id).unlink(missing_ok=True)

    #Cierra todos los archivos abiertos
    def close(self) -> None:
        for f in self._open.values():
            f.close()
        self._open.clear()
