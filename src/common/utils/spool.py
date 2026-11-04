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
