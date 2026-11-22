import logging
import os
import struct
import zlib
from pathlib import Path

from common.protocol.external_serializer import deserialize_value, serialize_value

log = logging.getLogger(__name__)
_HDR = struct.Struct(">II")

#Serializa un registro y le antepone largo y crc
def _frame(record) -> bytes:
    payload = serialize_value(record)
    return _HDR.pack(len(payload), zlib.crc32(payload) & 0xFFFFFFFF) + payload

#Lee registros hasta el primer incompleto o con crc inválido
def read_frames(path):
    records, good = [], 0
    try:
        data = Path(path).read_bytes()
    except FileNotFoundError:
        return records, 0
    pos = 0
    while pos + _HDR.size <= len(data):
        length, crc = _HDR.unpack_from(data, pos)
        end = pos + _HDR.size + length
        if end > len(data):
            break
        payload = data[pos + _HDR.size:end]
        if zlib.crc32(payload) & 0xFFFFFFFF != crc:
            break
        try:
            records.append(deserialize_value(payload))
        except Exception:                                 
            break
        pos = good = end
    return records, good

#fsync del directorio
def _fsync_dir(directory):
    try:
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


class Journal:
    def __init__(self, directory, fsync=None, fsync_every=None, snapshot_every=None):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.policy = (fsync or os.environ.get("FSYNC", "batch")).lower()
        self.fsync_every = int(fsync_every or os.environ.get("FSYNC_EVERY", "32"))
        self.snapshot_every = int(snapshot_every or os.environ.get("SNAPSHOT_EVERY", "500"))
        self.gen = 0
        self._f = None
        self._unsynced = 0
        self.since_snapshot = 0


    def _wal_path(self, gen):
        return self.dir / f"wal.{gen}.bin"

    @property
    def _snap_path(self):
        return self.dir / "snapshot.bin"

    #Lee el snapshot, lee el WAL de esa generación, trunca la cola rota, borra WAL de otras generaciones y archivos .tmp, y reabre el WAL
    def recover(self):
        state = None
        records, good = read_frames(self._snap_path)
        if records:
            self.gen = int(records[0]["gen"])
            state = records[0]["state"]
        wal, good = read_frames(self._wal_path(self.gen))
        path = self._wal_path(self.gen)
        if path.exists() and path.stat().st_size != good:
            log.warning("journal %s: se descarta una cola rota del WAL (%d bytes)",
                        self.dir, path.stat().st_size - good)
            with open(path, "r+b") as f:
                f.truncate(good)
        for old in self.dir.glob("wal.*.bin"):              
            if old != path:
                old.unlink(missing_ok=True)
        for tmp in self.dir.glob("*.tmp"):
            tmp.unlink(missing_ok=True)
        self._open()
        self.since_snapshot = len(wal)
        return state, wal

    def _open(self):
        if self._f is not None:
            self._f.close()
        self._f = open(self._wal_path(self.gen), "ab")
        self._unsynced = 0

    #write + flush
    def append(self, record) -> None:
        if self._f is None:
            self._open()
        self._f.write(_frame(record))
        self._f.flush()
        self._unsynced += 1
        self.since_snapshot += 1
        if self.policy == "always" or (self.policy == "batch" and self._unsynced >= self.fsync_every):
            self.sync()

    #Fuerza a disco lo pendiente
    def sync(self) -> None:
        if self._f is not None and self._unsynced and self.policy != "never":
            os.fsync(self._f.fileno())
        self._unsynced = 0
    
    #Para saber si hacer el snapshot, depende del limite del WAL
    def should_snapshot(self) -> bool:
        return self.since_snapshot >= self.snapshot_every

    #Escribe el estado completo de forma atomica y empieza un WAL nuevo
    def snapshot(self, state) -> None:
        self.sync()
        new_gen = self.gen + 1
        tmp = self.dir / "snapshot.tmp"
        with open(tmp, "wb") as f:
            f.write(_frame({"gen": new_gen, "state": state}))
            f.flush()
            if self.policy != "never":
                os.fsync(f.fileno())
        os.replace(tmp, self._snap_path)
        if self.policy != "never":
            _fsync_dir(self.dir)
        old = self._wal_path(self.gen)
        self.gen = new_gen
        self._open()
        old.unlink(missing_ok=True)
        self.since_snapshot = 0

    def close(self) -> None:
        if self._f is not None:
            self.sync()
            self._f.close()
            self._f = None
