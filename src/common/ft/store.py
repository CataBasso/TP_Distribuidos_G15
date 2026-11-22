from common.ft import failpoints
from common.ft.journal import Journal


class StateStore:
    def __init__(self, directory, apply, dump, load, **journal_kw):
        self.apply, self.dump, self.load = apply, dump, load
        self.journal = Journal(directory, **journal_kw) if directory else None

    #Carga el snapshot y replica el WAL. Devuelve cuantos registros reaplicó
    def recover(self) -> int:
        if self.journal is None:
            return 0
        state, records = self.journal.recover()
        if state is not None:
            self.load(state)
        for record in records:
            self.apply(record)
        return len(records)

    def commit(self, record) -> None:
        failpoints.hit("before_persist")
        self.apply(record)
        if self.journal is not None:
            self.journal.append(record)
        failpoints.hit("after_persist")

    #Compacta el WAL en un snapshot si ya tiene suficientes registros
    def checkpoint(self) -> None:
        if self.journal is not None and self.journal.should_snapshot():
            self.journal.snapshot(self.dump())

    def sync(self) -> None:
        if self.journal is not None:
            self.journal.sync()

    def close(self) -> None:
        if self.journal is not None:
            self.journal.close()
