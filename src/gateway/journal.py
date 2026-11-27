from common.ft.store import StateStore


class SessionJournal:

    #Estado en memoria y recuperación del disco
    def __init__(self, directory=None):
        self.sessions = {}     # n -> {"next": int, "eof": int|None, "reof": {query: set(origins)}}
        self.last_n = 0
        self.store = StateStore(directory, self._apply, self._dump, self._load)
        self.store.recover()

    #Aplica un registro: open crea la sesión, batch actualiza next, eof guarda el total, reof anota un EOF de resultados, close la borra
    def _apply(self, r):
        op, n = r[0], r[1]
        if op == "open":
            self.sessions[n] = {"next": 0, "eof": None, "reof": {}}
            self.last_n = max(self.last_n, n)
        elif op == "n":
            self.last_n = max(self.last_n, n)
        elif op == "batch" and n in self.sessions:
            self.sessions[n]["next"] = r[2]
        elif op == "eof" and n in self.sessions:
            self.sessions[n]["eof"] = r[2]
        elif op == "reof" and n in self.sessions:
            self.sessions[n]["reof"].setdefault(r[2], set()).add(r[3])
        elif op == "close":
            self.sessions.pop(n, None)

    #Devuelve un dict serializable con el estado de memoria
    def _dump(self):
        return {"last_n": self.last_n,
                "sessions": {str(n): {"next": s["next"], "eof": s["eof"],
                                      "reof": {str(q): sorted(o) for q, o in s["reof"].items()}}
                             for n, s in self.sessions.items()}}

    #Reconstruye el estado de memoria desde un dict serializable
    def _load(self, state):
        self.last_n = state["last_n"]
        self.sessions = {int(n): {"next": s["next"], "eof": s["eof"],
                                  "reof": {int(q): set(o) for q, o in s["reof"].items()}}
                         for n, s in state["sessions"].items()}

    #Registra una sesión nueva
    def open(self, n):
        self.store.commit(["open", n])

    #Anota que ya se publicó hasta next_batch - 1 y compacta si corresponde
    def batch(self, n, next_batch):
        self.store.commit(["batch", n, next_batch])
        self.store.checkpoint()

    #Anota que se publicó el EOF y compacta si corresponde
    def eof(self, n, total):
        self.store.commit(["eof", n, total])

    #Anota un EOF de resultados (sólo si es nuevo), para no pedirlo de nuevo tras una caída
    def result_eof(self, n, query, origin):
        if n in self.sessions and origin not in self.sessions[n]["reof"].get(query, ()):
            self.store.commit(["reof", n, query, origin])

    #Borra la sesión terminada o abortada
    def close(self, n):
        if n in self.sessions:
            self.store.commit(["close", n])
            self.store.checkpoint()
