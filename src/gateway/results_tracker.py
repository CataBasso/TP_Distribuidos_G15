import threading


class ResultTracker:

    def __init__(self, expected: dict):
        self.expected = dict(expected)
        self._seen = {}               
        self._lock = threading.Lock()

    def add(self, client_id, query, origin) -> bool:
        if query not in self.expected:
            raise ValueError(f"EOF de una consulta desconocida: {query!r}")
        client_id = tuple(client_id)
        with self._lock:
            per_query = self._seen.setdefault(client_id, {})
            per_query.setdefault(query, set()).add(origin)
            if all(len(per_query.get(q, ())) >= n for q, n in self.expected.items()):
                del self._seen[client_id]
                return True
            return False

    def pending(self, client_id) -> dict:
        with self._lock:
            per_query = self._seen.get(tuple(client_id), {})
            return {q: (len(per_query.get(q, ())), n) for q, n in self.expected.items()}

    def forget(self, client_id) -> None:
        with self._lock:
            self._seen.pop(tuple(client_id), None)
