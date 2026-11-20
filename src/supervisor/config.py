import os
from common.runner import env_int, env_float

class SupervisorConfig:
    ENV_SEPARATOR = ","

    def __init__(self):
        self.supervisor_id = env_int("SUPERVISOR_ID")
        self.supervisor_count = env_int("SUPERVISOR_COUNT")
        self.monitored_nodes = [
            node.strip()
            for node in os.environ.get("MONITORED_NODES", "").split(self.ENV_SEPARATOR)
            if node.strip()
        ]

        self.heartbeat_port = env_int("HEARTBEAT_PORT")
        self.election_port = env_int("ELECTION_PORT")

        self.ping_interval = env_float("PING_INTERVAL")
        self.ping_timeout = env_float("PING_TIMEOUT")
        self.miss_limit = env_int("MISS_LIMIT")                
        self.leader_miss_limit = env_int("LEADER_MISS_LIMIT")
        self.stall_limit = env_float("STALL_LIMIT")  

        self.election_timeout = env_float("ELECTION_TIMEOUT")
        self.coordinator_timeout = env_float("COORDINATOR_TIMEOUT")

        if not 0 <= self.supervisor_id < self.supervisor_count:
            raise ValueError(
                f"SUPERVISOR_ID={self.supervisor_id} fuera de rango [0, {self.supervisor_count})")

        self.supervisors = {i: f"supervisor_{i}" for i in range(self.supervisor_count)}
        self.peers = {i: n for i, n in self.supervisors.items() if i != self.supervisor_id}
        self.node_name = self.supervisors[self.supervisor_id]
        self.targets = [node for node in self.monitored_nodes if node != self.node_name]

        self._validate()

    def _validate(self):
        if not self.monitored_nodes:
            raise ValueError("falta MONITORED_NODES o está vacío")
        if self.ping_timeout >= self.ping_interval:
            raise ValueError("PING_TIMEOUT debe ser menor que PING_INTERVAL")
        if self.miss_limit < 1 or self.leader_miss_limit < 1:
            raise ValueError("MISS_LIMIT y LEADER_MISS_LIMIT deben ser >= 1")
        if self.election_timeout <= self.ping_timeout:
            raise ValueError("ELECTION_TIMEOUT debe ser mayor que PING_TIMEOUT")
        if self.stall_limit <= self.ping_interval * self.miss_limit:
            raise ValueError("STALL_LIMIT debe ser mayor que PING_INTERVAL * MISS_LIMIT")
