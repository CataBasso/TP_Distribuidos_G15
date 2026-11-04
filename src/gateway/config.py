import os
from dataclasses import dataclass


@dataclass
class GatewayConfig:
    gw_id: int = 1 
    host: str = "0.0.0.0" 
    port: int = 5000
    rabbit_host: str = "localhost"
    log_level: str = "INFO"
    word_partitions: int = 1 
    q5_partitions: int = 1 

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        env = os.environ.get
        return cls(
            gw_id=int(env("GATEWAY_ID", "1")),
            host=env("GATEWAY_BIND_HOST", "0.0.0.0"),
            port=int(env("GATEWAY_PORT", "5000")),
            rabbit_host=env("RABBIT_HOST", "localhost"),
            log_level=env("LOG_LEVEL", "INFO"),
            word_partitions=int(env("WORD_PARTITIONS", "1")),
            q5_partitions=int(env("Q5_PARTITIONS", "1")),
        )
