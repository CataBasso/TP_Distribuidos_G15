import os
from dataclasses import dataclass


@dataclass
class GatewayConfig:
    gw_id: int = 1 #identifica a este Gateway (primera parte del client_id)
    host: str = "0.0.0.0" #interfaz donde escucha a los clientes
    port: int = 5000
    rabbit_host: str = "localhost"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        env = os.environ.get
        return cls(
            gw_id=int(env("GATEWAY_ID", "1")),
            host=env("GATEWAY_BIND_HOST", "0.0.0.0"),
            port=int(env("GATEWAY_PORT", "5000")),
            rabbit_host=env("RABBIT_HOST", "localhost"),
            log_level=env("LOG_LEVEL", "INFO"),
        )
