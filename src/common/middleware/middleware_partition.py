import zlib

from .middleware_rabbitmq import RabbitConnection, MessageMiddlewareExchangeRabbitMQ

def partition(key, n):
    """Partición en [0, n) de una clave. Usa crc32 porque el hash() de Python cambia
    entre procesos y la misma clave iría a particiones distintas."""
    return zlib.crc32(str(key).encode("utf-8")) % n


def declare_partitions(host, exchange_name, queue_prefix, n, connection=None, durable=True):
    """Crea (si no existen) las n colas <queue_prefix>.<i>, ligadas con la clave str(i)
    a un exchange direct. Los productores lo llaman al arrancar para que ningún mensaje
    se pierda si el consumidor de esa partición todavía no está."""
    own = connection is None
    rc = connection if connection is not None else RabbitConnection(host)
    try:
        for i in range(n):
            MessageMiddlewareExchangeRabbitMQ(host, exchange_name, [str(i)],
                                              queue_name=f"{queue_prefix}.{i}",
                                              durable=durable, connection=rc)
    finally:
        if own:
            rc.close()