'''
"""Protocolo binario para la comunicación externa e interna."""

- external_serializer.py: tipos básicos y valores compuestos.
- messages.py: identificadores, estructuras y validación de mensajes.
- external.py: funciones para enviar y recibir mensajes por TCP.
- internal.py: funciones para serializar y deserializar publicaciones de RabbitMQ.
- framing.py: encabezado de longitud y lectura completa de TCP.
'''
from .framing import FrameError, send_frame, recv_frame
from .messages import (
    encode,
    decode,
    batch_message,
    eof_message,
    abort_message,
    result_message,
    done_message,
    client_id_of,
)

