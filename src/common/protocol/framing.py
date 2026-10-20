"""
Framing de los mensajes por TCP entre Client y Gateway.

Cada mensaje se envía como [4 bytes big-endian con el largo N][N bytes de JSON UTF-8]
"""
import struct

from .messages import encode, decode

_HEADER = struct.Struct(">I")
MAX_FRAME_BYTES = 64 * 1024 * 1024


class FrameError(Exception):
    """El flujo se cortó a mitad de un mensaje, o el largo declarado es inválid"""

#Codifica el mensaje, verifica que no pase el tope y envía 4 bytes de largo
def send_frame(sock, message: dict) -> None:
    data = encode(message)
    if len(data) > MAX_FRAME_BYTES:
        raise FrameError(f"mensaje de {len(data)} bytes supera el máximo")
    sock.sendall(_HEADER.pack(len(data)) + data)

#Lee exactamente n bytes
def _recv_exact(sock, n: int, allow_eof_at_start: bool = False):
    chunks = []
    remaining = n
    while remaining:
        chunk = sock.recv(min(remaining, 1 << 20))
        if not chunk:
            if allow_eof_at_start and remaining == n:
                return None
            raise FrameError("conexión cerrada a mitad de un mensaje")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


#Lee los 4 bytes de largo, valida el tope, lee el cuerpo y lo decodifica. Devuelve None si
#Eleva FrameError si se cortó a mitad de un mensaje
def recv_frame(sock):
    header = _recv_exact(sock, _HEADER.size, allow_eof_at_start=True)
    if header is None:
        return None
    (length,) = _HEADER.unpack(header)
    if length > MAX_FRAME_BYTES:
        raise FrameError(f"largo de mensaje inválido: {length}")
    try:
        return decode(_recv_exact(sock, length))
    except ValueError as e:
        raise FrameError(f"mensaje inválido: {e}") from e
