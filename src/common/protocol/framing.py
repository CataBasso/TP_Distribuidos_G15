from .. import safe_socket
from ..safe_socket import FrameError      # se re-exporta: el cliente, el Gateway y los tests lo importan de acá
from .messages import encode, decode

MAX_FRAME_BYTES = safe_socket.MAX_PACKET_SIZE
MIN_FRAME_BYTES = 4                       # el contenido empieza con el tipo de mensaje (un uint32)


def send_frame(sock, message):
    try:
        data = encode(message)
    except (ValueError, TypeError, UnicodeError) as exc:
        raise FrameError(f"mensaje inválido: {exc}") from exc

    safe_socket.send_packet(sock, data, max_size=MAX_FRAME_BYTES)


def recv_frame(sock):
    data = safe_socket.recv_packet(
        sock, min_size=MIN_FRAME_BYTES, max_size=MAX_FRAME_BYTES
    )

    if data is None:
        return None

    try:
        return decode(data)
    except (ValueError, TypeError, UnicodeError) as exc:
        raise FrameError(f"mensaje inválido: {exc}") from exc
