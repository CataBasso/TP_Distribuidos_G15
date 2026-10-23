
from .messages import encode, decode

MAX_FRAME_BYTES = 64 * 1024 * 1024
_HEADER_SIZE = 4


class FrameError(Exception):
    """Error al recibir o decodificar un mensaje TCP."""


def _recv_exact(sock, size, allow_eof_at_start=False):
    chunks = []
    remaining = size

    while remaining > 0:
        chunk = sock.recv(min(remaining, 1 << 20))

        if not chunk:
            if allow_eof_at_start and remaining == size:
                return None
            raise FrameError("conexión cerrada a mitad de un mensaje")

        chunks.append(chunk)
        remaining -= len(chunk)

    return b"".join(chunks)


def send_frame(sock, message):
    try:
        data = encode(message)
    except (ValueError, TypeError, UnicodeError) as exc:
        raise FrameError(f"mensaje inválido: {exc}") from exc

    if len(data) > MAX_FRAME_BYTES:
        raise FrameError(
            f"mensaje de {len(data)} bytes supera el máximo"
        )

    header = len(data).to_bytes(_HEADER_SIZE, "big")
    sock.sendall(header + data)


def recv_frame(sock):
    header = _recv_exact(
        sock, _HEADER_SIZE, allow_eof_at_start=True
    )

    if header is None:
        return None

    length = int.from_bytes(header, "big")

    if length < 4 or length > MAX_FRAME_BYTES:
        raise FrameError(f"largo de mensaje inválido: {length}")

    data = _recv_exact(sock, length)

    try:
        return decode(data)
    except (ValueError, TypeError, UnicodeError) as exc:
        raise FrameError(f"mensaje inválido: {exc}") from exc
