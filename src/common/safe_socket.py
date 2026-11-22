
HEADER_SIZE = 4
MAX_PACKET_SIZE = 64 * 1024 * 1024


class FrameError(Exception):
    """Error al recibir o mandar un mensaje TCP."""


def send_all(sock, data):
    sock.sendall(data)


def recv_all(sock, size, allow_eof_at_start=False):
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


def send_packet(sock, data, max_size=MAX_PACKET_SIZE):
    if len(data) > max_size:
        raise FrameError(f"mensaje de {len(data)} bytes supera el máximo")

    header = len(data).to_bytes(HEADER_SIZE, "big")
    send_all(sock, header + data)


def recv_packet(sock, min_size=0, max_size=MAX_PACKET_SIZE):
    header = recv_all(sock, HEADER_SIZE, allow_eof_at_start=True)

    if header is None:
        return None

    length = int.from_bytes(header, "big")

    if length < min_size or length > max_size:
        raise FrameError(f"largo de mensaje inválido: {length}")

    return recv_all(sock, length)
