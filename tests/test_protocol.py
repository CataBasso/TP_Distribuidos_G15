import socket
import struct

import pytest

from common.protocol import (
    FrameError, send_frame, recv_frame, encode, decode,
    batch_message, eof_message, abort_message, result_message, done_message, client_id_of,
)


def test_encode_decode_roundtrip_keeps_unicode():
    msg = batch_message((1, 7), 3, 1, [{"name": "Ñandú – café", "url": "u"}])
    assert decode(encode(msg)) == msg


def test_decode_rejects_messages_without_type():
    with pytest.raises(ValueError):
        decode(b'{"rows": []}')
    with pytest.raises(ValueError):
        decode(b'[1, 2]')


def test_header_fields_and_client_id_tuple():
    msg = batch_message((2, 9), 0, 2, [])
    assert msg["client_id"] == [2, 9] and msg["batch_id"] == 0 and msg["origin"] == 2
    assert client_id_of(msg) == (2, 9)
    assert eof_message((2, 9), 2)["type"] == "eof"
    assert abort_message((2, 9))["type"] == "abort"
    assert result_message((2, 9), 3, [{"word": "ada"}])["query"] == 3
    assert done_message((2, 9))["type"] == "done"


def test_frames_roundtrip_in_order():
    a, b = socket.socketpair()
    for i in range(3):
        send_frame(a, {"type": "batch", "rows": [{"i": i}]})
    assert [recv_frame(b)["rows"][0]["i"] for i in range(3)] == [0, 1, 2]
    a.close()
    assert recv_frame(b) is None            # cierre limpio entre mensajes
    b.close()


def test_large_frame_arrives_complete():
    a, b = socket.socketpair()
    big = {"type": "batch", "rows": [{"x": "a" * 3_000_000}]}
    import threading
    t = threading.Thread(target=send_frame, args=(a, big)); t.start()
    assert recv_frame(b) == big
    t.join(); a.close(); b.close()


def test_truncated_frame_raises():
    a, b = socket.socketpair()
    a.sendall(struct.pack(">I", 100) + b"{")
    a.close()
    with pytest.raises(FrameError):
        recv_frame(b)
    b.close()


def test_absurd_length_raises():
    a, b = socket.socketpair()
    a.sendall(struct.pack(">I", 0xFFFFFFFF))
    with pytest.raises(FrameError):
        recv_frame(b)
    a.close(); b.close()
