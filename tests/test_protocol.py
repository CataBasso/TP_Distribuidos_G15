
import socket
import struct
import threading

import pytest

from common.protocol import (
    FrameError,
    send_frame,
    recv_frame,
    encode,
    decode,
    batch_message,
    eof_message,
    abort_message,
    result_message,
    done_message,
    client_id_of,
)

from common.protocol.external_serializer import (
    serialize_value,
    deserialize_value,
)


def test_encode_decode_roundtrip_keeps_unicode():
    msg = batch_message((1, 7), 3, [{"name": "Ñandú – café", "url": "u"}], origin=1)

    decoded = decode(encode(msg))

    assert decoded == msg
    assert decoded["rows"][0]["name"] == "Ñandú – café"


def test_binary_encoding_is_not_json():
    msg = batch_message((1, 7), 3, [], origin=1)

    encoded = encode(msg)

    # Los primeros 4 bytes representan el tipo numérico.
    assert len(encoded) >= 4
    assert encoded != b'{"rows": []}'


def test_decode_rejects_unknown_message_type():
    # 999 no es un tipo definido en messages.py.
    encoded = struct.pack(">I", 999) + serialize_value({})

    with pytest.raises(ValueError):
        decode(encoded)


def test_decode_rejects_invalid_payload():
    # El tipo BATCH es válido, pero el payload no es un valor binario válido.
    encoded = struct.pack(">I", 1) + b"\xff"

    with pytest.raises(ValueError):
        decode(encoded)


def test_decode_rejects_trailing_bytes():
    payload = serialize_value({"batch_id": 0, "rows": []})
    encoded = struct.pack(">I", 1) + payload + b"\x00"

    with pytest.raises(ValueError):
        decode(encoded)


def test_header_fields_and_client_id():
    msg = batch_message((2, 9), 0, [], origin=2)

    decoded = decode(encode(msg))

    assert decoded["client_id"] == (2, 9)
    assert decoded["batch_id"] == 0
    assert decoded["origin"] == 2
    assert client_id_of(decoded) == (2, 9)

    assert eof_message((2, 9), 2)["type"] == "eof"
    assert abort_message((2, 9))["type"] == "abort"
    assert result_message((2, 9), 3, [{"word": "ada"}])["query"] == 3
    assert done_message((2, 9))["type"] == "done"


def test_encode_rejects_invalid_messages():
    with pytest.raises(ValueError):
        encode({"type": "unknown"})

    with pytest.raises((ValueError, TypeError)):
        encode({"type": "batch", "rows": "not-a-list"})


def test_binary_value_serializer_roundtrip():
    value = {
        "text": "Buenos días ☀️",
        "integer": 123456,
        "negative": -42,
        "float": 3.14,
        "boolean": True,
        "nothing": None,
        "bytes": b"\x00\x01\xff",
        "list": [1, "dos", False],
        "tuple": (4, 5),
    }

    assert deserialize_value(serialize_value(value)) == value


def test_frames_roundtrip_in_order():
    a, b = socket.socketpair()

    try:
        for i in range(3):
            send_frame(
                a,
                batch_message((1, 7), i, [{"i": i}], origin=0),
            )

        received = [recv_frame(b) for _ in range(3)]

        assert [msg["rows"][0]["i"] for msg in received] == [0, 1, 2]
    finally:
        a.close()
        b.close()


def test_large_frame_arrives_complete():
    a, b = socket.socketpair()
    msg = batch_message(
        (1, 7),
        0,
        [{"x": "a" * 3_000_000}],
        origin=0,
    )

    errors = []

    def send():
        try:
            send_frame(a, msg)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=send)
    thread.start()

    try:
        received = recv_frame(b)
        thread.join()

        assert not errors
        assert received == msg
    finally:
        a.close()
        b.close()


def test_truncated_frame_raises():
    a, b = socket.socketpair()

    try:
        # El encabezado anuncia 100 bytes, pero solo llega un byte.
        a.sendall(struct.pack(">I", 100) + b"\x01")
        a.close()

        with pytest.raises(FrameError):
            recv_frame(b)
    finally:
        a.close()
        b.close()


def test_absurd_length_raises():
    a, b = socket.socketpair()

    try:
        a.sendall(struct.pack(">I", 0xFFFFFFFF))

        with pytest.raises(FrameError):
            recv_frame(b)
    finally:
        a.close()
        b.close()


def test_clean_close_between_frames_returns_none():
    a, b = socket.socketpair()

    try:
        a.close()

        assert recv_frame(b) is None
    finally:
        b.close()
