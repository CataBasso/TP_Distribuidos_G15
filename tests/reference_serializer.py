
import struct

UINT32_SIZE = 4
INT64_SIZE = 8
FLOAT64_SIZE = 8
BOOL_SIZE = 1

_NULL = 0
_BOOL = 1
_INT = 2
_FLOAT = 3
_STRING = 4
_BYTES = 5
_LIST = 6
_TUPLE = 7
_DICT = 8

MAX_VALUE_SIZE = 64 * 1024 * 1024
MAX_COLLECTION_SIZE = 1_000_000
MAX_NESTING = 64


def serialize_uint32(value):
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError("se esperaba un uint32")
    if not 0 <= value <= 0xFFFFFFFF:
        raise ValueError("uint32 fuera de rango")
    return value.to_bytes(UINT32_SIZE, "big")


def deserialize_uint32(data):
    if len(data) != UINT32_SIZE:
        raise ValueError("uint32 inválido")
    return int.from_bytes(data, "big")


def serialize_int64(value):
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError("se esperaba un int64")
    try:
        return value.to_bytes(INT64_SIZE, "big", signed=True)
    except OverflowError as exc:
        raise ValueError("int64 fuera de rango") from exc


def deserialize_int64(data):
    if len(data) != INT64_SIZE:
        raise ValueError("int64 inválido")
    return int.from_bytes(data, "big", signed=True)


def serialize_bool(value):
    if not isinstance(value, bool):
        raise ValueError("se esperaba un booleano")
    return bytes([int(value)])


def deserialize_bool(data):
    if len(data) != BOOL_SIZE or data[0] not in (0, 1):
        raise ValueError("booleano inválido")
    return bool(data[0])


def serialize_string(value):
    if not isinstance(value, str):
        raise ValueError("se esperaba un string")
    return value.encode("utf-8")


def deserialize_string(data):
    return data.decode("utf-8")


def serialize_bytes(value):
    return bytes(value)


def deserialize_bytes(data):
    return data


def _pack_length(length):
    return serialize_uint32(length)


def _serialize_value(value, depth):
    if depth > MAX_NESTING:
        raise ValueError("estructura demasiado anidada")

    if value is None:
        return bytes([_NULL])

    if isinstance(value, bool):
        return bytes([_BOOL]) + serialize_bool(value)

    if isinstance(value, int):
        return bytes([_INT]) + serialize_int64(value)

    if isinstance(value, float):
        return bytes([_FLOAT]) + struct.pack(">d", value)

    if isinstance(value, str):
        data = serialize_string(value)
        if len(data) > MAX_VALUE_SIZE:
            raise ValueError("string demasiado grande")
        return bytes([_STRING]) + _pack_length(len(data)) + data

    if isinstance(value, bytes):
        if len(value) > MAX_VALUE_SIZE:
            raise ValueError("bloque de bytes demasiado grande")
        return bytes([_BYTES]) + _pack_length(len(value)) + value

    if isinstance(value, (list, tuple)):
        if len(value) > MAX_COLLECTION_SIZE:
            raise ValueError("colección demasiado grande")

        tag = _LIST if isinstance(value, list) else _TUPLE
        parts = [bytes([tag]), _pack_length(len(value))]
        parts.extend(_serialize_value(item, depth + 1) for item in value)
        return b"".join(parts)

    if isinstance(value, dict):
        if len(value) > MAX_COLLECTION_SIZE:
            raise ValueError("diccionario demasiado grande")

        parts = [bytes([_DICT]), _pack_length(len(value))]
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("las claves deben ser strings")
            parts.append(_serialize_value(key, depth + 1))
            parts.append(_serialize_value(item, depth + 1))
        return b"".join(parts)

    raise ValueError(f"tipo no soportado: {type(value).__name__}")


def serialize_value(value):
    data = _serialize_value(value, 0)
    if len(data) > MAX_VALUE_SIZE:
        raise ValueError("valor serializado demasiado grande")
    return data


def _read_exact(data, offset, size):
    end = offset + size
    if end > len(data):
        raise ValueError("valor truncado")
    return data[offset:end], end


def _read_length(data, offset):
    raw, offset = _read_exact(data, offset, UINT32_SIZE)
    length = deserialize_uint32(raw)
    if length > MAX_VALUE_SIZE:
        raise ValueError("longitud demasiado grande")
    return length, offset


def _deserialize_value(data, offset, depth):
    if depth > MAX_NESTING:
        raise ValueError("estructura demasiado anidada")

    raw_tag, offset = _read_exact(data, offset, 1)
    tag = raw_tag[0]

    if tag == _NULL:
        return None, offset

    if tag == _BOOL:
        raw, offset = _read_exact(data, offset, BOOL_SIZE)
        return deserialize_bool(raw), offset

    if tag == _INT:
        raw, offset = _read_exact(data, offset, INT64_SIZE)
        return deserialize_int64(raw), offset

    if tag == _FLOAT:
        raw, offset = _read_exact(data, offset, FLOAT64_SIZE)
        return struct.unpack(">d", raw)[0], offset

    if tag in (_STRING, _BYTES):
        length, offset = _read_length(data, offset)
        raw, offset = _read_exact(data, offset, length)
        if tag == _STRING:
            return deserialize_string(raw), offset
        return raw, offset

    if tag in (_LIST, _TUPLE):
        count, offset = _read_length(data, offset)
        if count > MAX_COLLECTION_SIZE:
            raise ValueError("colección demasiado grande")

        values = []
        for _ in range(count):
            value, offset = _deserialize_value(data, offset, depth + 1)
            values.append(value)

        if tag == _TUPLE:
            return tuple(values), offset
        return values, offset

    if tag == _DICT:
        count, offset = _read_length(data, offset)
        if count > MAX_COLLECTION_SIZE:
            raise ValueError("diccionario demasiado grande")

        result = {}
        for _ in range(count):
            key, offset = _deserialize_value(data, offset, depth + 1)
            if not isinstance(key, str):
                raise ValueError("clave de diccionario inválida")
            if key in result:
                raise ValueError("clave duplicada")
            value, offset = _deserialize_value(data, offset, depth + 1)
            result[key] = value

        return result, offset

    raise ValueError(f"etiqueta de tipo desconocida: {tag}")


def deserialize_value(data):
    value, offset = _deserialize_value(data, 0, 0)
    if offset != len(data):
        raise ValueError("sobran bytes al final del valor")
    return value
