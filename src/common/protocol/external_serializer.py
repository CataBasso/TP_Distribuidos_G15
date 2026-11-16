
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



_ST_BYTE_LEN = struct.Struct(">BI")      
_ST_INT = struct.Struct(">Bq")
_ST_FLOAT = struct.Struct(">Bd")
_ST_U32 = struct.Struct(">I")
_NULL_B = bytes([_NULL])
_TRUE_B = bytes([_BOOL, 1])
_FALSE_B = bytes([_BOOL, 0])
_KEY_CACHE = {}                          
_KEY_CACHE_MAX = 4096
_KEY_DECODE = {}                          


def _ser(value, depth, out):
    if depth > MAX_NESTING:
        raise ValueError("estructura demasiado anidada")
    t = type(value)
    if t is str:
        data = value.encode("utf-8")
        if len(data) > MAX_VALUE_SIZE:
            raise ValueError("string demasiado grande")
        out.append(_ST_BYTE_LEN.pack(_STRING, len(data)))
        out.append(data)
    elif t is dict:
        if len(value) > MAX_COLLECTION_SIZE:
            raise ValueError("diccionario demasiado grande")
        out.append(_ST_BYTE_LEN.pack(_DICT, len(value)))
        cache = _KEY_CACHE
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("las claves deben ser strings")
            kb = cache.get(key)
            if kb is None:
                raw = key.encode("utf-8")
                kb = _ST_BYTE_LEN.pack(_STRING, len(raw)) + raw
                if len(cache) < _KEY_CACHE_MAX:
                    cache[key] = kb
            out.append(kb)
            _ser(item, depth + 1, out)
    elif value is None:
        out.append(_NULL_B)
    elif t is int:
        try:
            out.append(_ST_INT.pack(_INT, value))
        except struct.error as exc:
            raise ValueError("int64 fuera de rango") from exc
    elif t is bool:
        out.append(_TRUE_B if value else _FALSE_B)
    elif t is list or t is tuple:
        if len(value) > MAX_COLLECTION_SIZE:
            raise ValueError("colección demasiado grande")
        out.append(_ST_BYTE_LEN.pack(_LIST if t is list else _TUPLE, len(value)))
        for item in value:
            _ser(item, depth + 1, out)
    elif t is float:
        out.append(_ST_FLOAT.pack(_FLOAT, value))
    elif t is bytes:
        if len(value) > MAX_VALUE_SIZE:
            raise ValueError("bloque de bytes demasiado grande")
        out.append(_ST_BYTE_LEN.pack(_BYTES, len(value)))
        out.append(value)
    elif isinstance(value, (bool, int, float, str, bytes, list, tuple, dict)):
        # subclases (p. ej. IntEnum, namedtuple): mismo resultado que la versión original
        _ser_subclass(value, depth, out)
    else:
        raise ValueError(f"tipo no soportado: {type(value).__name__}")


def _ser_subclass(value, depth, out):
    if isinstance(value, bool):
        value = bool(value)
    elif isinstance(value, int):
        value = int(value)
    elif isinstance(value, float):
        value = float(value)
    elif isinstance(value, str):
        value = str(value)
    elif isinstance(value, bytes):
        value = bytes(value)
    elif isinstance(value, tuple):
        value = tuple(value)
    elif isinstance(value, list):
        value = list(value)
    else:
        value = dict(value)
    _ser(value, depth, out)


def serialize_value(value):
    out = []
    _ser(value, 0, out)
    data = b"".join(out)
    if len(data) > MAX_VALUE_SIZE:
        raise ValueError("valor serializado demasiado grande")
    return data


def _de(data, offset, depth):
    if depth > MAX_NESTING:
        raise ValueError("estructura demasiado anidada")
    try:
        tag = data[offset]
    except IndexError:
        raise ValueError("valor truncado") from None
    offset += 1

    if tag == _STRING or tag == _BYTES:
        if offset + 4 > len(data):
            raise ValueError("valor truncado")
        length = _ST_U32.unpack_from(data, offset)[0]
        if length > MAX_VALUE_SIZE:
            raise ValueError("longitud demasiado grande")
        offset += 4
        end = offset + length
        if end > len(data):
            raise ValueError("valor truncado")
        raw = data[offset:end]
        return (raw.decode("utf-8") if tag == _STRING else raw), end

    if tag == _DICT:
        if offset + 4 > len(data):
            raise ValueError("valor truncado")
        count = _ST_U32.unpack_from(data, offset)[0]
        if count > MAX_COLLECTION_SIZE:
            raise ValueError("diccionario demasiado grande")
        offset += 4
        result = {}
        kcache = _KEY_DECODE
        n = len(data)
        for _ in range(count):
            if offset >= n:
                raise ValueError("valor truncado")
            if data[offset] != _STRING:
                _, offset = _de(data, offset, depth + 1)      # mismo error que antes si no es str
                raise ValueError("clave de diccionario inválida")
            if offset + 5 > n:
                raise ValueError("valor truncado")
            klen = _ST_U32.unpack_from(data, offset + 1)[0]
            kend = offset + 5 + klen
            if kend > n:
                raise ValueError("valor truncado")
            raw = data[offset + 5:kend]
            key = kcache.get(raw)
            if key is None:
                key = raw.decode("utf-8")
                if len(kcache) < _KEY_CACHE_MAX:
                    kcache[raw] = key
            if key in result:
                raise ValueError("clave duplicada")
            result[key], offset = _de(data, kend, depth + 1)
        return result, offset

    if tag == _LIST or tag == _TUPLE:
        if offset + 4 > len(data):
            raise ValueError("valor truncado")
        count = _ST_U32.unpack_from(data, offset)[0]
        if count > MAX_VALUE_SIZE:
            raise ValueError("longitud demasiado grande")
        if count > MAX_COLLECTION_SIZE:
            raise ValueError("colección demasiado grande")
        offset += 4
        values = []
        append = values.append
        for _ in range(count):
            item, offset = _de(data, offset, depth + 1)
            append(item)
        return (tuple(values) if tag == _TUPLE else values), offset

    if tag == _NULL:
        return None, offset

    if tag == _INT:
        if offset + 8 > len(data):
            raise ValueError("valor truncado")
        return int.from_bytes(data[offset:offset + 8], "big", signed=True), offset + 8

    if tag == _BOOL:
        if offset + 1 > len(data):
            raise ValueError("valor truncado")
        b = data[offset]
        if b not in (0, 1):
            raise ValueError("booleano inválido")
        return bool(b), offset + 1

    if tag == _FLOAT:
        if offset + 8 > len(data):
            raise ValueError("valor truncado")
        return struct.unpack_from(">d", data, offset)[0], offset + 8

    raise ValueError(f"etiqueta de tipo desconocida: {tag}")


def deserialize_value(data):
    value, offset = _de(data, 0, 0)
    if offset != len(data):
        raise ValueError("sobran bytes al final del valor")
    return value
