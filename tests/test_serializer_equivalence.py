"""El serializador rápido produce los MISMOS bytes y acepta/rechaza lo mismo que el original."""
import random
import struct

import pytest

import reference_serializer as ref
from common.protocol import external_serializer as fast


def _rand_value(rng, depth=0):
    kinds = ["none", "bool", "int", "float", "str", "bytes"] + (["list", "tuple", "dict"] if depth < 4 else [])
    k = rng.choice(kinds)
    if k == "none": return None
    if k == "bool": return rng.random() < .5
    if k == "int": return rng.choice([0, -1, 2**63 - 1, -2**63, rng.randint(-10**6, 10**6)])
    if k == "float": return rng.uniform(-1e6, 1e6)
    if k == "str": return "".join(rng.choice("abcñé漢 \n") for _ in range(rng.randint(0, 30)))
    if k == "bytes": return bytes(rng.randint(0, 255) for _ in range(rng.randint(0, 20)))
    if k == "list": return [_rand_value(rng, depth + 1) for _ in range(rng.randint(0, 5))]
    if k == "tuple": return tuple(_rand_value(rng, depth + 1) for _ in range(rng.randint(0, 5)))
    return {f"k{rng.randint(0, 9)}": _rand_value(rng, depth + 1) for _ in range(rng.randint(0, 5))}


def test_same_bytes_and_same_roundtrip_on_random_values():
    rng = random.Random(7)
    for _ in range(1500):
        v = _rand_value(rng)
        a, b = ref.serialize_value(v), fast.serialize_value(v)
        assert a == b
        assert fast.deserialize_value(a) == ref.deserialize_value(a)


@pytest.mark.parametrize("bad", [2**63, -2**63 - 1, {1: 2}, object(), {"a": {1: 2}}, bytearray(b"x")])
def test_same_serialization_errors(bad):
    with pytest.raises(ValueError):
        ref.serialize_value(bad)
    with pytest.raises(ValueError):
        fast.serialize_value(bad)


def test_same_behavior_on_subclasses():
    import enum, collections
    class E(enum.IntEnum):
        A = 3
    P = collections.namedtuple("P", "x y")
    for v in (E.A, P(1, 2), True):
        assert ref.serialize_value(v) == fast.serialize_value(v)


def test_same_rejections_on_corrupt_input():
    rng = random.Random(11)
    base = [ref.serialize_value(_rand_value(rng)) for _ in range(200)]
    for data in base:
        for cut in (0, 1, len(data) // 2, max(len(data) - 1, 0)):
            truncated = data[:cut]
            for fn in (ref.deserialize_value, fast.deserialize_value):
                try:
                    fn(truncated)
                except ValueError:
                    pass
            a = _outcome(ref.deserialize_value, truncated)
            b = _outcome(fast.deserialize_value, truncated)
            assert a == b
        mutated = bytearray(data)
        if mutated:
            mutated[rng.randrange(len(mutated))] ^= 0xFF
        assert _outcome(ref.deserialize_value, bytes(mutated)) == _outcome(fast.deserialize_value, bytes(mutated))


def _outcome(fn, data):
    try:
        return ("ok", fn(data))
    except ValueError:
        return ("error",)
    except UnicodeDecodeError:
        return ("error",)


def test_duplicate_key_is_rejected():
    dup = bytes([8]) + struct.pack(">I", 2) + ref.serialize_value("a") + ref.serialize_value(1) \
        + ref.serialize_value("a") + ref.serialize_value(2)
    with pytest.raises(ValueError):
        fast.deserialize_value(dup)
