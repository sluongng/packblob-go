"""Decode Go-produced envelopes with Apache Thrift and the system libzstd.

Run through TestInteroperability with PACKBLOB_INTEROP_PYTHON pointing to a
Python interpreter with the `thrift` package installed. No generated Go codec
or Go compression implementation is used by this checker.
"""

import base64
import ctypes
import ctypes.util
import hashlib
import json
import sys

from thrift.Thrift import TType
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.transport.TTransport import TMemoryBuffer


# Each schema maps a field ID to (wire type, nested schema/element schema).
SINGLE = {1: (TType.STRING, None), 2: (TType.STRING, None)}
DICT = {1: (TType.STRING, None), 2: (TType.STRING, None)}
VALUE = {1: (TType.STRUCT, SINGLE), 2: (TType.STRUCT, DICT)}
ENTRY = {1: (TType.STRING, None), 2: (TType.STRUCT, VALUE)}
PACK = {1: (TType.STRING, None), 2: (TType.LIST, ENTRY)}
STORAGE = {1: (TType.STRUCT, SINGLE), 2: (TType.STRUCT, PACK)}
ENVELOPE = {1: (TType.STRUCT, STORAGE)}
UNIONS = (SINGLE, VALUE, STORAGE)


def read_struct(protocol, schema):
    result = {}
    protocol.readStructBegin()
    while True:
        _, wire_type, field_id = protocol.readFieldBegin()
        if wire_type == TType.STOP:
            break
        assert field_id in schema, f"unknown field {field_id}"
        assert field_id not in result, f"duplicate field {field_id}"
        expected_type, child = schema[field_id]
        assert wire_type == expected_type, f"wrong type for field {field_id}"
        if wire_type == TType.STRING:
            value = protocol.readBinary()
        elif wire_type == TType.STRUCT:
            value = read_struct(protocol, child)
        elif wire_type == TType.LIST:
            element_type, count = protocol.readListBegin()
            assert element_type == TType.STRUCT, "wrong list element type"
            assert count >= 0, "negative list length"
            value = [read_struct(protocol, child) for _ in range(count)]
            protocol.readListEnd()
        else:
            raise AssertionError(f"unsupported schema type {wire_type}")
        result[field_id] = value
        protocol.readFieldEnd()
    protocol.readStructEnd()
    if any(schema is union for union in UNIONS):
        assert len(result) == 1, "union must contain exactly one field"
    else:
        assert result.keys() == schema.keys(), "missing required field"
    return result


class Zstd:
    def __init__(self):
        path = ctypes.util.find_library("zstd")
        assert path, "libzstd was not found"
        self.lib = ctypes.CDLL(path)
        self.lib.ZSTD_createDCtx.restype = ctypes.c_void_p
        self.lib.ZSTD_freeDCtx.argtypes = [ctypes.c_void_p]
        self.lib.ZSTD_decompress_usingDict.argtypes = [
            ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t,
            ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t,
        ]
        self.lib.ZSTD_decompress_usingDict.restype = ctypes.c_size_t
        self.lib.ZSTD_isError.argtypes = [ctypes.c_size_t]
        self.lib.ZSTD_isError.restype = ctypes.c_uint
        self.lib.ZSTD_getErrorName.argtypes = [ctypes.c_size_t]
        self.lib.ZSTD_getErrorName.restype = ctypes.c_char_p
        self.lib.ZSTD_getDictID_fromFrame.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        self.lib.ZSTD_getDictID_fromFrame.restype = ctypes.c_uint
        self.lib.ZSTD_findFrameCompressedSize.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        self.lib.ZSTD_findFrameCompressedSize.restype = ctypes.c_size_t

    def decode(self, data, expected_size, dictionary=b""):
        # Raw-content dictionaries do not carry a trained dictionary ID.
        assert self.lib.ZSTD_getDictID_fromFrame(data, len(data)) == 0
        frame_size = self.lib.ZSTD_findFrameCompressedSize(data, len(data))
        self.check(frame_size)
        assert frame_size == len(data), "trailing compressed-frame data"
        context = self.lib.ZSTD_createDCtx()
        assert context, "could not allocate Zstandard decoder"
        try:
            output = ctypes.create_string_buffer(max(1, expected_size))
            size = self.lib.ZSTD_decompress_usingDict(
                context, output, expected_size, data, len(data), dictionary, len(dictionary)
            )
            self.check(size)
            assert size == expected_size, "wrong decoded length"
            return output.raw[:size]
        finally:
            self.lib.ZSTD_freeDCtx(context)

    def check(self, result):
        assert not self.lib.ZSTD_isError(result), self.lib.ZSTD_getErrorName(result).decode()


def decode_single(zstd, single, expected):
    if 1 in single:
        return "raw", single[1]
    return "zstd", zstd.decode(single[2], len(expected))


def verify_case(zstd, case):
    encoded = base64.b64decode(case["data"], validate=True)
    assert encoded[:4] == b"\0\0\0\0", "wrong four-byte format header"
    transport = TMemoryBuffer(encoded[4:])
    envelope = read_struct(TCompactProtocol(transport), ENVELOPE)
    assert transport.read(1) == b"", "trailing Thrift input"
    storage = envelope[1]
    expected = case["entries"]
    if case["kind"] == "single":
        assert set(storage) == {1}, "expected standalone storage"
        assert len(expected) == 1
        want = base64.b64decode(expected[0]["data"], validate=True)
        variant, actual = decode_single(zstd, storage[1], want)
        assert actual == want, "standalone contents differ"
        assert variant == expected[0]["variant"], "wrong standalone variant"
        return
    assert set(storage) == {2}, "expected packed storage"
    packed = storage[2]
    keys = [entry["key"] for entry in expected]
    digest = hashlib.blake2b(digest_size=32, key=b"pack")
    for key in sorted(keys, key=lambda item: item.encode("utf-8")):
        encoded_key = key.encode("utf-8")
        digest.update(len(encoded_key).to_bytes(8, "little"))
        digest.update(encoded_key)
    pack_key = case["prefix"] + digest.hexdigest() + ".pack"
    assert case["key"] == pack_key, "returned pack key differs from independent hash"
    assert packed[1].decode("utf-8") == pack_key, "embedded pack key differs"
    assert sorted(case["link_keys"]) == sorted(keys), "returned link keys differ"
    assert len(packed[2]) == len(expected), "wrong entry count"
    decoded = {}
    for entry, want_entry in zip(packed[2], expected):
        key = entry[1].decode("utf-8")
        assert key == want_entry["key"], "wrong entry order/key"
        assert key not in decoded, "duplicate packed key"
        want = base64.b64decode(want_entry["data"], validate=True)
        value = entry[2]
        if 1 in value:
            variant, actual = decode_single(zstd, value[1], want)
        else:
            variant = "delta"
            dictionary_key = value[2][1].decode("utf-8")
            assert dictionary_key == want_entry["dict_key"], "wrong dictionary key"
            assert dictionary_key in decoded, "dictionary missing or forward-referenced"
            actual = zstd.decode(value[2][2], len(want), decoded[dictionary_key])
            if want_entry["requires_dictionary"]:
                try:
                    without_dictionary = zstd.decode(value[2][2], len(want))
                except AssertionError:
                    pass
                else:
                    assert without_dictionary != want, "delta does not use its dictionary"
        assert actual == want, f"contents differ for {key}"
        if want_entry["variant"]:
            assert variant == want_entry["variant"], f"wrong variant for {key}: {variant}"
        decoded[key] = actual


def main():
    cases = json.load(sys.stdin)
    zstd = Zstd()
    for case in cases:
        try:
            verify_case(zstd, case)
        except Exception as error:
            raise AssertionError(f'{case["name"]}: {error}') from error
    print(f"Apache Thrift/libzstd verified {len(cases)} envelopes")


if __name__ == "__main__":
    main()
