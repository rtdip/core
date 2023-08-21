# Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import struct
import uuid
from typing import cast, List, Callable
from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import MapType, StringType

SYSTEM_PROPERTIES = {
    "x-opt-sequence-number": b"\x52",
    "x-opt-offset": b"\xa1",
    "x-opt-partition-key": b"\xa1",
    "x-opt-enqueued-time": b"\x83",
    "message-id": b"\xa1",
    "user-id": b"\xa1",
    "to": b"\xa1",
    "subject": b"\xa1",
    "reply-to": b"\xa1",
    "correlation-id": b"\xa1",
    "content-type": b"\xa1",
    "content-encoding": b"\xa1",
    "absolute-expiry-time": b"\x83",
    "creation-time": b"\x83",
    "group-id": b"\xa1",
    "group-sequence": b"\xa1",
    "reply-to-group-id": b"\xa1",
}

c_unsigned_char = struct.Struct(">B")
c_signed_char = struct.Struct(">b")
c_unsigned_short = struct.Struct(">H")
c_signed_short = struct.Struct(">h")
c_unsigned_int = struct.Struct(">I")
c_signed_int = struct.Struct(">i")
c_unsigned_long = struct.Struct(">L")
c_unsigned_long_long = struct.Struct(">Q")
c_signed_long_long = struct.Struct(">q")
c_float = struct.Struct(">f")
c_double = struct.Struct(">d")


def _decode_null(buffer):
    return buffer, None


def _decode_true(buffer):
    return buffer, True


def _decode_false(buffer):
    return buffer, False


def _decode_zero(buffer):
    return buffer, 0


def _decode_empty(buffer):
    return buffer, []


def _decode_boolean(buffer):
    return buffer[1:], buffer[:1] == b"\x01"


def _decode_ubyte(buffer):
    return buffer[1:], buffer[0]


def _decode_ushort(buffer):
    return buffer[2:], c_unsigned_short.unpack(buffer[:2])[0]


def _decode_uint_small(buffer):
    return buffer[1:], buffer[0]


def _decode_uint_large(buffer):
    return buffer[4:], c_unsigned_int.unpack(buffer[:4])[0]


def _decode_ulong_small(buffer):
    return buffer[1:], buffer[0]


def _decode_ulong_large(buffer):
    return buffer[8:], c_unsigned_long_long.unpack(buffer[:8])[0]


def _decode_byte(buffer):
    return buffer[1:], c_signed_char.unpack(buffer[:1])[0]


def _decode_short(buffer):
    return buffer[2:], c_signed_short.unpack(buffer[:2])[0]


def _decode_int_small(buffer):
    return buffer[1:], c_signed_char.unpack(buffer[:1])[0]


def _decode_int_large(buffer):
    return buffer[4:], c_signed_int.unpack(buffer[:4])[0]


def _decode_long_small(buffer):
    return buffer[1:], c_signed_char.unpack(buffer[:1])[0]


def _decode_long_large(buffer):
    return buffer[8:], c_signed_long_long.unpack(buffer[:8])[0]


def _decode_float(buffer):
    return buffer[4:], c_float.unpack(buffer[:4])[0]


def _decode_double(buffer):
    return buffer[8:], c_double.unpack(buffer[:8])[0]


def _decode_timestamp(buffer):
    return buffer[8:], c_signed_long_long.unpack(buffer[:8])[0]


def _decode_uuid(buffer):
    return buffer[16:], uuid.UUID(bytes=buffer[:16].tobytes())


def _decode_binary_small(buffer):
    length_index = buffer[0] + 1
    return buffer[length_index:], buffer[1:length_index].tobytes()


def _decode_binary_large(buffer):
    length_index = c_unsigned_long.unpack(buffer[:4])[0] + 4
    return buffer[length_index:], buffer[4:length_index].tobytes()


def _decode_list_small(buffer):
    count = buffer[1]
    buffer = buffer[2:]
    values = [None] * count
    for i in range(count):
        buffer, values[i] = _DECODE_BY_CONSTRUCTOR[buffer[0]](buffer[1:])
    return buffer, values


def _decode_list_large(buffer):
    count = c_unsigned_long.unpack(buffer[4:8])[0]
    buffer = buffer[8:]
    values = [None] * count
    for i in range(count):
        buffer, values[i] = _DECODE_BY_CONSTRUCTOR[buffer[0]](buffer[1:])
    return buffer, values


def _decode_map_small(buffer):
    count = int(buffer[1] / 2)
    buffer = buffer[2:]
    values = {}
    for _ in range(count):
        buffer, key = _DECODE_BY_CONSTRUCTOR[buffer[0]](buffer[1:])
        buffer, value = _DECODE_BY_CONSTRUCTOR[buffer[0]](buffer[1:])
        values[key] = value
    return buffer, values


def _decode_map_large(buffer):
    count = int(c_unsigned_long.unpack(buffer[4:8])[0] / 2)
    buffer = buffer[8:]
    values = {}
    for _ in range(count):
        buffer, key = _DECODE_BY_CONSTRUCTOR[buffer[0]](buffer[1:])
        buffer, value = _DECODE_BY_CONSTRUCTOR[buffer[0]](buffer[1:])
        values[key] = value
    return buffer, values


def _decode_array_small(buffer):
    count = buffer[1]  # Ignore first byte (size) and just rely on count
    if count:
        subconstructor = buffer[2]
        buffer = buffer[3:]
        values = [None] * count
        for i in range(count):
            buffer, values[i] = _DECODE_BY_CONSTRUCTOR[subconstructor](buffer)
        return buffer, values
    return buffer[2:], []


def _decode_array_large(buffer):
    count = c_unsigned_long.unpack(buffer[4:8])[0]
    if count:
        subconstructor = buffer[8]
        buffer = buffer[9:]
        values = [None] * count
        for i in range(count):
            buffer, values[i] = _DECODE_BY_CONSTRUCTOR[subconstructor](buffer)
        return buffer, values
    return buffer[8:], []


_COMPOSITES = {
    35: "received",
    36: "accepted",
    37: "rejected",
    38: "released",
    39: "modified",
}


def _decode_described(buffer):
    composite_type = buffer[0]
    buffer, descriptor = _DECODE_BY_CONSTRUCTOR[composite_type](buffer[1:])
    buffer, value = _DECODE_BY_CONSTRUCTOR[buffer[0]](buffer[1:])
    try:
        composite_type = cast(int, _COMPOSITES[descriptor])
        return buffer, {composite_type: value}
    except KeyError:
        return buffer, value


_DECODE_BY_CONSTRUCTOR: List[Callable] = cast(List[Callable], [None] * 256)
_DECODE_BY_CONSTRUCTOR[0] = _decode_described
_DECODE_BY_CONSTRUCTOR[64] = _decode_null
_DECODE_BY_CONSTRUCTOR[65] = _decode_true
_DECODE_BY_CONSTRUCTOR[66] = _decode_false
_DECODE_BY_CONSTRUCTOR[67] = _decode_zero
_DECODE_BY_CONSTRUCTOR[68] = _decode_zero
_DECODE_BY_CONSTRUCTOR[69] = _decode_empty
_DECODE_BY_CONSTRUCTOR[80] = _decode_ubyte
_DECODE_BY_CONSTRUCTOR[81] = _decode_byte
_DECODE_BY_CONSTRUCTOR[82] = _decode_uint_small
_DECODE_BY_CONSTRUCTOR[83] = _decode_ulong_small
_DECODE_BY_CONSTRUCTOR[84] = _decode_int_small
_DECODE_BY_CONSTRUCTOR[85] = _decode_long_small
_DECODE_BY_CONSTRUCTOR[86] = _decode_boolean
_DECODE_BY_CONSTRUCTOR[96] = _decode_ushort
_DECODE_BY_CONSTRUCTOR[97] = _decode_short
_DECODE_BY_CONSTRUCTOR[112] = _decode_uint_large
_DECODE_BY_CONSTRUCTOR[113] = _decode_int_large
_DECODE_BY_CONSTRUCTOR[114] = _decode_float
_DECODE_BY_CONSTRUCTOR[128] = _decode_ulong_large
_DECODE_BY_CONSTRUCTOR[129] = _decode_long_large
_DECODE_BY_CONSTRUCTOR[130] = _decode_double
_DECODE_BY_CONSTRUCTOR[131] = _decode_timestamp
_DECODE_BY_CONSTRUCTOR[152] = _decode_uuid
_DECODE_BY_CONSTRUCTOR[160] = _decode_binary_small
_DECODE_BY_CONSTRUCTOR[161] = _decode_binary_small
_DECODE_BY_CONSTRUCTOR[163] = _decode_binary_small
_DECODE_BY_CONSTRUCTOR[176] = _decode_binary_large
_DECODE_BY_CONSTRUCTOR[177] = _decode_binary_large
_DECODE_BY_CONSTRUCTOR[179] = _decode_binary_large
_DECODE_BY_CONSTRUCTOR[192] = _decode_list_small
_DECODE_BY_CONSTRUCTOR[193] = _decode_map_small
_DECODE_BY_CONSTRUCTOR[208] = _decode_list_large
_DECODE_BY_CONSTRUCTOR[209] = _decode_map_large
_DECODE_BY_CONSTRUCTOR[224] = _decode_array_small
_DECODE_BY_CONSTRUCTOR[240] = _decode_array_large


def _decode_to_string(decoder_value, value):
    if decoder_value == b"\x83":
        return datetime.fromtimestamp(int(value) / 1000).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
    elif type(value) is bytes or type(value) is bytearray:
        return value.decode("utf-8")
    else:
        return str(value)


@udf(returnType=MapType(StringType(), StringType()))
def decode_kafka_headers_to_amqp_properties(headers: dict) -> dict:
    if headers is None or len(headers) == 0 or type(headers) is not dict:
        return {}
    else:
        properties = {}
        for key, value in headers.items():
            try:
                if key in SYSTEM_PROPERTIES:
                    properties[key] = _decode_to_string(SYSTEM_PROPERTIES[key], value)
                else:
                    decoder_value = value[0:1]
                    buffer_val = memoryview(value)
                    buffer_val, decoded_value = _DECODE_BY_CONSTRUCTOR[buffer_val[0]](
                        buffer_val[1:]
                    )
                    properties[key] = _decode_to_string(decoder_value, decoded_value)
            except Exception as e:
                print(f"Error decoding header {key}: {e}")
                properties[key] = _decode_to_string(None, value)
        return properties
