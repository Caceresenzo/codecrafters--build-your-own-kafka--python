import typing


def zigzag_encode(value):
    if value >= 0:
        return value << 1

    return (value << 1) ^ (~0)


def zigzag_decode(value):
    if not value & 0x1:
        return value >> 1

    return (value >> 1) ^ (~0)


def read_signed_int(stream: typing.BinaryIO) -> int:
    return zigzag_decode(read_unsigned_int(stream))


def read_unsigned_int(stream: typing.BinaryIO):
    return _read(stream, 32)


def write_signed_int(stream: typing.BinaryIO, value: int):
    write_unsigned_int(stream, zigzag_encode(value))


def write_unsigned_int(stream: typing.BinaryIO, value: int):
    _write(stream, value)


def read_signed_long(stream: typing.BinaryIO) -> int:
    return zigzag_decode(read_unsigned_long(stream))


def read_unsigned_long(stream: typing.BinaryIO):
    return _read(stream, 64)


def write_signed_long(stream: typing.BinaryIO, value: int):
    write_unsigned_long(stream, zigzag_encode(value))


def write_unsigned_long(stream: typing.BinaryIO, value: int):
    _write(stream, value)


def _read(stream: typing.BinaryIO, bits: int):
    result = 0
    shift = 0

    while True:
        byte = stream.read(1)
        if not byte:
            raise EOFError("unexpected while reading varint")

        byte_value = ord(byte)

        result |= (byte_value & 0x7F) << shift

        if not (byte_value & 0x80):
            break

        shift += 7
        if shift >= bits:
            raise ValueError("varint is too long")

    return result


def _write(stream: typing.BinaryIO, value: int):
    while True:
        to_write = value & 0x7F
        value >>= 7

        if value:
            stream.write(bytes([to_write | 0x80]))
        else:
            stream.write(bytes([to_write]))
            break
