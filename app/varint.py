import typing


def read_unsigned(stream: typing.BinaryIO):
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
        if shift >= 64:
            raise ValueError("varint is too long")

    return result


def write_unsigned(stream: typing.BinaryIO, value: int):
    while True:
        to_write = value & 0x7F
        value >>= 7

        if value:
            stream.write(bytes([to_write | 0x80]))
        else:
            stream.write(bytes([to_write]))
            break
