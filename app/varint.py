import typing


def read(stream: typing.BinaryIO):
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
