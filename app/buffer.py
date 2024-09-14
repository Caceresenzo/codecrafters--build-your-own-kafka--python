import io
import struct
import typing

from . import varint


class ByteReader:

    def __init__(self, data: bytes):
        self._data = io.BytesIO(data)

    def read(self, n: int):
        return self._data.read(n)

    def read_signed_short(self):
        x, = struct.unpack("!h", self.read(2))
        return x

    def read_signed_int(self):
        x, = struct.unpack("!i", self.read(4))
        return x

    def read_unsigned_varint(self):
        return varint.read_unsigned(self._data)

    def read_string(self):
        length = self.read_signed_short()

        if length == -1:
            return None

        return self.read(length).decode("utf-8")

    def read_compact_string(self):
        length = self.read_unsigned_varint()

        if length == 0:
            return None

        return self.read(length - 1).decode("utf-8")

    def skip_empty_tagged_field_array(self):
        self.read_unsigned_varint()


T = typing.TypeVar("T")


class ByteWriter:

    def __init__(self):
        self._data = io.BytesIO()

    def write(self, bytes: bytes):
        self._data.write(bytes)

    def write_signed_short(self, value: int):
        self.write(struct.pack("!h", value))

    def write_signed_int(self, value: int):
        self.write(struct.pack("!i", value))

    def write_unsigned_varint(self, value: int):
        varint.write_unsigned(self._data, value)

    def write_compact_array(
        self,
        items: typing.List[T],
        serializer: typing.Callable[[T, "ByteWriter"], None]
    ):
        if items is None:
            self.write_unsigned_varint(0)
            return

        self.write_unsigned_varint(len(items) + 1)

        for item in items:
            serializer(item, self)

    def skip_empty_tagged_field_array(self):
        self.write_unsigned_varint(0)

    @property
    def bytes(self):
        return self._data.getvalue()
