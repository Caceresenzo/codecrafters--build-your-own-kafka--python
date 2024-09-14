import io
import struct
import typing
import uuid

from . import varint

T = typing.TypeVar("T")


class ByteReader:

    def __init__(self, data: bytes):
        self._data = io.BytesIO(data)

    def read(self, n: int):
        return self._data.read(n)

    def read_signed_char(self):
        x, = struct.unpack("!b", self.read(1))
        return x

    def read_signed_short(self):
        x, = struct.unpack("!h", self.read(2))
        return x

    def read_signed_int(self):
        x, = struct.unpack("!i", self.read(4))
        return x

    def read_signed_long(self):
        x, = struct.unpack("!q", self.read(8))
        return x

    def read_unsigned_varint(self):
        return varint.read_unsigned(self._data)

    def read_uuid(self):
        return uuid.UUID(bytes=self.read(16))

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

    def read_compact_array(
        self,
        deserializer: typing.Callable[["ByteWriter"], T]
    ) -> typing.Optional[typing.List[T]]:
        length = self.read_unsigned_varint()

        if length == 0:
            return None

        return [
            deserializer(self)
            for _ in range(length - 1)
        ]


class ByteWriter:

    def __init__(self):
        self._data = io.BytesIO()

    def write(self, bytes: bytes):
        self._data.write(bytes)

    def write_signed_short(self, value: int):
        self.write(struct.pack("!h", value))

    def write_signed_int(self, value: int):
        self.write(struct.pack("!i", value))

    def write_signed_long(self, value: int):
        self.write(struct.pack("!q", value))

    def write_unsigned_varint(self, value: int):
        varint.write_unsigned(self._data, value)

    def write_uuid(self, value: uuid.UUID):
        self.write(value.bytes)

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

    def write_compact_records(
        self,
        records: bytes,
    ):
        if records is None:
            self.write_unsigned_varint(0)
            return

        self.write_unsigned_varint(len(records) + 1)
        self.write(records)

    def skip_empty_tagged_field_array(self):
        self.write_unsigned_varint(0)

    @property
    def bytes(self):
        return self._data.getvalue()
