import dataclasses
import io
import socket
import struct
import typing

from . import varint


@dataclasses.dataclass
class RequestHeaderV2:
    request_api_key: int
    request_api_version: int
    correlation_id: int
    client_id: typing.Optional[str]


@dataclasses.dataclass
class Request:
    header: RequestHeaderV2


@dataclasses.dataclass
class ApiVersionsRequestV4(Request):

    client_software_name: typing.Optional[str]
    client_software_version: typing.Optional[str]

    @staticmethod
    def parse(header: RequestHeaderV2, reader: "ByteReader"):
        reader.skip_empty_tagged_field_array()

        client_software_name = reader.read_compact_string()
        client_software_version = reader.read_compact_string()

        reader.skip_empty_tagged_field_array()

        return ApiVersionsRequestV4(
            header,
            client_software_name,
            client_software_version,
        )


class ByteReader:

    def __init__(self, data: bytes):
        self._data = io.BytesIO(data)

    def read_signed_short(self):
        x, = struct.unpack("!h", self._data.read(2))
        return x

    def read_signed_int(self):
        x, = struct.unpack("!i", self._data.read(4))
        return x

    def read_unsigned_varint(self):
        return varint.read(self._data)

    def read_string(self):
        length = self.read_signed_short()

        if length == -1:
            return None

        return self._data.read(length).decode("utf-8")

    def read_compact_string(self):
        length = self.read_unsigned_varint()

        if length == 0:
            return None

        return self._data.read(length - 1).decode("utf-8")

    def skip_empty_tagged_field_array(self):
        self.read_unsigned_varint()



class MessageReader:

    REGISTRY = {
        (18, 4): ApiVersionsRequestV4
    }

    def __init__(self, socket: socket.socket):
        self.socket = socket

    def next(self) -> Request:
        message_size, = struct.unpack("!i", self.socket.recv(4))
        data = self.socket.recv(message_size)

        reader = ByteReader(data)

        api_key = reader.read_signed_short()
        api_version = reader.read_signed_short()

        correlation_id = reader.read_signed_int()
        client_id = reader.read_string()

        header = RequestHeaderV2(
            api_key,
            api_version,
            correlation_id,
            client_id,
        )

        print(header)

        clazz = self.REGISTRY[(api_key, api_version)]
        return clazz.parse(header, reader)
