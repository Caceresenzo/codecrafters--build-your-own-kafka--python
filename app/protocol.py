
import socket
import struct
import typing

from . import buffer
from .messages.api_versions import *
from .messages.base import *
from .messages.error import *
from .messages.fetch import *


class ProtocolError(ValueError):

    def __init__(
        self,
        error_code: ErrorCode,
        *args,
        correlation_id: typing.Optional[int] = None
    ):
        self.error_code = error_code
        self.correlation_id = correlation_id

        if not len(args):
            args = (error_code.name, )

        super().__init__(*args)


class MessageReader:

    DESERIALIZERS = {
        (1, 16): FetchRequestV16.deserialize,
        (18, 4): ApiVersionsRequestV4.deserialize,
    }

    def __init__(self, socket: socket.socket):
        self._socket = socket

    def next(self) -> Request:
        data = self._next_message()
        reader = buffer.ByteReader(data)

        header = RequestHeaderV2.deserialize(reader)

        index = (header.request_api_key, header.request_api_version)
        deserializer = self.DESERIALIZERS.get(index)
        if deserializer is None:
            raise ProtocolError(
                ErrorCode.UNSUPPORTED_VERSION,
                correlation_id=header.correlation_id
            )

        body = deserializer(reader)

        return Request(header, body)

    def _next_message(self):
        data = self._socket.recv(4)
        if not len(data):
            raise EOFError("could not read message size")

        message_size, = struct.unpack("!i", data)
        data = self._socket.recv(message_size)

        if len(data) != message_size:
            raise EOFError("message size does not match")
        
        print("read", data.hex())
        return data


class MessageWriter:

    SERIALIZERS = {
        (ApiVersionsResponseV4): ApiVersionsResponseV4.serialize,
        (FetchResponseV16): FetchResponseV16.serialize,
    }

    def __init__(self, socket: socket.socket):
        self._socket = socket

    def send(
        self,
        response: Response
    ):
        writer = buffer.ByteWriter()
        response.serialize(writer)

        bytes = writer.bytes

        self._socket.send(struct.pack(
            "!i",
            len(bytes),
        ))

        self._socket.send(bytes)

    def send_error(
        self,
        correlation_id: int,
        error_code: ErrorCode
    ):
        self._socket.send(struct.pack(
            "!iih",
            4 + 2,
            correlation_id,
            error_code.value,
        ))
