
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

        api_key = reader.read_signed_short()
        api_version = reader.read_signed_short()

        correlation_id = reader.read_signed_int()
        client_id = reader.read_string()

        reader.skip_empty_tagged_field_array()

        header = RequestHeaderV2(
            api_key,
            api_version,
            correlation_id,
            client_id,
        )

        deserializer = self.DESERIALIZERS.get((api_key, api_version))
        if deserializer is None:
            raise ProtocolError(
                ErrorCode.UNSUPPORTED_VERSION,
                correlation_id=correlation_id
            )

        return deserializer(header, reader)

    def _next_message(self):
        data = self._socket.recv(4)
        if not len(data):
            raise EOFError("could not read message size")

        message_size, = struct.unpack("!i", data)
        data = self._socket.recv(message_size)

        if len(data) != message_size:
            raise EOFError("message size does not match")

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
        correlation_id: int,
        response: Response
    ):
        writer = buffer.ByteWriter()
        response.serialize(writer)

        self._send(
            correlation_id,
            ErrorCode.NONE,
            writer.bytes
        )

    def send_error(
        self,
        correlation_id: int,
        error_code: ErrorCode
    ):
        self._send(
            correlation_id,
            error_code,
            bytes()
        )

    def _send(
        self,
        correlation_id: int,
        error_code: ErrorCode,
        body: bytes
    ):
        self._socket.send(struct.pack(
            "!iih",
            4 + 2 + len(body),
            correlation_id,
            error_code.value,
        ))

        self._socket.send(body)
