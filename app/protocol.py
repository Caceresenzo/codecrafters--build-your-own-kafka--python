import dataclasses
import enum
import socket
import struct
import typing
import uuid

from . import buffer


@enum.unique
class ErrorCode(enum.Enum):

    NONE = 0
    UNKNOWN_SERVER_ERROR = -1
    UNSUPPORTED_VERSION = 35


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
class Response:

    def serialize(self, writer: buffer.ByteWriter):
        pass


@dataclasses.dataclass
class ApiVersionsRequestV4(Request):
    client_software_name: typing.Optional[str]
    client_software_version: typing.Optional[str]

    @staticmethod
    def deserialize(header: RequestHeaderV2, reader: buffer.ByteReader):
        reader.skip_empty_tagged_field_array()

        client_software_name = reader.read_compact_string()
        client_software_version = reader.read_compact_string()

        reader.skip_empty_tagged_field_array()

        return ApiVersionsRequestV4(
            header,
            client_software_name,
            client_software_version,
        )


@dataclasses.dataclass
class ApiVersionsResponseKeyV4:
    api_key: int
    min_version: int
    max_version: int

    def serialize(self, writer: buffer.ByteWriter):
        writer.write_signed_short(self.api_key)
        writer.write_signed_short(self.min_version)
        writer.write_signed_short(self.max_version)
        writer.skip_empty_tagged_field_array()


@dataclasses.dataclass
class ApiVersionsResponseV4(Response):
    api_keys: typing.List[ApiVersionsResponseKeyV4]
    throttle_time_ms: int

    def serialize(self, writer: buffer.ByteWriter):
        writer.write_compact_array(self.api_keys, ApiVersionsResponseKeyV4.serialize)
        writer.write_signed_int(self.throttle_time_ms)
        writer.skip_empty_tagged_field_array()


@dataclasses.dataclass
class FetchRequestTopicPartitionV16:

    partition: int
    current_leader_epoch: int
    fetch_offset: int
    last_fetched_epoch: int
    log_start_offset: int
    partition_max_bytes: int

    @staticmethod
    def deserialize(reader: buffer.ByteReader):
        partition = reader.read_signed_int()
        current_leader_epoch = reader.read_signed_int()
        fetch_offset = reader.read_signed_long()
        last_fetched_epoch = reader.read_signed_int()
        log_start_offset = reader.read_signed_long()
        partition_max_bytes = reader.read_signed_int()

        reader.skip_empty_tagged_field_array()

        return FetchRequestTopicPartitionV16(
            partition,
            current_leader_epoch,
            fetch_offset,
            last_fetched_epoch,
            log_start_offset,
            partition_max_bytes,
        )


@dataclasses.dataclass
class FetchRequestTopicV16:

    topic_id: uuid.UUID
    partitions: typing.List[FetchRequestTopicPartitionV16]

    @staticmethod
    def deserialize(reader: buffer.ByteReader):
        topic_id = reader.read_uuid()
        partitions = reader.read_compact_array(FetchRequestTopicPartitionV16.deserialize)

        reader.skip_empty_tagged_field_array()

        return FetchRequestTopicV16(
            topic_id,
            partitions
        )


@dataclasses.dataclass
class FetchRequestForgottenTopicsDataV16:

    topic_id: uuid.UUID
    partitions: typing.List[int]

    @staticmethod
    def deserialize(reader: buffer.ByteReader):
        topic_id = reader.read_uuid()
        partitions = reader.read_compact_array(buffer.ByteReader.read_signed_int)

        reader.skip_empty_tagged_field_array()

        return FetchRequestTopicV16(
            topic_id,
            partitions
        )


@dataclasses.dataclass
class FetchRequestV16(Request):

    max_wait_ms: int
    min_bytes: int
    max_bytes: int
    isolation_level: int
    session_id: int
    session_epoch: int
    topics: typing.List[FetchRequestTopicV16]
    forgotten_topics_data: typing.List[FetchRequestForgottenTopicsDataV16]
    rack_id: str

    @staticmethod
    def deserialize(header: RequestHeaderV2, reader: buffer.ByteReader):
        max_wait_ms = reader.read_signed_int()
        min_bytes = reader.read_signed_int()
        max_bytes = reader.read_signed_char()
        isolation_level = reader.read_signed_char()
        session_id = reader.read_signed_int()
        session_epoch = reader.read_signed_int()
        topics = reader.read_compact_array(FetchRequestTopicV16.deserialize)
        forgotten_topics_data = reader.read_compact_array(FetchRequestForgottenTopicsDataV16.deserialize)
        rack_id = reader.read_compact_string()

        return FetchRequestV16(
            header,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
        )


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

        header = RequestHeaderV2(
            api_key,
            api_version,
            correlation_id,
            client_id,
        )

        if api_version not in [0, 1, 2, 3, 4]:
            raise ProtocolError(
                ErrorCode.UNSUPPORTED_VERSION,
                correlation_id=correlation_id
            )

        deserializer = self.DESERIALIZERS[(api_key, api_version)]
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
        (ApiVersionsResponseV4): ApiVersionsResponseV4.serialize
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
