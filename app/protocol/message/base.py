import dataclasses
import typing

from ... import buffer


class RequestHeader:
    request_api_key: int
    request_api_version: int
    correlation_id: int


class RequestBody:
    pass


class ResponseHeader:

    def serialize(self, writer: buffer.ByteWriter):
        raise NotImplementedError()


class ResponseBody:

    def serialize(self, writer: buffer.ByteWriter):
        raise NotImplementedError()


@dataclasses.dataclass
class Request:
    header: RequestHeader
    body: RequestBody


@dataclasses.dataclass
class Response:
    header: RequestHeader
    body: ResponseBody

    def serialize(self, writer: buffer.ByteWriter):
        self.header.serialize(writer)
        self.body.serialize(writer)


@dataclasses.dataclass
class RequestHeaderV2(RequestHeader):
    request_api_key: int
    request_api_version: int
    correlation_id: int
    client_id: typing.Optional[str]

    @staticmethod
    def deserialize(reader: buffer.ByteReader):
        api_key = reader.read_signed_short()
        api_version = reader.read_signed_short()

        correlation_id = reader.read_signed_int()
        client_id = reader.read_string()

        reader.skip_empty_tagged_field_array()

        return RequestHeaderV2(
            api_key,
            api_version,
            correlation_id,
            client_id,
        )


@dataclasses.dataclass
class ResponseHeaderV0(ResponseHeader):
    correlation_id: int

    def serialize(self, writer: buffer.ByteWriter):
        writer.write_signed_int(self.correlation_id)


@dataclasses.dataclass
class ResponseHeaderV1(ResponseHeader):
    correlation_id: int

    def serialize(self, writer: buffer.ByteWriter):
        writer.write_signed_int(self.correlation_id)

        writer.skip_empty_tagged_field_array()
