import dataclasses
import typing

from ... import buffer
from ..error import ErrorCode
from .base import RequestBody, ResponseBody


@dataclasses.dataclass
class ApiVersionsRequestV4(RequestBody):
    client_software_name: typing.Optional[str]
    client_software_version: typing.Optional[str]

    @staticmethod
    def deserialize(reader: buffer.ByteReader):
        client_software_name = reader.read_compact_string()
        client_software_version = reader.read_compact_string()

        reader.skip_empty_tagged_field_array()

        return ApiVersionsRequestV4(
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
class ApiVersionsResponseV4(ResponseBody):
    error_code: ErrorCode
    api_keys: typing.List[ApiVersionsResponseKeyV4]
    throttle_time_ms: int

    def serialize(self, writer: buffer.ByteWriter):
        writer.write_signed_short(self.error_code.value)

        if self.error_code != ErrorCode.NONE:
            return

        writer.write_compact_array(self.api_keys, ApiVersionsResponseKeyV4.serialize)
        writer.write_signed_int(self.throttle_time_ms)
        writer.skip_empty_tagged_field_array()
