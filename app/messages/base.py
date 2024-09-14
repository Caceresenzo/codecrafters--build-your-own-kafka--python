import dataclasses
import typing

from .. import buffer


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
