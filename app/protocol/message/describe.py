import dataclasses
import typing
import uuid

from ... import buffer
from ..error import ErrorCode
from .base import RequestBody


def _serialize_signed_int(value: int, writer: buffer.ByteWriter):
    writer.write_signed_int(value)


@dataclasses.dataclass
class DescribeTopicPartitionsTopicRequestV0(RequestBody):

    name: str

    @staticmethod
    def deserialize(reader: buffer.ByteReader):
        name = reader.read_compact_string()

        reader.skip_empty_tagged_field_array()

        return DescribeTopicPartitionsTopicRequestV0(
            name,
        )


@dataclasses.dataclass
class DescribeTopicPartitionsCursorV0(RequestBody):

    topic_name: str
    partition_index: int

    def serialize(self, writer: buffer.ByteWriter):
        if self is None:
            return writer.write_byte(0xff)

        writer.write_compact_string(self.topic_name)
        writer.write_signed_int(self.partition_index)

        writer.skip_empty_tagged_field_array()

    @staticmethod
    def deserialize(reader: buffer.ByteReader):
        with reader.mark():
            if reader.read(1)[0] == 0xff:
                return None

        topic_name = reader.read_compact_string()
        partition_index = reader.read_signed_int()

        reader.skip_empty_tagged_field_array()

        return DescribeTopicPartitionsCursorV0(
            topic_name,
            partition_index,
        )


@dataclasses.dataclass
class DescribeTopicPartitionsRequestV0(RequestBody):

    topics: typing.List[DescribeTopicPartitionsTopicRequestV0]
    response_partition_limit: int
    next_cursor: DescribeTopicPartitionsCursorV0

    @staticmethod
    def deserialize(reader: buffer.ByteReader):
        topics = reader.read_compact_array(DescribeTopicPartitionsTopicRequestV0.deserialize)
        response_partition_limit = reader.read_signed_int()
        next_cursor = DescribeTopicPartitionsCursorV0.deserialize(reader)

        reader.skip_empty_tagged_field_array()

        return DescribeTopicPartitionsRequestV0(
            topics,
            response_partition_limit,
            next_cursor,
        )


@dataclasses.dataclass
class DescribeTopicPartitionsTopicPartitionResponseV0(RequestBody):

    error_code: ErrorCode
    partition_index: int
    leader_id: int
    leader_epoch: int
    replica_nodes: typing.List[int]
    isr_nodes: typing.List[int]
    eligible_leader_replicas: typing.List[int]
    last_known_elr: typing.List[int]
    offline_replicas: typing.List[int]

    def serialize(self, writer: buffer.ByteWriter):
        writer.write_signed_short(self.error_code.value)
        writer.write_signed_int(self.partition_index)
        writer.write_signed_int(self.leader_id)
        writer.write_signed_int(self.leader_epoch)
        writer.write_compact_array(self.replica_nodes, _serialize_signed_int)
        writer.write_compact_array(self.isr_nodes, _serialize_signed_int)
        writer.write_compact_array(self.eligible_leader_replicas, _serialize_signed_int)
        writer.write_compact_array(self.last_known_elr, _serialize_signed_int)
        writer.write_compact_array(self.offline_replicas, _serialize_signed_int)

        writer.skip_empty_tagged_field_array()


@dataclasses.dataclass
class DescribeTopicPartitionsTopicResponseV0(RequestBody):

    error_code: ErrorCode
    name: str
    topic_id: uuid.UUID
    is_internal: bool
    partitions: typing.List[DescribeTopicPartitionsTopicPartitionResponseV0]
    topic_authorized_operations: int

    def serialize(self, writer: buffer.ByteWriter):
        writer.write_signed_short(self.error_code.value)
        writer.write_compact_string(self.name)
        writer.write_uuid(self.topic_id)
        writer.write_boolean(self.is_internal)
        writer.write_compact_array(self.partitions, DescribeTopicPartitionsTopicPartitionResponseV0.serialize)
        writer.write_signed_int(self.topic_authorized_operations)

        writer.skip_empty_tagged_field_array()


@dataclasses.dataclass
class DescribeTopicPartitionsResponseV0(RequestBody):

    throttle_time_ms: int
    topics: typing.List[DescribeTopicPartitionsTopicResponseV0]
    next_cursor: DescribeTopicPartitionsCursorV0

    def serialize(self, writer: buffer.ByteWriter):
        writer.write_signed_int(self.throttle_time_ms)
        writer.write_compact_array(self.topics, DescribeTopicPartitionsTopicResponseV0.serialize)
        DescribeTopicPartitionsCursorV0.serialize(self.next_cursor, writer)

        writer.skip_empty_tagged_field_array()

    @staticmethod
    def deserialize(reader: buffer.ByteReader):
        throttle_time_ms = reader.read_signed_int()
        topics = reader.read_compact_array(DescribeTopicPartitionsTopicResponseV0.deserialize)
        next_cursor = DescribeTopicPartitionsCursorV0.deserialize(reader)

        reader.skip_empty_tagged_field_array()

        return DescribeTopicPartitionsResponseV0(
            throttle_time_ms,
            topics,
            next_cursor,
        )
