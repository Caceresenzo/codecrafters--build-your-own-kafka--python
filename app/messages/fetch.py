import dataclasses
import typing
import uuid

from .. import buffer
from .base import RequestBody, ResponseBody
from .error import ErrorCode


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
class FetchRequestV16(RequestBody):

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
    def deserialize(reader: buffer.ByteReader):
        max_wait_ms = reader.read_signed_int()
        min_bytes = reader.read_signed_int()
        max_bytes = reader.read_signed_int()
        isolation_level = reader.read_signed_char()
        session_id = reader.read_signed_int()
        session_epoch = reader.read_signed_int()
        topics = reader.read_compact_array(FetchRequestTopicV16.deserialize)
        forgotten_topics_data = reader.read_compact_array(FetchRequestForgottenTopicsDataV16.deserialize)
        rack_id = reader.read_compact_string()

        reader.skip_empty_tagged_field_array()

        return FetchRequestV16(
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


@dataclasses.dataclass
class FetchResponseResponsePartitionAbortedTransactionV16:

    producer_id: int
    first_offset: int

    def serialize(self, writer: buffer.ByteWriter):
        writer.write_signed_long(self.producer_id)
        writer.write_signed_long(self.first_offset)

        writer.skip_empty_tagged_field_array()


@dataclasses.dataclass
class FetchResponseResponsePartitionV16:

    partition_index: int
    error_code: ErrorCode
    high_watermark: int
    last_stable_offset: int
    log_start_offset: int
    aborted_transactions: typing.List[FetchResponseResponsePartitionAbortedTransactionV16]
    preferred_read_replica: int
    records: bytes

    def serialize(self, writer: buffer.ByteWriter):
        writer.write_signed_int(self.partition_index)
        writer.write_signed_short(self.error_code.value)
        writer.write_signed_long(self.high_watermark)
        writer.write_signed_long(self.last_stable_offset)
        writer.write_signed_long(self.log_start_offset)
        writer.write_compact_array(self.aborted_transactions, FetchResponseResponsePartitionAbortedTransactionV16.serialize)
        writer.write_signed_int(self.preferred_read_replica)
        writer.write_compact_records(self.records)

        writer.skip_empty_tagged_field_array()


@dataclasses.dataclass
class FetchResponseResponseV16:

    topic_id: uuid.UUID
    partitions: typing.List[FetchResponseResponsePartitionV16]

    def serialize(self, writer: buffer.ByteWriter):
        writer.write_uuid(self.topic_id)
        writer.write_compact_array(self.partitions, FetchResponseResponsePartitionV16.serialize)

        writer.skip_empty_tagged_field_array()


@dataclasses.dataclass
class FetchResponseV16(ResponseBody):

    throttle_time_ms: int
    error_code: ErrorCode
    session_id: int
    responses: typing.List[FetchResponseResponseV16]

    def serialize(self, writer: buffer.ByteWriter):
        writer.write_signed_int(self.throttle_time_ms)
        writer.write_signed_short(self.error_code.value)

        if self.error_code != ErrorCode.NONE:
            return

        writer.write_signed_int(self.session_id)
        writer.write_compact_array(self.responses, FetchResponseResponseV16.serialize)

        writer.skip_empty_tagged_field_array()
