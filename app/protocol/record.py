import dataclasses
import typing
import uuid

from .. import buffer


class Record:

    @staticmethod
    def deserialize(reader: buffer.ByteReader):
        length = reader.read_signed_varint()
        attributes = reader.read_signed_char()
        timestamp_delta = reader.read_signed_varlong()
        offset_delta = reader.read_signed_varint()

        key_length = reader.read_unsigned_varint()
        key = reader.read(key_length - 1) if key_length else None
        # key = reader.read_compact_bytes()

        value_length = reader.read_signed_varint()
        value = reader.read(value_length)

        record_reader = buffer.ByteReader(value)
        record_frame_version = record_reader.read_signed_char()
        record_type = record_reader.read_signed_char()
        record_version = record_reader.read_signed_char()

        match record_type:
            case 2:
                record = TopicRecord.deserialize(record_reader)

            case 3:
                record = PartitionRecord.deserialize(record_reader, None)

            case 12:
                record = FeatureLevelRecord.deserialize(record_reader)

            case _:
                raise ValueError(f"unknown record type: {record_type}")
        
        assert record_reader.eof

        headers = reader.read_compact_dict(
            buffer.ByteReader.read_compact_string,
            buffer.ByteReader.read_compact_bytes,
        )

        print(value)
        return record


@dataclasses.dataclass
class TopicRecord(Record):
    name: str
    id: uuid.UUID

    @staticmethod
    def deserialize(reader: buffer.ByteReader):
        name = reader.read_compact_string()
        id = reader.read_uuid()

        reader.skip_empty_tagged_field_array()

        return TopicRecord(
            name,
            id,
        )


@dataclasses.dataclass
class PartitionRecord(Record):
    id: int
    topic_id: uuid.UUID
    replicas: typing.List[int]
    in_sync_replicas: typing.List[int]
    removing_replicas: typing.List[int]
    adding_replicas: typing.List[int]
    leader: int
    leader_epoch: int
    partition_epoch: int
    directories: typing.List[uuid.UUID]

    value: bytes

    @staticmethod
    def deserialize(reader: buffer.ByteReader, value: bytes):
        id = reader.read_signed_int()
        topic_id = reader.read_uuid()
        replicas = reader.read_compact_array(buffer.ByteReader.read_signed_int)
        in_sync_replicas = reader.read_compact_array(buffer.ByteReader.read_signed_int)
        removing_replicas = reader.read_compact_array(buffer.ByteReader.read_signed_int)
        adding_replicas = reader.read_compact_array(buffer.ByteReader.read_signed_int)
        leader = reader.read_signed_int()
        leader_epoch = reader.read_signed_int()
        partition_epoch = reader.read_signed_int()
        directories = reader.read_compact_array(buffer.ByteReader.read_uuid)

        reader.skip_empty_tagged_field_array()

        return PartitionRecord(
            id,
            topic_id,
            replicas,
            in_sync_replicas,
            removing_replicas,
            adding_replicas,
            leader,
            leader_epoch,
            partition_epoch,
            directories,

            value,
        )


@dataclasses.dataclass
class FeatureLevelRecord(Record):
    name: str
    feature_level: int

    @staticmethod
    def deserialize(reader: buffer.ByteReader):
        name = reader.read_compact_string()
        feature_level = reader.read_signed_short()

        reader.skip_empty_tagged_field_array()

        return FeatureLevelRecord(
            name,
            feature_level,
        )


@dataclasses.dataclass
class Batch:
    base_offset: int
    partition_leader_epoch: int
    magic: int
    crc: int
    attributes: int
    last_offset_delta: int
    base_timestamp: int
    max_timestamp: int
    producer_id: int
    producer_epoch: int
    base_sequence: int
    records: typing.List[Record]

    @staticmethod
    def deserialize(reader: buffer.ByteReader):
        base_offset = reader.read_signed_long()
        reader = buffer.ByteReader(reader.read_bytes())
        partition_leader_epoch = reader.read_signed_int()
        magic = reader.read_signed_char()
        crc = reader.read_unsigned_int()
        attributes = reader.read_signed_short()
        last_offset_delta = reader.read_signed_int()
        base_timestamp = reader.read_signed_long()
        max_timestamp = reader.read_signed_long()
        producer_id = reader.read_signed_long()
        producer_epoch = reader.read_signed_short()
        base_sequence = reader.read_signed_int()

        records = reader.read_array(Record.deserialize)

        return Batch(
            base_offset,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        )
