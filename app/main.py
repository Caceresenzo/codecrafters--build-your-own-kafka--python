import os
import socket
import typing
import uuid
import itertools

from . import protocol, buffer

PORT = 9092


def _read_batches():
    topics: typing.List[protocol.record.TopicRecord] = []
    partitions: typing.List[protocol.record.PartitionRecord] = []

    cluster_meta_data_path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
    os.system(f"cat {cluster_meta_data_path} | base64")
    with open(cluster_meta_data_path, "rb") as fd:
        reader = buffer.ByteReader(fd.read())

    while not reader.eof:
        batch = protocol.record.Batch.deserialize(reader)
        print(f"batch: {batch}")

        for record in batch.records:
            print(f"record: {batch}")

            if isinstance(record, protocol.record.TopicRecord):
                topics.append(record)
            if isinstance(record, protocol.record.PartitionRecord):
                partitions.append(record)
    
    return topics, partitions


def _handle_fetch(request: protocol.message.FetchRequestV16):
    topics, partitions = _read_batches()

    topic_per_uuid = {
        topic.id: topic
        for topic in topics
    }

    responses: typing.List[protocol.message.FetchResponseResponseV16] = []
    for topic_request in request.topics:
        topic = topic_per_uuid.get(topic_request.topic_id)

        if topic is None:
            responses.append(protocol.message.FetchResponseResponseV16(
                topic_request.topic_id,
                [
                    protocol.message.FetchResponseResponsePartitionV16(
                        partition_index=0,
                        error_code=protocol.ErrorCode.UNKNOWN_TOPIC_ID,
                        high_watermark=0,
                        last_stable_offset=0,
                        log_start_offset=0,
                        aborted_transactions=[],
                        preferred_read_replica=0,
                        records=bytes(),
                    )
                ]
            ))
            continue

        responses.append(protocol.message.FetchResponseResponseV16(
            topic_request.topic_id,
            [
                protocol.message.FetchResponseResponsePartitionV16(
                    partition_index=0,
                    error_code=protocol.ErrorCode.NONE,
                    high_watermark=0,
                    last_stable_offset=0,
                    log_start_offset=0,
                    aborted_transactions=[],
                    preferred_read_replica=0,
                    records=bytes(),
                )
            ]
        ))

    return protocol.message.FetchResponseV16(
        throttle_time_ms=0,
        error_code=protocol.ErrorCode.NONE,
        session_id=0,
        responses=responses,
    )


def _handle_describe_topic_partitions(request: protocol.message.DescribeTopicPartitionsRequestV0):
    topics, partitions = _read_batches()

    topic_per_name = {
        topic.name: topic
        for topic in topics
    }

    by_topic_id = lambda x: x.topic_id
    partitions_per_topic_id = {
        topic_id: sorted(grouper, key=lambda x: x.id)
        for topic_id, grouper in itertools.groupby(sorted(partitions, key=by_topic_id), by_topic_id)
    }

    topic_responses = []
    for topic_request in request.topics:
        topic = topic_per_name.get(topic_request.name)

        if topic is None:
            topic_responses.append(protocol.message.DescribeTopicPartitionsTopicResponseV0(
                error_code=protocol.ErrorCode.UNKNOWN_TOPIC,
                name=topic_request.name,
                topic_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                is_internal=False,
                partitions=[],
                topic_authorized_operations=0,
            ))
            continue

        partition_responses = []
        for partition in partitions_per_topic_id.get(topic.id, []):
            partition_responses.append(protocol.message.DescribeTopicPartitionsTopicPartitionResponseV0(
                error_code=protocol.ErrorCode.NONE,
                partition_index=partition.id,
                leader_id=partition.leader,
                leader_epoch=partition.leader_epoch,
                replica_nodes=partition.replicas,
                isr_nodes=partition.in_sync_replicas,
                eligible_leader_replicas=partition.adding_replicas,
                last_known_elr=[],
                offline_replicas=partition.removing_replicas,
            ))

        topic_responses.append(protocol.message.DescribeTopicPartitionsTopicResponseV0(
            error_code=protocol.ErrorCode.NONE,
            name=topic_request.name,
            topic_id=topic.id,
            is_internal=False,
            partitions=partition_responses,
            topic_authorized_operations=0,
        ))

    return protocol.message.DescribeTopicPartitionsResponseV0(
        throttle_time_ms=0,
        topics=topic_responses,
        next_cursor=None,
    )


def handle(
    client_id: int,
    client_socket: socket.socket,
):
    message_reader = protocol.MessageReader(client_socket)
    message_writer = protocol.MessageWriter(client_socket)

    try:
        request = message_reader.next()
        correlation_id = request.header.correlation_id

        print(request)

        if isinstance(request.body, protocol.message.ApiVersionsRequestV4):
            response = protocol.message.Response(
                protocol.message.ResponseHeaderV0(correlation_id),
                protocol.message.ApiVersionsResponseV4(
                    error_code=protocol.ErrorCode.NONE,
                    api_keys=[
                        protocol.message.ApiVersionsResponseKeyV4(
                            api_key,
                            version,
                            version
                        )
                        for api_key, version in message_reader.DESERIALIZERS.keys()
                    ],
                    throttle_time_ms=0
                )
            )
        elif isinstance(request.body, protocol.message.FetchRequestV16):
            response = protocol.message.Response(
                protocol.message.ResponseHeaderV1(correlation_id),
                _handle_fetch(request.body),
            )
        elif isinstance(request.body, protocol.message.DescribeTopicPartitionsRequestV0):
            response = protocol.message.Response(
                protocol.message.ResponseHeaderV1(correlation_id),
                _handle_describe_topic_partitions(request.body),
            )
        else:
            raise protocol.ProtocolError(
                protocol.ErrorCode.UNSUPPORTED_VERSION,
                correlation_id=correlation_id
            )

        print(response)
        message_writer.send(response)
    except protocol.ProtocolError as error:
        print(f"[{client_id}] error: {error}")

        message_writer.send_error(
            error.correlation_id,
            error.error_code
        )

    return True


def main():
    print(f"listen: {PORT}")
    server_socket = socket.create_server(("localhost", PORT), reuse_port=True)

    client_id = 0

    while True:
        client_socket, client_address = server_socket.accept()
        client_id += 1

        pid = os.fork()
        if pid:
            client_socket.close()
            print(f"[{client_id}] connected: {client_address}")

        else:
            try:
                while True:
                    handle(
                        client_id,
                        client_socket
                    )
            except EOFError as error:
                print(f"[{client_id}] eof: {error}")

            client_socket.close()
            exit(0)


if __name__ == "__main__":
    main()
