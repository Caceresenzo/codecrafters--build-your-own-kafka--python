import os
import socket
import typing

from . import protocol

PORT = 9092


def _handle_fetch(request: protocol.FetchRequestV16):
    responses: typing.List[protocol.FetchResponseResponseV16] = []

    for topic in request.topics:
        responses.append(protocol.FetchResponseResponseV16(
            topic.topic_id,
            [
                protocol.FetchResponseResponsePartitionV16(
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

    return protocol.FetchResponseV16(
        throttle_time_ms=0,
        error_code=protocol.ErrorCode.NONE,
        session_id=0,
        responses=responses,
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

        if isinstance(request.body, protocol.ApiVersionsRequestV4):
            response = protocol.Response(
                protocol.ResponseHeaderV0(correlation_id),
                protocol.ApiVersionsResponseV4(
                    error_code=protocol.ErrorCode.NONE,
                    api_keys=[
                        protocol.ApiVersionsResponseKeyV4(
                            api_key,
                            version,
                            version
                        )
                        for api_key, version in message_reader.DESERIALIZERS.keys()
                    ],
                    throttle_time_ms=0
                )
            )
        elif isinstance(request.body, protocol.FetchRequestV16):
            response = protocol.Response(
                protocol.ResponseHeaderV1(correlation_id),
                _handle_fetch(request.body),
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
