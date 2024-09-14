import struct
import socket

from . import protocol

PORT = 9092


def main():
    print(f"listen: {PORT}")
    server_socket = socket.create_server(("localhost", PORT), reuse_port=True)
    client_socket, client_address = server_socket.accept()
    
    print(f"connected: {client_address}")

    message_reader = protocol.MessageReader(client_socket)
    try:
        request = message_reader.next()
        print(request)

        if isinstance(request, protocol.ApiVersionsRequestV4):
            client_socket.send(struct.pack(
                "!ii",
                4,
                request.header.correlation_id
            ))
    except protocol.ProtocolError as error:
        print(error)
        client_socket.send(struct.pack(
            "!iih",
            6,
            error.header.correlation_id,
            error.error_code.value
        ))

    client_socket.close()


if __name__ == "__main__":
    main()
