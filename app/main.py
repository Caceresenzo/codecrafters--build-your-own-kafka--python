import struct
import socket

PORT = 9092


def main():
    print(f"listen: {PORT}")
    server_socket = socket.create_server(("localhost", PORT), reuse_port=True)
    client_socket, client_address = server_socket.accept()
    
    print(f"connected: {client_address}")
    client_socket.recv(1024)
    client_socket.send(struct.pack(
        "!ii",
        0,
        7
    ))

    client_socket.close()


if __name__ == "__main__":
    main()
