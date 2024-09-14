import socket

PORT = 9092


def main():
    print(f"listen: {PORT}")
    server = socket.create_server(("localhost", PORT), reuse_port=True)
    server.accept()


if __name__ == "__main__":
    main()
