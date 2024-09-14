import socket
import sys
import typing

target = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
target.connect(("localhost", 9092))


def encode(lines: typing.List[str]):
    return bytes((
        int(x, 16)
        for line in lines
        for x in line.split(" ")
    ))


# target.send(encode([
#     "00 00 00 23",  # message size
#     "00 12",  # request api key
#     "00 04",  # request api version
#     "00 00 00 07",  # correlation id

#     # client id
#     "00 09",  # length
#     "6b 61 66 6b 61 2d 63 6c 69",  # utf-8 data

#     "00",  # "empty tagged field array" -> varint 0

#     # client software name
#     "0a",  # length -> varint n+1 -> 9
#     "6b 61 66 6b 61 2d 63 6c 69",  # utf-8 data

#     # client software version
#     "04",  # length -> varint n+1 -> 3
#     "30 2e 31",  # utf-8 data

#     "00",  # "empty tagged field array" -> varint 0
# ]))

target.send(encode([
    "00 00 00 30",  # message length

    "00 01 00 10",  # api key + version

    "57 63 74 35",  # correlation_id

    # client_id
    "00 0c",
    "6b 61 66 6b 61 2d 74 65 73 74 65 72",

    "00",

    "00 00 01 f4",  # max_wait_ms
    "00 00 00 01",  # min_bytes
    "03 20 00 00",  # max_bytes
    "00",  # isolation_level

    "00 00 00 00",  # session_id
    "00 00 00 00",  # session_epoch

    "01",  # topics -> empty list
    "01",  # forgotten_topics_data -> empty list

    "01",  # rack_id -> empty string

    "00",
]))

# target.close()

offset = 0
while True:
    byte = target.recv(1)

    if not len(byte):
        break

    print(hex(byte[0])[2:].zfill(2), end=" ")
    sys.stdout.flush()

    offset += 1
    if offset % 16 == 0:
        offset = 0
        print()
