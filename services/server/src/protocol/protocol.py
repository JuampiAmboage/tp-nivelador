import socket
import safe_socket

HELLO = b"H"
BET = b"B"
DONE = b"D"
WINNERS = b"W"
ACK = b"A"

_MESSAGE_TYPE_SIZE = 1
_PAYLOAD_LENGTH_SIZE = 4


def write_message(sock: socket.socket, message_type: bytes, payload: bytes) -> None:
    header = message_type + len(payload).to_bytes(_PAYLOAD_LENGTH_SIZE, "big")
    safe_socket.send_all(sock, header + payload)


def read_message(sock: socket.socket) -> tuple[bytes, bytes]:
    header = safe_socket.recv_all(
        sock, _MESSAGE_TYPE_SIZE + _PAYLOAD_LENGTH_SIZE
    )
    if not header:
        # Peer closed the connection instead of sending a new message
        raise ConnectionError("connection closed before a new message header")

    message_type = header[:_MESSAGE_TYPE_SIZE]
    payload_length = int.from_bytes(header[_MESSAGE_TYPE_SIZE:], "big")
    payload = safe_socket.recv_all(sock, payload_length)
    return message_type, payload
