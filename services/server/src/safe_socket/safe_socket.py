import socket


def recv_all(socket: socket.socket, size):
    received = b""
    while len(received) < size:
        chunk = socket.recv(size - len(received))
        if not chunk:
            if received:
                # Peer closed the connection mid-message: we got some bytes but not all of them
                raise ConnectionError(
                    f"connection closed after receiving {len(received)} of {size} expected bytes"
                )
            # Peer closed the connection cleanly before sending anything new
            return b""
        received += chunk
    return received


def send_all(socket: socket.socket, bytes):
    sent = 0
    while sent < len(bytes):
        # send() may only accept part of the buffer per call (short write); a real closed
        # connection raises instead of returning 0, so just resume with the remaining bytes
        sent += socket.send(bytes[sent:])
