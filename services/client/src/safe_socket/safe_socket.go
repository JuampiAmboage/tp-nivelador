package safe_socket

import (
	"fmt"
	"io"
)

func SendAll(socket io.Writer, bytes []byte) error {
	written := 0
	for written < len(bytes) {
		n, err := socket.Write(bytes[written:])
		written += n
		if err != nil {
			// Write can fail partway through; whatever it did manage to write is already counted above
			return err
		}
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	read := 0
	for read < size {
		n, err := socket.Read(buff[read:])
		read += n
		if err != nil {
			if err == io.EOF {
				// Peer closed the connection before sending all the bytes this call expected
				return nil, fmt.Errorf("connection closed after receiving %d of %d expected bytes", read, size)
			}
			return nil, err
		}
	}
	return buff, nil
}
