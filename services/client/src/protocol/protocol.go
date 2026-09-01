package protocol

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

type MessageType byte

const (
	Hello   MessageType = 'H'
	Bet     MessageType = 'B'
	Done    MessageType = 'D'
	Winners MessageType = 'W'
)

const messageTypeSize = 1
const payloadLengthSize = 4
const headerSize = messageTypeSize + payloadLengthSize

func WriteMessage(conn io.Writer, messageType MessageType, payload []byte) error {
	header := make([]byte, headerSize)
	header[0] = byte(messageType)
	binary.BigEndian.PutUint32(header[messageTypeSize:], uint32(len(payload)))

	if err := safe_socket.SendAll(conn, append(header, payload...)); err != nil {
		return fmt.Errorf("sending message: %w", err)
	}
	return nil
}

func ReadMessage(conn io.Reader) (MessageType, []byte, error) {
	header, err := safe_socket.RecvAll(conn, headerSize)
	if err != nil {
		return 0, nil, fmt.Errorf("reading message header: %w", err)
	}

	messageType := MessageType(header[0])
	payloadLength := binary.BigEndian.Uint32(header[messageTypeSize:])

	payload, err := safe_socket.RecvAll(conn, int(payloadLength))
	if err != nil {
		return 0, nil, fmt.Errorf("reading message payload: %w", err)
	}

	return messageType, payload, nil
}
