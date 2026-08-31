package client

import (
	"bufio"
	"net"
	"os"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const ECHO_CLIENT_BUFFER_SIZE = 512

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	const mainAction = "process-bets"
	agencyArgs := []any{"agency-id", client.config.AgencyId}
	defer client.conn.Close()

	// INPUT_FILE must exist and be readable inside the mounted volume
	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, agencyArgs...)
		return err
	}
	defer inputFile.Close()

	// os.Create truncates OUTPUT_FILE if it already exists from a previous run
	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("open-output-file", logger.Fail, agencyArgs...)
		return err
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	logger.Info(mainAction, logger.InProgress, agencyArgs...)

	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		// Network error (connection reset, broken pipe, etc.) while sending this bet to the server
		if err := safe_socket.SendAll(client.conn, []byte(line)); err != nil {
			logger.Error("send-message", logger.Fail, agencyArgs...)
			return err
		}

		// Network error while waiting for the server's response to this bet
		response, err := safe_socket.RecvAll(client.conn, ECHO_CLIENT_BUFFER_SIZE)
		if err != nil {
			logger.Error("recv-response", logger.Fail, agencyArgs...)
			return err
		}

		// Disk/filesystem error persisting the response (e.g. output volume full or unmounted)
		if _, err := writer.Write(response); err != nil {
			logger.Error("write-output-file", logger.Fail, agencyArgs...)
			return err
		}
		if err := writer.WriteByte('\n'); err != nil {
			logger.Error("write-output-file", logger.Fail, agencyArgs...)
			return err
		}
	}

	// scanner.Err() is non-nil only for read errors, not for a clean EOF
	if err := scanner.Err(); err != nil {
		logger.Error("read-input-file", logger.Fail, agencyArgs...)
		return err
	}

	logger.Info(mainAction, logger.Success, agencyArgs...)
	return nil
}
