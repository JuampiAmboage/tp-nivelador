package client

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

// Generous retry budget: docker compose starting every container at once means the server
// may still be binding its socket well after this client's process has already started
const CONNECTION_ATTEMPTS_MAX = 15
const CONNECTION_ATTEMPS_DELAY_MS = 500

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

	logger.Info(mainAction, logger.InProgress, agencyArgs...)

	// Network error identifying this agency to the server before sending any bets
	if err := protocol.WriteMessage(client.conn, protocol.Hello, []byte(client.config.AgencyId)); err != nil {
		logger.Error("send-hello", logger.Fail, agencyArgs...)
		return err
	}

	betAmount := 0
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		// Network error (connection reset, broken pipe, etc.) while sending this bet to the server
		if err := protocol.WriteMessage(client.conn, protocol.Bet, []byte(line)); err != nil {
			logger.Error("send-bet", logger.Fail, agencyArgs...)
			return err
		}
		betAmount++
	}

	// scanner.Err() is non-nil only for read errors, not for a clean EOF
	if err := scanner.Err(); err != nil {
		logger.Error("read-input-file", logger.Fail, agencyArgs...)
		return err
	}

	// Network error signaling the server this agency has no more bets to send
	if err := protocol.WriteMessage(client.conn, protocol.Done, []byte{}); err != nil {
		logger.Error("send-done", logger.Fail, agencyArgs...)
		return err
	}

	// Network error waiting for the server to compute and send back this agency's winners
	messageType, payload, err := protocol.ReadMessage(client.conn)
	if err != nil {
		logger.Error("recv-winners", logger.Fail, agencyArgs...)
		return err
	}
	if messageType != protocol.Winners {
		logger.Error("recv-winners", logger.Fail, agencyArgs...)
		return fmt.Errorf("expected a WINNERS message, got type %q", messageType)
	}

	// Disk/filesystem error persisting the winners (e.g. output volume full or unmounted)
	if _, err := outputFile.Write(payload); err != nil {
		logger.Error("write-output-file", logger.Fail, agencyArgs...)
		return err
	}

	logger.Info(mainAction, logger.Success, append(agencyArgs, "bet-amount", betAmount)...)
	return nil
}
