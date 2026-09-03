import socket
import threading

import logger
import protocol
from lottery import Bet, Lottery

_BETS_STORAGE_PATH = "/tmp/bets.csv"


class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agency_quorum_min = agency_quorum_min
        self.lottery = Lottery(storage_path=_BETS_STORAGE_PATH)
        # load_bets() requires the file to already exist, even with zero bets stored so far
        open(_BETS_STORAGE_PATH, "a").close()

        # Guards concurrent access to bets.csv, now that multiple client threads can
        # store_bets/load_bets at the same time
        self._storage_lock = threading.Lock()
        # Coordinates the quorum wait: agencies block here until enough of them are done
        self._quorum_condition = threading.Condition()
        self._finished_agencies = set()

    def _parse_bet(self, agency_id: int, line: str) -> Bet:
        [first_name, last_name, document, birthdate, number] = line.split(",")
        return Bet(agency_id, first_name, last_name, int(document), birthdate, int(number))

    def _parse_bet_batch(self, agency_id: int, payload: bytes) -> list[Bet]:
        return [
            self._parse_bet(agency_id, line) for line in payload.decode().split("\n")
        ]

    def _compute_winners(self, agency_id: int) -> bytes:
        winners = [
            bet
            for bet in self.lottery.load_bets()
            if bet.agency_id == agency_id and self.lottery.has_won(bet)
        ]
        rows = [
            f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"
            for bet in winners
        ]
        return "\n".join(rows).encode()

    def _await_quorum(self, agency_id: int) -> None:
        # Blocks until AGENCY_QUORUM_MIN distinct agencies have all finished sending bets.
        # If that count is never reached (e.g. fewer agencies connect than the quorum needs),
        # this waits forever by design - the draw only happens once enough agencies are in.
        with self._quorum_condition:
            self._finished_agencies.add(agency_id)
            self._quorum_condition.notify_all()
            self._quorum_condition.wait_for(
                lambda: len(self._finished_agencies) >= self.agency_quorum_min
            )

    def _handle_client(self, client_socket):
        action = "handle-client"
        agency_id = None
        bet_amount = 0
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                message_type, payload = protocol.read_message(client_socket)

                if message_type == protocol.HELLO:
                    agency_id = int(payload.decode())
                elif message_type == protocol.BET:
                    # Parsing happens before store_bets, so a malformed bet fails the whole
                    # batch atomically instead of partially storing it
                    bets = self._parse_bet_batch(agency_id, payload)
                    with self._storage_lock:
                        self.lottery.store_bets(bets)
                    bet_amount += len(bets)
                    protocol.write_message(client_socket, protocol.ACK, b"")
                elif message_type == protocol.DONE:
                    break
                else:
                    # Client and server versions disagree on the protocol, or the stream got corrupted
                    raise ValueError(f"unexpected message type {message_type!r}")

            self._await_quorum(agency_id)

            with self._storage_lock:
                winners_payload = self._compute_winners(agency_id)
            protocol.write_message(client_socket, protocol.WINNERS, winners_payload)

            logger.info(
                action,
                logger.LogResult.success,
                "agency-id",
                agency_id,
                "bet-amount",
                bet_amount,
            )
        except Exception as e:
            # Covers network errors from protocol.read_message/write_message and malformed bet lines;
            # an uncaught exception here only kills this client's own thread, not the whole server
            logger.error(
                action,
                logger.LogResult.fail,
                "agency-id",
                agency_id,
                "bet-amount",
                bet_amount,
            )
            raise e

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                threading.Thread(
                    target=self._handle_client, args=(client_socket,), daemon=True
                ).start()
