import socket

import logger
import protocol
from lottery import Bet, Lottery

_BETS_STORAGE_PATH = "/tmp/bets.csv"


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery(storage_path=_BETS_STORAGE_PATH)
        # load_bets() requires the file to already exist, even with zero bets stored so far
        open(_BETS_STORAGE_PATH, "a").close()

    def _parse_bet(self, agency_id: int, payload: bytes) -> Bet:
        [first_name, last_name, document, birthdate, number] = payload.decode().split(",")
        return Bet(agency_id, first_name, last_name, int(document), birthdate, int(number))

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
                    bet = self._parse_bet(agency_id, payload)
                    self.lottery.store_bets([bet])
                    bet_amount += 1
                elif message_type == protocol.DONE:
                    break
                else:
                    # Client and server versions disagree on the protocol, or the stream got corrupted
                    raise ValueError(f"unexpected message type {message_type!r}")

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
            # Covers network errors from protocol.read_message/write_message and malformed bet lines
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

                self._handle_client(client_socket)
