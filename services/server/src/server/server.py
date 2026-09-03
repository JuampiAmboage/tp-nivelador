import socket
import signal
import threading
import time

import logger
import protocol
from lottery import Bet, Lottery

_BETS_STORAGE_PATH = "/tmp/bets.csv"
# Upper bound on how long shutdown() waits for in-flight client threads to wind down,
# so the close time stays known and bounded regardless of how many clients are connected
_SHUTDOWN_JOIN_TIMEOUT_SECONDS = 4


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

        # Graceful shutdown state
        self._shutting_down = threading.Event()
        self._server_socket = None
        self._client_sockets = []
        self._client_sockets_lock = threading.Lock()
        self._client_threads = []

    def shutdown(self, *_args) -> None:
        # Runs as the SIGTERM handler, on the main thread. Unblocks accept() by closing the
        # listening socket, unblocks quorum-waiting threads, and unblocks any thread that's
        # mid-read on its client socket - each of those calls then fails and the corresponding
        # thread winds down on its own.
        action = "sigterm"
        logger.info(action, logger.LogResult.in_progress)
        self._shutting_down.set()

        if self._server_socket is not None:
            self._server_socket.close()

        with self._quorum_condition:
            self._quorum_condition.notify_all()

        with self._client_sockets_lock:
            client_sockets = list(self._client_sockets)
        for client_socket in client_sockets:
            client_socket.close()

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
        # Blocks until AGENCY_QUORUM_MIN distinct agencies have all finished sending bets,
        # or until shutdown() fires. If quorum is never reached and no shutdown happens
        # either, this waits forever by design.
        with self._quorum_condition:
            self._finished_agencies.add(agency_id)
            self._quorum_condition.notify_all()
            self._quorum_condition.wait_for(
                lambda: len(self._finished_agencies) >= self.agency_quorum_min
                or self._shutting_down.is_set()
            )

    def _handle_client(self, client_socket):
        action = "handle-client"
        agency_id = None
        bet_amount = 0
        with self._client_sockets_lock:
            self._client_sockets.append(client_socket)
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
            if self._shutting_down.is_set():
                # Woken up by shutdown(), not by reaching quorum - the system is closing,
                # there's no point computing/sending a response
                logger.info(
                    action, logger.LogResult.success, "agency-id", agency_id, "bet-amount", bet_amount
                )
                return

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
        finally:
            with self._client_sockets_lock:
                self._client_sockets.remove(client_socket)
            client_socket.close()

    def run(self):
        action = "accept-connection"
        signal.signal(signal.SIGTERM, self.shutdown)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            self._server_socket = server_socket
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    if self._shutting_down.is_set():
                        # shutdown() closed the listening socket on purpose to unblock accept()
                        break
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                thread = threading.Thread(
                    target=self._handle_client, args=(client_socket,), daemon=True
                )
                self._client_threads.append(thread)
                thread.start()

        if self._shutting_down.is_set():
            # Bounds the total wait across every thread, not per thread, so the shutdown
            # time stays known regardless of how many clients are connected
            deadline = time.monotonic() + _SHUTDOWN_JOIN_TIMEOUT_SECONDS
            for thread in self._client_threads:
                thread.join(timeout=max(0, deadline - time.monotonic()))
