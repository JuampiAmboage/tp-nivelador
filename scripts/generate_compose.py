import sys

SERVER_TEMPLATE = """services:
  server:
    build:
      context: ./services/server
      dockerfile: Dockerfile
    container_name: server
    environment:
      - PYTHONUNBUFFERED=1
      - SERVER_HOST=server
      - SERVER_PORT=5678
"""

CLIENT_TEMPLATE = """
  client_{agency_id}:
    build:
      context: ./services/client
      dockerfile: Dockerfile
    container_name: client_{agency_id}
    depends_on:
      - server
    environment:
      - AGENCY_ID={agency_id}
      - SERVER_HOST=server
      - SERVER_PORT=5678
"""

OUTPUT_PATH = "docker-compose.yaml"


def generate(client_amount: int) -> str:
    content = SERVER_TEMPLATE
    for agency_id in range(client_amount):
        content += CLIENT_TEMPLATE.format(agency_id=agency_id)
    return content


def main() -> int:
    if len(sys.argv) != 2 or not sys.argv[1].isdigit() or int(sys.argv[1]) < 1:
        print(f"Usage: python3 {sys.argv[0]} <client_amount>", file=sys.stderr)
        return 1

    client_amount = int(sys.argv[1])
    with open(OUTPUT_PATH, "w") as compose_file:
        compose_file.write(generate(client_amount).rstrip("\n"))

    print(f"Generated {OUTPUT_PATH} with {client_amount} client(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
