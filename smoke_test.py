"""Smoke test for Mini-Redis using the official redis-py client."""

import redis


def main() -> None:
    client = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True, protocol=2)

    print("PING:", client.ping())
    print("SET 'a' 1:", client.set("a", 1))
    print("GET 'a':", client.get("a"))
    print("EXISTS 'a':", client.exists("a"))
    print("DELETE 'a':", client.delete("a"))
    print("GET 'a':", client.get("a"))

    client.close()


if __name__ == "__main__":
    main()
