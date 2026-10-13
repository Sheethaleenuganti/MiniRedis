"""Asyncio TCP Server for Mini-Redis.

Runs an asyncio TCP server binding to 127.0.0.1:6379 by default.
Features:
- Pipelining support: process all parsed messages in the buffer before draining.
- Connection resilience: closes gracefully on client disconnect or protocol errors without tracebacks.
- Active TTL expiration: background task sampling and purging expired keys every 100 ms.
- Windows-compatible: standard library asyncio, no Unix-only calls or signals.
"""

import argparse
import asyncio
import logging
from typing import Optional

from mini_redis.commands import execute_command
from mini_redis.resp import RESPParser, RespError, RespParseError, encode_resp
from mini_redis.store import Store

logger = logging.getLogger("mini_redis.server")


class Server:
    """Asyncio TCP Redis server."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        store: Optional[Store] = None,
    ) -> None:
        self.host = host
        self._configured_port = port
        self.store = store or Store()
        self._server: Optional[asyncio.Server] = None
        self._active_expiry_task: Optional[asyncio.Task] = None

    @property
    def port(self) -> int:
        """Return the active listening port (resolves ephemeral port 0 if started)."""
        if self._server and self._server.sockets:
            sock = self._server.sockets[0]
            return sock.getsockname()[1]
        return self._configured_port

    async def _active_expiry_loop(self) -> None:
        """Background coroutine that samples and purges expired keys every 100 ms."""
        try:
            while True:
                await asyncio.sleep(0.1)  # 100 ms
                self.store.active_expire_sweep(
                    sample_size=20, threshold_ratio=0.25, max_loops=10
                )
        except asyncio.CancelledError:
            # Clean cancellation on server shutdown
            pass

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Coroutine handling a single client connection."""
        parser = RESPParser()
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    # Client closed connection (EOF)
                    break

                parser.feed(data)
                responses: list[bytes] = []

                # Pipelining: process all complete commands in buffer
                while True:
                    try:
                        msg = parser.get_message()
                    except RespParseError as err:
                        # Protocol syntax or limit error: reply error and close connection
                        err_reply = encode_resp(RespError(f"ERR Protocol error: {err}"))
                        writer.write(err_reply)
                        await writer.drain()
                        return

                    if msg is None:
                        break

                    # Dispatch command
                    if isinstance(msg, list):
                        result = execute_command(self.store, msg)
                    else:
                        result = RespError(
                            "ERR Protocol error: expected array of command arguments"
                        )

                    responses.append(encode_resp(result))

                # Send all accumulated responses back to the client
                if responses:
                    writer.write(b"".join(responses))
                    await writer.drain()

        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            # Normal client disconnection on Windows / network
            pass
        except Exception as err:
            logger.debug("Client handler error: %s", err)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def start(self) -> None:
        """Start listening for incoming client connections and launch background tasks."""
        self._server = await asyncio.start_server(
            self.handle_client, host=self.host, port=self._configured_port
        )
        self._active_expiry_task = asyncio.create_task(self._active_expiry_loop())

    async def stop(self) -> None:
        """Stop listening, cancel background tasks, and close the server."""
        if self._active_expiry_task:
            self._active_expiry_task.cancel()
            try:
                await self._active_expiry_task
            except asyncio.CancelledError:
                pass
            self._active_expiry_task = None

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def serve_forever(self) -> None:
        """Run the server until cancelled."""
        if not self._server:
            await self.start()
        print(f"Mini-Redis server running on {self.host}:{self.port}")
        async with self._server:  # type: ignore[union-attr]
            await self._server.serve_forever()


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Mini-Redis Server")
    parser.add_argument(
        "--host", default="127.0.0.1", help="Host address to bind (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", type=int, default=6379, help="TCP port to bind (default: 6379)"
    )
    args = parser.parse_args()

    server = Server(host=args.host, port=args.port)
    try:
        asyncio.run(server.serve_forever())
    except KeyboardInterrupt:
        print("\nMini-Redis server stopped.")


if __name__ == "__main__":
    main()
