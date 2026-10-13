"""End-to-end integration tests for Mini-Redis (Phase 5).

Spawns the real server as a subprocess with --appendfsync always, executes a series
of write commands with TTL, plain SET, DEL, and PERSIST via the official redis-py client,
kills the server mid-run, restarts on the same AOF, and verifies state and TTL restoration.
"""

import asyncio
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
import redis.asyncio as aioredis


def find_free_port() -> int:
    """Allocate and return an available local TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def wait_for_server_ready(port: int, timeout: float = 5.0) -> aioredis.Redis:
    """
    Poll until server accepts connections and responds to PING.
    Uses dynamic polling with exponential backoff / micro-intervals, no fixed sleep.
    """
    client = aioredis.Redis(
        host="127.0.0.1", port=port, decode_responses=True, protocol=2
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if await client.ping():
                return client
        except Exception:
            await asyncio.sleep(0.02)

    await client.aclose()
    raise TimeoutError(f"Server on port {port} failed to become ready within {timeout}s")


class TestFullIntegration:
    """Full integration test with subprocess crash and recovery."""

    @pytest.mark.asyncio
    async def test_subprocess_lifecycle_and_aof_recovery(self, tmp_path: Path) -> None:
        port = find_free_port()
        aof_file = str(tmp_path / "integration.aof")
        project_root = str(Path(__file__).parent.parent)

        env = os.environ.copy()
        env["PYTHONPATH"] = project_root

        server_cmd = [
            sys.executable,
            "-m",
            "mini_redis.server",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--aof-path",
            aof_file,
            "--appendfsync",
            "always",
        ]

        # 1. Start server process 1
        proc1 = subprocess.Popen(server_cmd, env=env)
        try:
            client1 = await wait_for_server_ready(port)

            # SET with EX
            assert await client1.set("exp_key", "exp_val", ex=60) is True

            # Plain SET
            assert await client1.set("plain_key", "plain_val") is True

            # SET then DEL
            assert await client1.set("to_del", "temporary_val") is True
            assert await client1.delete("to_del") == 1

            # SET with TTL then PERSIST
            assert await client1.set("persist_key", "persisted_val", ex=30) is True
            assert await client1.persist("persist_key") == 1

            await client1.aclose()

        finally:
            # 2. Simulate crash by killing process 1
            proc1.kill()
            proc1.wait()

        # 3. Restart server on the same AOF file
        proc2 = subprocess.Popen(server_cmd, env=env)
        try:
            client2 = await wait_for_server_ready(port)

            # Assert plain SET value and TTL
            assert await client2.get("plain_key") == "plain_val"
            assert await client2.ttl("plain_key") == -1

            # Assert DEL key is missing
            assert await client2.get("to_del") is None
            assert await client2.ttl("to_del") == -2

            # Assert PERSIST key is present without expiry
            assert await client2.get("persist_key") == "persisted_val"
            assert await client2.ttl("persist_key") == -1

            # Assert SET with EX is present with remaining TTL
            assert await client2.get("exp_key") == "exp_val"
            ttl = await client2.ttl("exp_key")
            assert 0 < ttl <= 60

            await client2.aclose()

        finally:
            proc2.kill()
            proc2.wait()
