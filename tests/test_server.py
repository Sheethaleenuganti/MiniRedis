"""Integration tests for Mini-Redis asyncio TCP server."""

import asyncio
import pytest
import pytest_asyncio
import redis.asyncio as aioredis

from mini_redis.server import Server


@pytest_asyncio.fixture
async def server():
    """Start Mini-Redis server on an ephemeral port (port=0)."""
    srv = Server(host="127.0.0.1", port=0)
    await srv.start()
    yield srv
    await srv.stop()


class TestServerRawSocket:
    """Tests using raw asyncio TCP connections."""

    @pytest.mark.asyncio
    async def test_ping(self, server: Server) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        try:
            writer.write(b"*1\r\n$4\r\nPING\r\n")
            await writer.drain()

            resp = await reader.read(4096)
            assert resp == b"+PONG\r\n"
        finally:
            writer.close()
            await writer.wait_closed()

    @pytest.mark.asyncio
    async def test_set_get_roundtrip(self, server: Server) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        try:
            # SET foo bar
            writer.write(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            await writer.drain()
            resp_set = await reader.read(4096)
            assert resp_set == b"+OK\r\n"

            # GET foo
            writer.write(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            await writer.drain()
            resp_get = await reader.read(4096)
            assert resp_get == b"$3\r\nbar\r\n"

            # GET nonexistent
            writer.write(b"*2\r\n$3\r\nGET\r\n$3\r\nbaz\r\n")
            await writer.drain()
            resp_nil = await reader.read(4096)
            assert resp_nil == b"$-1\r\n"
        finally:
            writer.close()
            await writer.wait_closed()

    @pytest.mark.asyncio
    async def test_del_exists_counts(self, server: Server) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        try:
            # SET k1 v1, SET k2 v2
            writer.write(
                b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n"
                b"*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n"
            )
            await writer.drain()
            await reader.read(4096)  # consume both +OK\r\n+OK\r\n

            # EXISTS k1 k2 k3 -> returns 2
            writer.write(b"*4\r\n$6\r\nEXISTS\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n")
            await writer.drain()
            resp_exists = await reader.read(4096)
            assert resp_exists == b":2\r\n"

            # DEL k1 k2 -> returns 2
            writer.write(b"*3\r\n$3\r\nDEL\r\n$2\r\nk1\r\n$2\r\nk2\r\n")
            await writer.drain()
            resp_del = await reader.read(4096)
            assert resp_del == b":2\r\n"

            # EXISTS k1 -> returns 0
            writer.write(b"*2\r\n$6\r\nEXISTS\r\n$2\r\nk1\r\n")
            await writer.drain()
            resp_exists2 = await reader.read(4096)
            assert resp_exists2 == b":0\r\n"
        finally:
            writer.close()
            await writer.wait_closed()

    @pytest.mark.asyncio
    async def test_pipelining(self, server: Server) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        try:
            # 3 commands in a single TCP write
            pipelined = (
                b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
                b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"
                b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n"
            )
            writer.write(pipelined)
            await writer.drain()

            resp = await reader.read(4096)
            assert resp == b"+OK\r\n+OK\r\n$1\r\n1\r\n"
        finally:
            writer.close()
            await writer.wait_closed()

    @pytest.mark.asyncio
    async def test_command_split_across_two_writes(self, server: Server) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        try:
            # Part 1: incomplete command
            writer.write(b"*3\r\n$3\r\nSET\r\n$5\r\nsplit\r\n$5\r\nva")
            await writer.drain()
            await asyncio.sleep(0.02)  # Pause to ensure packet arrives split

            # Part 2: completion of command
            writer.write(b"lue\r\n")
            await writer.drain()

            resp_set = await reader.read(4096)
            assert resp_set == b"+OK\r\n"

            # Verify key was stored
            writer.write(b"*2\r\n$3\r\nGET\r\n$5\r\nsplit\r\n")
            await writer.drain()
            resp_get = await reader.read(4096)
            assert resp_get == b"$5\r\nvalue\r\n"
        finally:
            writer.close()
            await writer.wait_closed()

    @pytest.mark.asyncio
    async def test_unknown_command(self, server: Server) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        try:
            writer.write(b"*1\r\n$7\r\nUNKNOWN\r\n")
            await writer.drain()

            resp = await reader.read(4096)
            assert resp == b"-ERR unknown command 'UNKNOWN'\r\n"
        finally:
            writer.close()
            await writer.wait_closed()

    @pytest.mark.asyncio
    async def test_two_clients_at_once(self, server: Server) -> None:
        r1, w1 = await asyncio.open_connection("127.0.0.1", server.port)
        r2, w2 = await asyncio.open_connection("127.0.0.1", server.port)
        try:
            # Client 1 sets c1_key
            w1.write(b"*3\r\n$3\r\nSET\r\n$6\r\nc1_key\r\n$8\r\nc1_value\r\n")
            await w1.drain()
            assert await r1.read(4096) == b"+OK\r\n"

            # Client 2 sets c2_key
            w2.write(b"*3\r\n$3\r\nSET\r\n$6\r\nc2_key\r\n$8\r\nc2_value\r\n")
            await w2.drain()
            assert await r2.read(4096) == b"+OK\r\n"

            # Client 1 reads c2_key set by client 2
            w1.write(b"*2\r\n$3\r\nGET\r\n$6\r\nc2_key\r\n")
            await w1.drain()
            assert await r1.read(4096) == b"$8\r\nc2_value\r\n"

            # Client 2 reads c1_key set by client 1
            w2.write(b"*2\r\n$3\r\nGET\r\n$6\r\nc1_key\r\n")
            await w2.drain()
            assert await r2.read(4096) == b"$8\r\nc1_value\r\n"
        finally:
            w1.close()
            w2.close()
            await w1.wait_closed()
            await w2.wait_closed()

    @pytest.mark.asyncio
    async def test_protocol_error_disconnects(self, server: Server) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        try:
            # Send corrupted integer
            writer.write(b":invalid_int\r\n")
            await writer.drain()

            err_resp = await reader.read(4096)
            assert err_resp.startswith(b"-ERR Protocol error:")

            # Connection should be closed by server now
            eof = await reader.read(4096)
            assert eof == b""
        finally:
            writer.close()
            await writer.wait_closed()


class TestServerRedisClientCompatibility:
    """Prove compatibility with the official redis client library."""

    @pytest.mark.asyncio
    async def test_redis_py_compatibility(self, server: Server) -> None:
        client = aioredis.Redis(
            host="127.0.0.1", port=server.port, decode_responses=True, protocol=2
        )
        try:
            # PING
            assert await client.ping() is True

            # ECHO
            assert await client.echo("hello mini-redis") == "hello mini-redis"

            # SET & GET
            assert await client.set("session:123", "active") is True
            assert await client.get("session:123") == "active"
            assert await client.get("session:nonexistent") is None

            # EXISTS
            assert await client.exists("session:123", "missing") == 1

            # DEL
            assert await client.delete("session:123") == 1
            assert await client.get("session:123") is None
        finally:
            await client.aclose()
