"""Unit and integration tests for TTL (Phase 3).

Uses injected fake clocks for 100% deterministic, zero-sleep testing of TTL
boundaries, lazy expiry, and active sweep logic.
"""

from typing import Any
import pytest
import pytest_asyncio
import redis.asyncio as aioredis

from mini_redis.commands import execute_command
from mini_redis.resp import RespError, SimpleString
from mini_redis.server import Server
from mini_redis.store import Store


class FakeClock:
    """Deterministic, controllable clock for testing."""

    def __init__(self, initial_ms: int = 1000) -> None:
        self.current_ms = initial_ms

    def __call__(self) -> int:
        return self.current_ms

    def advance(self, ms: int) -> None:
        self.current_ms += ms


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock(initial_ms=10000)


@pytest.fixture
def store(fake_clock: FakeClock) -> Store:
    return Store(clock=fake_clock)


class TestTTLUnit:
    """Unit tests for TTL commands and store behavior."""

    def test_set_ex_and_px(self, store: Store, fake_clock: FakeClock) -> None:
        # SET with EX seconds
        res = execute_command(store, [b"SET", b"k1", b"v1", b"EX", b"5"])
        assert res == SimpleString("OK")
        assert store.get_expiry(b"k1") == fake_clock.current_ms + 5000

        # SET with PX milliseconds
        res2 = execute_command(store, [b"SET", b"k2", b"v2", b"PX", b"2500"])
        assert res2 == SimpleString("OK")
        assert store.get_expiry(b"k2") == fake_clock.current_ms + 2500

    def test_set_reject_ex_and_px_together(self, store: Store) -> None:
        res = execute_command(
            store, [b"SET", b"k", b"v", b"EX", b"5", b"PX", b"5000"]
        )
        assert isinstance(res, RespError)
        assert res.message == "ERR syntax error"

    def test_set_reject_non_integer_expiry(self, store: Store) -> None:
        res = execute_command(store, [b"SET", b"k", b"v", b"EX", b"abc"])
        assert isinstance(res, RespError)
        assert res.message == "ERR value is not an integer or out of range"

        res2 = execute_command(store, [b"SET", b"k", b"v", b"PX", b"xyz"])
        assert isinstance(res2, RespError)
        assert res2.message == "ERR value is not an integer or out of range"

    def test_set_reject_non_positive_expiry(self, store: Store) -> None:
        res = execute_command(store, [b"SET", b"k", b"v", b"EX", b"0"])
        assert isinstance(res, RespError)
        assert res.message == "ERR invalid expire time in 'set' command"

        res2 = execute_command(store, [b"SET", b"k", b"v", b"EX", b"-10"])
        assert isinstance(res2, RespError)
        assert res2.message == "ERR invalid expire time in 'set' command"

        res3 = execute_command(store, [b"SET", b"k", b"v", b"PX", b"0"])
        assert isinstance(res3, RespError)
        assert res3.message == "ERR invalid expire time in 'set' command"

    def test_set_clears_old_ttl(self, store: Store, fake_clock: FakeClock) -> None:
        # Key with TTL
        execute_command(store, [b"SET", b"k", b"v", b"EX", b"10"])
        assert store.get_expiry(b"k") is not None
        assert execute_command(store, [b"TTL", b"k"]) == 10

        # Overwrite with plain SET
        execute_command(store, [b"SET", b"k", b"v_new"])
        assert store.get_expiry(b"k") is None
        assert execute_command(store, [b"TTL", b"k"]) == -1
        assert execute_command(store, [b"GET", b"k"]) == b"v_new"

    def test_exact_boundary_expiry(self, store: Store, fake_clock: FakeClock) -> None:
        # Initial time: 10000 ms. Expiry: 5 seconds -> 15000 ms
        execute_command(store, [b"SET", b"key", b"val", b"EX", b"5"])
        assert store.get_expiry(b"key") == 15000

        # At 14999 ms (1 ms before boundary): key is alive
        fake_clock.current_ms = 14999
        assert execute_command(store, [b"GET", b"key"]) == b"val"
        assert execute_command(store, [b"TTL", b"key"]) >= 1

        # At 15000 ms (exact boundary): key is expired
        fake_clock.current_ms = 15000
        assert execute_command(store, [b"GET", b"key"]) is None
        assert execute_command(store, [b"EXISTS", b"key"]) == 0
        assert execute_command(store, [b"TTL", b"key"]) == -2

    def test_lazy_deletion_on_get(self, store: Store, fake_clock: FakeClock) -> None:
        execute_command(store, [b"SET", b"lazy", b"value", b"PX", b"500"])
        # Directly verify internal storage
        assert b"lazy" in store._data
        assert b"lazy" in store._expires

        # Fast-forward past expiry
        fake_clock.advance(600)

        # GET triggers lazy deletion
        assert execute_command(store, [b"GET", b"lazy"]) is None
        assert b"lazy" not in store._data
        assert b"lazy" not in store._expires

    def test_expire_missing_key(self, store: Store) -> None:
        assert execute_command(store, [b"EXPIRE", b"missing", b"10"]) == 0
        assert execute_command(store, [b"PEXPIRE", b"missing", b"1000"]) == 0
        assert execute_command(store, [b"TTL", b"missing"]) == -2

    def test_ttl_and_pttl_return_codes(self, store: Store, fake_clock: FakeClock) -> None:
        # Key missing -> -2
        assert execute_command(store, [b"TTL", b"no_such_key"]) == -2
        assert execute_command(store, [b"PTTL", b"no_such_key"]) == -2

        # Key exists with no expiry -> -1
        execute_command(store, [b"SET", b"persistent", b"data"])
        assert execute_command(store, [b"TTL", b"persistent"]) == -1
        assert execute_command(store, [b"PTTL", b"persistent"]) == -1

        # Key with expiry -> returns remaining time
        execute_command(store, [b"SET", b"temp", b"data", b"EX", b"10"])
        assert execute_command(store, [b"TTL", b"temp"]) == 10
        assert execute_command(store, [b"PTTL", b"temp"]) == 10000

        # Advance clock by 3 seconds
        fake_clock.advance(3000)
        assert execute_command(store, [b"TTL", b"temp"]) == 7
        assert execute_command(store, [b"PTTL", b"temp"]) == 7000

    def test_persist_command(self, store: Store) -> None:
        execute_command(store, [b"SET", b"k", b"v", b"EX", b"60"])
        assert execute_command(store, [b"TTL", b"k"]) == 60

        # PERSIST removes expiry
        assert execute_command(store, [b"PERSIST", b"k"]) == 1
        assert execute_command(store, [b"TTL", b"k"]) == -1

        # Second PERSIST returns 0 (already persistent)
        assert execute_command(store, [b"PERSIST", b"k"]) == 0

        # PERSIST on missing key returns 0
        assert execute_command(store, [b"PERSIST", b"missing"]) == 0

    def test_pexpireat_command(self, store: Store, fake_clock: FakeClock) -> None:
        execute_command(store, [b"SET", b"k", b"v"])
        target_timestamp = fake_clock.current_ms + 12345
        assert execute_command(store, [b"PEXPIREAT", b"k", str(target_timestamp).encode("ascii")]) == 1
        assert store.get_expiry(b"k") == target_timestamp

        # PEXPIREAT with timestamp in past deletes key
        past_timestamp = fake_clock.current_ms - 1000
        assert execute_command(store, [b"PEXPIREAT", b"k", str(past_timestamp).encode("ascii")]) == 1
        assert execute_command(store, [b"GET", b"k"]) is None

    def test_active_expire_sweep(self, store: Store, fake_clock: FakeClock) -> None:
        # Create 30 keys with expiry at t = 15000
        for i in range(30):
            k = f"k_{i}".encode("ascii")
            execute_command(store, [b"SET", k, b"val", b"PX", b"5000"])

        # Also create 10 persistent keys
        for i in range(10):
            k = f"perm_{i}".encode("ascii")
            execute_command(store, [b"SET", k, b"val"])

        assert len(store._expires) == 30
        assert len(store._data) == 40

        # Clock has not reached expiry: sweep should expire 0 keys
        purged = store.active_expire_sweep(sample_size=20, threshold_ratio=0.25, max_loops=10)
        assert purged == 0
        assert len(store._expires) == 30

        # Advance clock past expiry
        fake_clock.advance(6000)

        # Call active sweep directly
        purged = store.active_expire_sweep(sample_size=20, threshold_ratio=0.25, max_loops=10)
        assert purged == 30
        assert len(store._expires) == 0
        assert len(store._data) == 10  # Only persistent keys remain


class TestServerRedisPyTTL:
    """Server-level integration test using official redis-py client."""

    @pytest_asyncio.fixture
    async def server(self):
        srv = Server(host="127.0.0.1", port=0)
        await srv.start()
        yield srv
        await srv.stop()

    @pytest.mark.asyncio
    async def test_redis_py_set_ex_and_ttl(self, server: Server) -> None:
        client = aioredis.Redis(
            host="127.0.0.1", port=server.port, decode_responses=True, protocol=2
        )
        try:
            # SET with ex=1
            assert await client.set("k", "v", ex=1) is True
            # Check TTL
            ttl = await client.ttl("k")
            assert ttl in (1, 0)
            assert await client.get("k") == "v"
        finally:
            await client.aclose()
