"""Unit tests for Mini-Redis commands and dispatch table (no networking)."""

import pytest
from mini_redis.commands import execute_command
from mini_redis.resp import RespError, SimpleString
from mini_redis.store import Store


@pytest.fixture
def store() -> Store:
    return Store()


class TestCommands:
    """Unit tests for individual command handlers and dispatching."""

    def test_ping_no_args(self, store: Store) -> None:
        res = execute_command(store, [b"PING"])
        assert res == SimpleString("PONG")

    def test_ping_with_message(self, store: Store) -> None:
        res = execute_command(store, [b"PING", b"hello world"])
        assert res == b"hello world"

    def test_ping_wrong_args(self, store: Store) -> None:
        res = execute_command(store, [b"PING", b"msg1", b"msg2"])
        assert isinstance(res, RespError)
        assert res.message == "ERR wrong number of arguments for 'ping' command"

    def test_echo(self, store: Store) -> None:
        res = execute_command(store, [b"ECHO", b"hello"])
        assert res == b"hello"

    def test_echo_wrong_args(self, store: Store) -> None:
        res1 = execute_command(store, [b"ECHO"])
        assert isinstance(res1, RespError)
        assert res1.message == "ERR wrong number of arguments for 'echo' command"

        res2 = execute_command(store, [b"ECHO", b"a", b"b"])
        assert isinstance(res2, RespError)
        assert res2.message == "ERR wrong number of arguments for 'echo' command"

    def test_set_and_get(self, store: Store) -> None:
        # SET returns +OK
        set_res = execute_command(store, [b"SET", b"mykey", b"myvalue"])
        assert set_res == SimpleString("OK")

        # GET existing key
        get_res = execute_command(store, [b"GET", b"mykey"])
        assert get_res == b"myvalue"

        # GET missing key returns None (nil)
        assert execute_command(store, [b"GET", b"nonexistent"]) is None

    def test_set_wrong_args(self, store: Store) -> None:
        res = execute_command(store, [b"SET", b"keyonly"])
        assert isinstance(res, RespError)
        assert res.message == "ERR wrong number of arguments for 'set' command"

        res2 = execute_command(store, [b"SET", b"k", b"v", b"extra"])
        assert isinstance(res2, RespError)
        assert res2.message == "ERR wrong number of arguments for 'set' command"

    def test_get_wrong_args(self, store: Store) -> None:
        res = execute_command(store, [b"GET"])
        assert isinstance(res, RespError)
        assert res.message == "ERR wrong number of arguments for 'get' command"

        res2 = execute_command(store, [b"GET", b"k1", b"k2"])
        assert isinstance(res2, RespError)
        assert res2.message == "ERR wrong number of arguments for 'get' command"

    def test_del_single_and_multiple(self, store: Store) -> None:
        execute_command(store, [b"SET", b"k1", b"v1"])
        execute_command(store, [b"SET", b"k2", b"v2"])
        execute_command(store, [b"SET", b"k3", b"v3"])

        # DEL 1 key
        assert execute_command(store, [b"DEL", b"k1"]) == 1
        assert execute_command(store, [b"GET", b"k1"]) is None

        # DEL multiple keys (k2 exists, k3 exists, k4 does not)
        assert execute_command(store, [b"DEL", b"k2", b"k3", b"k4"]) == 2
        assert execute_command(store, [b"GET", b"k2"]) is None
        assert execute_command(store, [b"GET", b"k3"]) is None

        # DEL non-existing key
        assert execute_command(store, [b"DEL", b"nonexistent"]) == 0

    def test_del_wrong_args(self, store: Store) -> None:
        res = execute_command(store, [b"DEL"])
        assert isinstance(res, RespError)
        assert res.message == "ERR wrong number of arguments for 'del' command"

    def test_exists_single_and_multiple(self, store: Store) -> None:
        execute_command(store, [b"SET", b"k1", b"v1"])
        execute_command(store, [b"SET", b"k2", b"v2"])

        # EXISTS single
        assert execute_command(store, [b"EXISTS", b"k1"]) == 1
        assert execute_command(store, [b"EXISTS", b"missing"]) == 0

        # EXISTS multiple
        assert execute_command(store, [b"EXISTS", b"k1", b"k2", b"k3"]) == 2

    def test_exists_wrong_args(self, store: Store) -> None:
        res = execute_command(store, [b"EXISTS"])
        assert isinstance(res, RespError)
        assert res.message == "ERR wrong number of arguments for 'exists' command"

    def test_case_insensitivity(self, store: Store) -> None:
        assert execute_command(store, [b"ping"]) == SimpleString("PONG")
        assert execute_command(store, [b"Ping"]) == SimpleString("PONG")
        assert execute_command(store, [b"set", b"k", b"v"]) == SimpleString("OK")
        assert execute_command(store, [b"Get", b"k"]) == b"v"
        assert execute_command(store, [b"dEl", b"k"]) == 1

    def test_unknown_command(self, store: Store) -> None:
        res = execute_command(store, [b"UNKNOWN", b"arg"])
        assert isinstance(res, RespError)
        assert res.message == "ERR unknown command 'UNKNOWN'"

    def test_empty_command(self, store: Store) -> None:
        res = execute_command(store, [])
        assert isinstance(res, RespError)
        assert res.message == "ERR empty command"


class TestStoreClockInjection:
    """Verify that Store accepts an injectable clock for determinism."""

    def test_custom_clock(self) -> None:
        current_time = 1000.0

        def fake_clock() -> float:
            return current_time

        s = Store(clock=fake_clock)
        assert s.clock() == 1000.0

        current_time = 1050.0
        assert s.clock() == 1050.0
