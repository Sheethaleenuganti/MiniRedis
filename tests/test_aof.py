"""Tests for AOF persistence (Phase 4).

Covers write logging, canonical PXAT form, startup replay, truncated tail recovery,
middle corruption detection, fsync policies, and subprocess crash recovery.
"""

import asyncio
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable

import pytest
import pytest_asyncio
import redis.asyncio as aioredis

from mini_redis.aof import Aof, AofCorruptedError, to_aof_record
from mini_redis.commands import execute_command
from mini_redis.resp import encode_resp
from mini_redis.store import Store


class FakeClock:
    """Controllable clock for deterministic TTL/AOF testing."""

    def __init__(self, initial_ms: int = 10000) -> None:
        self.current_ms = initial_ms

    def __call__(self) -> int:
        return self.current_ms

    def advance(self, ms: int) -> None:
        self.current_ms += ms


def find_free_port() -> int:
    """Find a free local TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestAofUnit:
    """Unit tests for Aof operations, canonical formatting, and replay."""

    def test_write_and_replay_identical_state(self, tmp_path: Path) -> None:
        clock = FakeClock(initial_ms=10000)
        aof_path = str(tmp_path / "test.aof")
        aof = Aof(filepath=aof_path, fsync="no")
        store = Store(clock=clock)

        commands_to_run = [
            [b"SET", b"name", b"alice"],
            [b"SET", b"session", b"12345", b"EX", b"10"],  # Expiry at 20000
            [b"SET", b"counter", b"1"],
            [b"DEL", b"counter"],
            [b"EXPIRE", b"name", b"50"],  # Expiry at 60000
        ]

        for cmd in commands_to_run:
            reply = execute_command(store, cmd)
            rec = to_aof_record(store, cmd, reply)
            if rec is not None:
                aof.append(rec)

        aof.close()

        # Replay into fresh Store
        store2 = Store(clock=clock)
        aof2 = Aof(filepath=aof_path, fsync="no")
        aof2.replay(store2)
        aof2.close()

        # Verify state in store2 matches store1 identically
        assert store2.get(b"name") == b"alice"
        assert store2.get_expiry(b"name") == 60000
        assert store2.get(b"session") == b"12345"
        assert store2.get_expiry(b"session") == 20000
        assert store2.get(b"counter") is None

    def test_set_ex_logged_as_pxat_and_expires(self, tmp_path: Path) -> None:
        clock = FakeClock(initial_ms=10000)
        aof_path = str(tmp_path / "test_pxat.aof")
        aof = Aof(filepath=aof_path, fsync="no")
        store = Store(clock=clock)

        cmd = [b"SET", b"temp_key", b"temp_val", b"EX", b"10"]
        reply = execute_command(store, cmd)
        rec = to_aof_record(store, cmd, reply)
        assert rec is not None
        assert rec == [b"SET", b"temp_key", b"temp_val", b"PXAT", b"20000"]
        aof.append(rec)
        aof.close()

        # Check raw file contains PXAT and 20000
        with open(aof_path, "rb") as f:
            content = f.read()
        assert b"PXAT" in content
        assert b"20000" in content
        assert b"EX" not in content

        # Replay after clock has advanced past expiry (t = 25000)
        clock_future = FakeClock(initial_ms=25000)
        store_future = Store(clock=clock_future)
        aof_replay = Aof(filepath=aof_path, fsync="no")
        aof_replay.replay(store_future)
        aof_replay.close()

        # Key is absent
        assert store_future.get(b"temp_key") is None

    def test_failed_or_noop_commands_not_logged(self, tmp_path: Path) -> None:
        store = Store()
        execute_command(store, [b"SET", b"existing", b"val"])

        noop_commands = [
            [b"DEL", b"missing_key"],               # reply 0
            [b"EXPIRE", b"missing_key", b"10"],      # reply 0
            [b"PERSIST", b"missing_key"],            # reply 0
            [b"PERSIST", b"existing"],               # reply 0 (no ttl)
            [b"GET", b"existing"],                   # read command
            [b"PING"],                               # read/server command
            [b"SET", b"bad", b"val", b"EX", b"-1"],  # RespError
        ]

        for cmd in noop_commands:
            reply = execute_command(store, cmd)
            rec = to_aof_record(store, cmd, reply)
            assert rec is None, f"Expected None for no-op command {cmd}"

    def test_replay_does_not_grow_file(self, tmp_path: Path) -> None:
        aof_path = str(tmp_path / "nogrow.aof")
        aof = Aof(filepath=aof_path, fsync="no")
        aof.append([b"SET", b"k1", b"v1"])
        aof.append([b"SET", b"k2", b"v2"])
        aof.close()

        size_before = os.path.getsize(aof_path)

        store = Store()
        aof_replay = Aof(filepath=aof_path, fsync="no")
        aof_replay.replay(store)
        aof_replay.close()

        size_after = os.path.getsize(aof_path)
        assert size_before == size_after
        assert store.get(b"k1") == b"v1"
        assert store.get(b"k2") == b"v2"

    def test_truncated_tail_recovery(self, tmp_path: Path) -> None:
        aof_path = str(tmp_path / "truncated.aof")
        aof = Aof(filepath=aof_path, fsync="no")
        aof.append([b"SET", b"k1", b"v1"])
        aof.append([b"SET", b"k2", b"v2"])
        aof.close()

        size_good = os.path.getsize(aof_path)

        # Append incomplete truncated record (e.g. crash mid-write)
        with open(aof_path, "ab") as f:
            f.write(b"*3\r\n$3\r\nSET\r\n$2\r\nk3\r\n$")

        assert os.path.getsize(aof_path) > size_good

        store = Store()
        aof_recover = Aof(filepath=aof_path, fsync="no")
        aof_recover.replay(store)
        aof_recover.close()

        # File was truncated to size_good and complete commands were recovered
        assert os.path.getsize(aof_path) == size_good
        assert store.get(b"k1") == b"v1"
        assert store.get(b"k2") == b"v2"
        assert store.get(b"k3") is None

    def test_corrupted_middle_record_raises_error(self, tmp_path: Path) -> None:
        aof_path = str(tmp_path / "corrupted.aof")
        with open(aof_path, "wb") as f:
            f.write(encode_resp([b"SET", b"k1", b"v1"]))
            # Corrupted syntax in the middle
            f.write(b":notanumber\r\n")
            f.write(encode_resp([b"SET", b"k2", b"v2"]))

        store = Store()
        aof = Aof(filepath=aof_path, fsync="no")
        with pytest.raises(AofCorruptedError) as exc_info:
            aof.replay(store)
        aof.close()

        assert "corrupted" in str(exc_info.value).lower()

    def test_always_policy_calls_os_fsync(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fsync_calls: list[int] = []
        original_fsync = os.fsync

        def mock_fsync(fd: int) -> None:
            fsync_calls.append(fd)
            original_fsync(fd)

        monkeypatch.setattr(os, "fsync", mock_fsync)

        aof_path = str(tmp_path / "always.aof")
        aof = Aof(filepath=aof_path, fsync="always")
        aof.append([b"SET", b"foo", b"bar"])
        aof.close()

        # os.fsync was invoked on write
        assert len(fsync_calls) >= 1


class TestAofCrashRecovery:
    """Subprocess crash test: kill server mid-run and verify AOF persistence."""

    @pytest.mark.asyncio
    async def test_subprocess_crash_recovery(self, tmp_path: Path) -> None:
        port = find_free_port()
        aof_file = str(tmp_path / "crash_recovery.aof")
        project_root = str(Path(__file__).parent.parent)

        env = os.environ.copy()
        env["PYTHONPATH"] = project_root

        # 1. Start server subprocess with --appendfsync always
        proc1 = subprocess.Popen(
            [
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
            ],
            env=env,
        )

        client1 = aioredis.Redis(
            host="127.0.0.1", port=port, decode_responses=True, protocol=2
        )

        try:
            # Poll until server is ready (timeout 5s)
            start_time = time.monotonic()
            connected = False
            while time.monotonic() - start_time < 5.0:
                try:
                    await client1.ping()
                    connected = True
                    break
                except Exception:
                    await asyncio.sleep(0.05)

            assert connected, "Server failed to accept connections within 5 seconds"

            # Write key via redis-py
            assert await client1.set("persisted_key", "persisted_val") is True

        finally:
            await client1.aclose()
            # 2. Simulate crash by killing the process immediately
            proc1.kill()
            proc1.wait()

        # 3. Restart server on the same port and AOF file
        proc2 = subprocess.Popen(
            [
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
            ],
            env=env,
        )

        client2 = aioredis.Redis(
            host="127.0.0.1", port=port, decode_responses=True, protocol=2
        )

        try:
            # Wait for restarted server
            start_time = time.monotonic()
            connected2 = False
            while time.monotonic() - start_time < 5.0:
                try:
                    await client2.ping()
                    connected2 = True
                    break
                except Exception:
                    await asyncio.sleep(0.05)

            assert connected2, "Restarted server failed to accept connections"

            # 4. Verify value survived the crash!
            recovered_val = await client2.get("persisted_key")
            assert recovered_val == "persisted_val"

        finally:
            await client2.aclose()
            proc2.kill()
            proc2.wait()
