"""AOF (Append Only File) Persistence for Mini-Redis.

Implements write-ahead append-only logging of state-modifying Redis commands
using the standard RESP2 wire format (arrays of bulk strings).

Fsync policies:
- always: flush + os.fsync on every write before replying.
- everysec: background asyncio task flushes + fsyncs once per second.
- no: flush to OS buffer only.

Canonical absolute-time form:
- SET k v EX/PX ... is logged as: SET k v PXAT <absolute_ms>
- EXPIRE/PEXPIRE are logged as: PEXPIREAT k <absolute_ms>
"""

import logging
import os
from typing import Any, BinaryIO, Optional

from mini_redis.commands import execute_command
from mini_redis.resp import IncompleteDataError, RespParseError, decode_resp, encode_resp
from mini_redis.store import Store

logger = logging.getLogger("mini_redis.aof")


class AofCorruptedError(Exception):
    """Raised when the AOF file contains corrupted records that cannot be parsed."""
    pass


def to_aof_record(store: Store, cmd_parts: list[Any], reply: Any) -> Optional[list[bytes]]:
    """
    Convert an executed command to its canonical absolute-time AOF record format.
    Returns None if the command was a read, caused an error, or made no state changes.
    """
    if not cmd_parts:
        return None

    raw_cmd = cmd_parts[0]
    cmd_name = (
        raw_cmd.decode("utf-8", errors="replace").upper()
        if isinstance(raw_cmd, (bytes, bytearray))
        else str(raw_cmd).upper()
    )

    def _to_bytes(v: Any) -> bytes:
        return bytes(v) if isinstance(v, (bytes, bytearray)) else str(v).encode("utf-8")

    # 1. SET: log only if successful (+OK)
    if cmd_name == "SET":
        if getattr(reply, "data", None) == "OK":
            if len(cmd_parts) >= 3:
                key = _to_bytes(cmd_parts[1])
                val = _to_bytes(cmd_parts[2])
                exp = store.get_expiry(key)
                if exp is not None:
                    return [b"SET", key, val, b"PXAT", str(exp).encode("ascii")]
                # If key was immediately expired because PXAT was in the past:
                if store.get(key) is None:
                    return [b"DEL", key]
                return [b"SET", key, val]
        return None

    # 2. DEL: log only if count > 0
    if cmd_name == "DEL":
        if isinstance(reply, int) and reply > 0:
            return [_to_bytes(p) for p in cmd_parts]
        return None

    # 3. EXPIRE / PEXPIRE: log only if reply is 1 (key existed and timeout was set)
    if cmd_name in ("EXPIRE", "PEXPIRE"):
        if reply == 1 and len(cmd_parts) >= 2:
            key = _to_bytes(cmd_parts[1])
            exp = store.get_expiry(key)
            if exp is not None:
                return [b"PEXPIREAT", key, str(exp).encode("ascii")]
        return None

    # 4. PEXPIREAT: log only if reply is 1
    if cmd_name == "PEXPIREAT":
        if reply == 1 and len(cmd_parts) >= 2:
            key = _to_bytes(cmd_parts[1])
            exp = store.get_expiry(key)
            if exp is not None:
                return [b"PEXPIREAT", key, str(exp).encode("ascii")]
            return [b"DEL", key]
        return None

    # 5. PERSIST: log only if reply is 1
    if cmd_name == "PERSIST":
        if reply == 1 and len(cmd_parts) >= 2:
            key = _to_bytes(cmd_parts[1])
            return [b"PERSIST", key]
        return None

    return None


class Aof:
    """Manages the Append Only File (AOF)."""

    def __init__(self, filepath: str = "appendonly.aof", fsync: str = "everysec") -> None:
        if fsync not in ("always", "everysec", "no"):
            raise ValueError(
                f"Invalid fsync policy: {fsync!r}. Must be always, everysec, or no."
            )
        self.filepath = filepath
        self.fsync = fsync
        self._file: Optional[BinaryIO] = None

    def open(self) -> None:
        """Open the AOF file in binary append mode ('ab')."""
        if self._file is None or self._file.closed:
            self._file = open(self.filepath, "ab")

    def append(self, record: list[bytes]) -> None:
        """
        Append a command record in RESP2 array format.
        Flushes and fsyncs according to the configured fsync policy.
        """
        if self._file is None or self._file.closed:
            self.open()

        wire_bytes = encode_resp(record)
        assert self._file is not None
        self._file.write(wire_bytes)

        if self.fsync == "always":
            self._file.flush()
            os.fsync(self._file.fileno())
        elif self.fsync == "no":
            self._file.flush()
        elif self.fsync == "everysec":
            self._file.flush()

    def sync(self) -> None:
        """Flush Python buffer and fsync to disk (used by everysec background task)."""
        if self._file and not self._file.closed:
            self._file.flush()
            os.fsync(self._file.fileno())

    def flush(self) -> None:
        """Flush Python buffer to OS."""
        if self._file and not self._file.closed:
            self._file.flush()

    def close(self) -> None:
        """Flush, fsync (if applicable), and close the file handle."""
        if self._file and not self._file.closed:
            try:
                self._file.flush()
                if self.fsync != "no":
                    os.fsync(self._file.fileno())
            finally:
                self._file.close()
                self._file = None

    def replay(self, store: Store, chunk_size: int = 65536) -> None:
        """
        Replay commands from AOF into Store on startup.
        - Reads file in chunks without unbounded memory.
        - If file ends with an incomplete record (crash mid-write), truncate to last good offset.
        - If record in the middle is corrupted, raise AofCorruptedError.
        """
        if not os.path.exists(self.filepath):
            return

        last_good_offset = 0
        buffer = bytearray()
        truncated = False

        with open(self.filepath, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    # EOF reached
                    break

                buffer.extend(chunk)

                # Process all complete records currently in the buffer
                while buffer:
                    try:
                        msg, consumed = decode_resp(buffer)
                    except IncompleteDataError:
                        # Need more data from the file
                        break
                    except RespParseError as err:
                        raise AofCorruptedError(
                            f"AOF corrupted at byte offset {last_good_offset}: {err}"
                        ) from err

                    # Execute command against store with logging disabled
                    if isinstance(msg, list):
                        execute_command(store, msg)
                    last_good_offset += consumed
                    del buffer[:consumed]

        # Handle remaining buffer at EOF
        if buffer:
            while buffer:
                try:
                    msg, consumed = decode_resp(buffer)
                    if isinstance(msg, list):
                        execute_command(store, msg)
                    last_good_offset += consumed
                    del buffer[:consumed]
                except IncompleteDataError:
                    # Truncated tail at the end of the file
                    truncated = True
                    break
                except RespParseError as err:
                    raise AofCorruptedError(
                        f"AOF corrupted at byte offset {last_good_offset}: {err}"
                    ) from err

        if truncated:
            logger.warning(
                "AOF file ends with an incomplete record. Truncating to offset %d.",
                last_good_offset,
            )
            with open(self.filepath, "r+b") as f:
                f.truncate(last_good_offset)
