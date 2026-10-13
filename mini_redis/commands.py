"""Command registry and handlers for Mini-Redis.

Dispatches parsed RESP arrays to command handler functions.
Each handler takes (store, args) and returns a RESP-encodable response
(SimpleString, RespError, int, bytes, or None).
"""

from typing import Any, Callable
from mini_redis.resp import RespError, SimpleString
from mini_redis.store import Store

CommandHandler = Callable[[Store, list[Any]], Any]


def _to_bytes(val: Any) -> bytes:
    """Helper to convert any argument to bytes."""
    if isinstance(val, (bytes, bytearray)):
        return bytes(val)
    return str(val).encode("utf-8")


# ---------------------------------------------------------------------------
# Command Handlers
# ---------------------------------------------------------------------------

def handle_ping(_store: Store, args: list[Any]) -> Any:
    """
    PING [message]
    Returns PONG or the provided message.
    """
    if len(args) == 0:
        return SimpleString("PONG")
    if len(args) == 1:
        return args[0]
    return RespError("ERR wrong number of arguments for 'ping' command")


def handle_echo(_store: Store, args: list[Any]) -> Any:
    """
    ECHO message
    Returns the message.
    """
    if len(args) != 1:
        return RespError("ERR wrong number of arguments for 'echo' command")
    return args[0]


def handle_set(store: Store, args: list[Any]) -> Any:
    """
    SET key value [EX seconds | PX milliseconds | PXAT unix-time-milliseconds]
    Sets key to hold string value with optional expiration.
    """
    if len(args) < 2:
        return RespError("ERR wrong number of arguments for 'set' command")

    key_bytes = _to_bytes(args[0])
    val_bytes = _to_bytes(args[1])

    # Plain SET without options clears any existing TTL
    if len(args) == 2:
        store.set(key_bytes, val_bytes)
        return SimpleString("OK")

    expire_ms = None
    has_ex = False
    has_px = False
    has_pxat = False

    i = 2
    while i < len(args):
        raw_opt = args[i]
        opt_str = (
            raw_opt.decode("utf-8", errors="replace").upper()
            if isinstance(raw_opt, (bytes, bytearray))
            else str(raw_opt).upper()
        )

        if opt_str == "EX":
            if has_ex or has_px or has_pxat or (i + 1 >= len(args)):
                return RespError("ERR syntax error")
            raw_val = args[i + 1]
            try:
                ex_sec = int(raw_val)
            except ValueError:
                return RespError("ERR value is not an integer or out of range")
            if ex_sec <= 0:
                return RespError("ERR invalid expire time in 'set' command")
            has_ex = True
            expire_ms = store.clock() + ex_sec * 1000
            i += 2

        elif opt_str == "PX":
            if has_ex or has_px or has_pxat or (i + 1 >= len(args)):
                return RespError("ERR syntax error")
            raw_val = args[i + 1]
            try:
                px_ms = int(raw_val)
            except ValueError:
                return RespError("ERR value is not an integer or out of range")
            if px_ms <= 0:
                return RespError("ERR invalid expire time in 'set' command")
            has_px = True
            expire_ms = store.clock() + px_ms
            i += 2

        elif opt_str == "PXAT":
            if has_ex or has_px or has_pxat or (i + 1 >= len(args)):
                return RespError("ERR syntax error")
            raw_val = args[i + 1]
            try:
                pxat_ms = int(raw_val)
            except ValueError:
                return RespError("ERR value is not an integer or out of range")
            if pxat_ms <= 0:
                return RespError("ERR invalid expire time in 'set' command")
            has_pxat = True
            expire_ms = pxat_ms
            i += 2

        else:
            return RespError("ERR syntax error")

    store.set(key_bytes, val_bytes, expire_ms=expire_ms)
    return SimpleString("OK")



def handle_get(store: Store, args: list[Any]) -> Any:
    """
    GET key
    Get the value of key, or None (nil) if it does not exist or has expired.
    """
    if len(args) != 1:
        return RespError("ERR wrong number of arguments for 'get' command")

    return store.get(_to_bytes(args[0]))


def handle_del(store: Store, args: list[Any]) -> Any:
    """
    DEL key [key ...]
    Removes the specified keys. Returns count of removed keys.
    """
    if len(args) == 0:
        return RespError("ERR wrong number of arguments for 'del' command")

    key_bytes_list = [_to_bytes(k) for k in args]
    return store.delete(*key_bytes_list)


def handle_exists(store: Store, args: list[Any]) -> Any:
    """
    EXISTS key [key ...]
    Returns the number of specified keys that exist and have not expired.
    """
    if len(args) == 0:
        return RespError("ERR wrong number of arguments for 'exists' command")

    key_bytes_list = [_to_bytes(k) for k in args]
    return store.exists(*key_bytes_list)


def handle_expire(store: Store, args: list[Any]) -> Any:
    """
    EXPIRE key seconds
    Sets a timeout on key in seconds.
    """
    if len(args) != 2:
        return RespError("ERR wrong number of arguments for 'expire' command")

    key_bytes = _to_bytes(args[0])
    try:
        seconds = int(args[1])
    except ValueError:
        return RespError("ERR value is not an integer or out of range")

    return store.expire(key_bytes, seconds)


def handle_pexpire(store: Store, args: list[Any]) -> Any:
    """
    PEXPIRE key milliseconds
    Sets a timeout on key in milliseconds.
    """
    if len(args) != 2:
        return RespError("ERR wrong number of arguments for 'pexpire' command")

    key_bytes = _to_bytes(args[0])
    try:
        ms = int(args[1])
    except ValueError:
        return RespError("ERR value is not an integer or out of range")

    return store.pexpire(key_bytes, ms)


def handle_pexpireat(store: Store, args: list[Any]) -> Any:
    """
    PEXPIREAT key milliseconds-timestamp
    Sets an absolute expiration timestamp on key in milliseconds.
    """
    if len(args) != 2:
        return RespError("ERR wrong number of arguments for 'pexpireat' command")

    key_bytes = _to_bytes(args[0])
    try:
        timestamp_ms = int(args[1])
    except ValueError:
        return RespError("ERR value is not an integer or out of range")

    return store.pexpireat(key_bytes, timestamp_ms)


def handle_ttl(store: Store, args: list[Any]) -> Any:
    """
    TTL key
    Returns remaining TTL in seconds (-2 if missing/expired, -1 if no expiry).
    """
    if len(args) != 1:
        return RespError("ERR wrong number of arguments for 'ttl' command")

    return store.ttl(_to_bytes(args[0]))


def handle_pttl(store: Store, args: list[Any]) -> Any:
    """
    PTTL key
    Returns remaining TTL in milliseconds (-2 if missing/expired, -1 if no expiry).
    """
    if len(args) != 1:
        return RespError("ERR wrong number of arguments for 'pttl' command")

    return store.pttl(_to_bytes(args[0]))


def handle_persist(store: Store, args: list[Any]) -> Any:
    """
    PERSIST key
    Removes the timeout on key (returns 1 if removed, 0 if no expiry or key missing).
    """
    if len(args) != 1:
        return RespError("ERR wrong number of arguments for 'persist' command")

    return store.persist(_to_bytes(args[0]))


# ---------------------------------------------------------------------------
# Dispatch Table
# ---------------------------------------------------------------------------

COMMAND_TABLE: dict[str, CommandHandler] = {
    "PING": handle_ping,
    "ECHO": handle_echo,
    "SET": handle_set,
    "GET": handle_get,
    "DEL": handle_del,
    "EXISTS": handle_exists,
    "EXPIRE": handle_expire,
    "PEXPIRE": handle_pexpire,
    "PEXPIREAT": handle_pexpireat,
    "TTL": handle_ttl,
    "PTTL": handle_pttl,
    "PERSIST": handle_persist,
}


def execute_command(store: Store, cmd_parts: list[Any]) -> Any:
    """
    Execute a command represented as a list of arguments (from parsed RESP).

    Args:
        store: The Store instance.
        cmd_parts: List of parsed tokens/bulk strings, e.g. [b"SET", b"k", b"v"].

    Returns:
        The response object (SimpleString, RespError, int, bytes, or None).
    """
    if not cmd_parts:
        return RespError("ERR empty command")

    raw_name = cmd_parts[0]
    if isinstance(raw_name, (bytes, bytearray)):
        cmd_str = raw_name.decode("utf-8", errors="replace")
    else:
        cmd_str = str(raw_name)

    cmd_upper = cmd_str.upper()
    handler = COMMAND_TABLE.get(cmd_upper)

    if handler is None:
        return RespError(f"ERR unknown command '{cmd_str}'")

    return handler(store, cmd_parts[1:])
