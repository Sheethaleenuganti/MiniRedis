"""Command registry and handlers for Mini-Redis.

Dispatches parsed RESP arrays to command handler functions.
Each handler takes (store, args) and returns a RESP-encodable response
(SimpleString, RespError, int, bytes, or None).
"""

from typing import Any, Callable
from mini_redis.resp import RespError, SimpleString
from mini_redis.store import Store

CommandHandler = Callable[[Store, list[Any]], Any]


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
    SET key value
    Sets key to hold string value.
    """
    if len(args) != 2:
        return RespError("ERR wrong number of arguments for 'set' command")

    key, value = args[0], args[1]
    # Ensure key and value are bytes
    key_bytes = key if isinstance(key, bytes) else str(key).encode("utf-8")
    val_bytes = value if isinstance(value, bytes) else str(value).encode("utf-8")

    store.set(key_bytes, val_bytes)
    return SimpleString("OK")


def handle_get(store: Store, args: list[Any]) -> Any:
    """
    GET key
    Get the value of key, or None (nil) if it does not exist.
    """
    if len(args) != 1:
        return RespError("ERR wrong number of arguments for 'get' command")

    key = args[0]
    key_bytes = key if isinstance(key, bytes) else str(key).encode("utf-8")
    return store.get(key_bytes)


def handle_del(store: Store, args: list[Any]) -> Any:
    """
    DEL key [key ...]
    Removes the specified keys. Returns count of removed keys.
    """
    if len(args) == 0:
        return RespError("ERR wrong number of arguments for 'del' command")

    key_bytes_list = [
        k if isinstance(k, bytes) else str(k).encode("utf-8") for k in args
    ]
    return store.delete(*key_bytes_list)


def handle_exists(store: Store, args: list[Any]) -> Any:
    """
    EXISTS key [key ...]
    Returns the number of specified keys that exist.
    """
    if len(args) == 0:
        return RespError("ERR wrong number of arguments for 'exists' command")

    key_bytes_list = [
        k if isinstance(k, bytes) else str(k).encode("utf-8") for k in args
    ]
    return store.exists(*key_bytes_list)


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
